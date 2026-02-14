from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..agent_workflow import AgentWorkflowService
from ..config import AppConfig
from ..logging_utils import get_logger
from ..notion_helpers import property_plain_text
from ..notion_gateway import NotionGateway
from ..postgres_store import PostgresStore
from ..sync_service import SyncOptions, SyncService
from .background import BatchTaskStore, run_in_background
from .helpers import parse_iso_datetime, safe_json, url_with_message

CN_TZ = timezone(timedelta(hours=8))
REASON_KEYWORDS: list[tuple[str, list[str]]] = [
    ("审题偏差", ["审题", "题意", "条件", "漏看"]),
    ("计算失误", ["计算", "口算", "运算", "笔算", "抄错", "粗心"]),
    ("概念不清", ["概念", "定义", "性质", "混淆"]),
    ("单位换算", ["单位", "换算"]),
    ("公式误用", ["公式", "定理", "法则"]),
    ("应用题建模", ["应用题", "方程", "建模", "数量关系"]),
]


def _week_label(dt: datetime) -> str:
    dt_cn = dt.astimezone(CN_TZ)
    year, week, _ = dt_cn.isocalendar()
    return f"{year}-W{week:02d}"


def _month_label(dt: datetime) -> str:
    return dt.astimezone(CN_TZ).strftime("%Y-%m")


def _extract_reason_label(row: dict[str, Any]) -> str:
    source = safe_json(str(row.get("source_snapshot_json") or ""), {})
    text_parts = [
        str(row.get("reasoning_summary") or ""),
        str(row.get("validation_notes") or ""),
        str(source.get("property_text") or ""),
        str(source.get("plain_text") or ""),
    ]
    text = "\n".join(text_parts).lower()
    for label, words in REASON_KEYWORDS:
        if any(word in text for word in words):
            return label
    cleaned = re.sub(r"\s+", " ", str(row.get("reasoning_summary") or "").strip())
    if cleaned:
        return cleaned[:16]
    return "未分类"


def _extract_five_pass_status(page: dict[str, Any]) -> bool | None:
    props = safe_json(str(page.get("properties_json") or ""), {})
    if isinstance(props, dict):
        for key, value in props.items():
            key_text = str(key or "").strip()
            if not any(token in key_text for token in ("五遍", "遍", "通关", "复盘")):
                continue
            plain = property_plain_text(value if isinstance(value, dict) else {}).strip().lower()
            if not plain:
                continue
            if any(token in plain for token in ("通关", "完成", "已完成", "pass", "第5遍", "五遍", "5遍")):
                return True
            if any(token in plain for token in ("第1遍", "第2遍", "第3遍", "第4遍", "待", "进行中", "未")):
                return False
    property_text = str(page.get("property_text") or "").lower()
    if "通关" in property_text or "第5遍" in property_text or "五遍" in property_text:
        return True
    if any(token in property_text for token in ("第1遍", "第2遍", "第3遍", "第4遍")):
        return False
    return None


def _limit_sorted_counter(counter: Counter[str], limit: int) -> list[dict[str, Any]]:
    rows = [{"label": key, "count": value} for key, value in counter.most_common(limit)]
    if not rows:
        return []
    top = max(int(row["count"]) for row in rows) or 1
    for row in rows:
        row["ratio"] = round(100 * int(row["count"]) / top, 1)
    return rows


def _build_sync_overview(store: PostgresStore) -> dict[str, Any]:
    runs = store.list_sync_runs(limit=8, offset=0)
    latest_run_id = int(runs[0]["run_id"]) if runs else 0
    latest_changes: list[dict[str, Any]] = []
    if latest_run_id:
        events = store.list_sync_events(latest_run_id, step="database_synced", limit=50)
        for event in events:
            detail = safe_json(str(event.get("detail_json") or ""), {})
            pages = detail.get("changed_pages", [])
            if not isinstance(pages, list):
                continue
            for item in pages:
                if not isinstance(item, dict):
                    continue
                latest_changes.append(
                    {
                        "logical_db": str(event.get("logical_db") or "").strip(),
                        "change_type": str(item.get("change_type") or "").strip(),
                        "title": str(item.get("title") or "").strip(),
                        "page_id": str(item.get("page_id") or "").strip(),
                        "last_edited_time": str(item.get("last_edited_time") or "").strip(),
                    }
                )
    return {
        "runs": runs,
        "latest_run_id": latest_run_id,
        "latest_changes": latest_changes[:120],
    }


def _build_error_analytics(store: PostgresStore) -> dict[str, Any]:
    suggestions = store.list_agent_suggestions(limit=4000)
    error_pages = [row for row in store.get_pages("errors") if int(row.get("archived") or 0) == 0]
    concept_map = {
        str(row.get("page_id") or ""): str(row.get("title") or "").strip()
        for row in store.get_pages("concepts")
        if int(row.get("archived") or 0) == 0
    }

    reason_counts: Counter[str] = Counter()
    concept_counts: Counter[str] = Counter()
    week_counts: Counter[str] = Counter()
    month_counts: Counter[str] = Counter()
    pass_total: Counter[str] = Counter()
    pass_done: Counter[str] = Counter()

    for row in suggestions:
        reason_counts[_extract_reason_label(row)] += 1
        concept_id = str(row.get("proposed_concept_id") or "").strip()
        if concept_id:
            concept_counts[concept_map.get(concept_id, concept_id[:8])] += 1
        created = parse_iso_datetime(str(row.get("created_at") or ""))
        if created:
            week_counts[_week_label(created)] += 1
            month_counts[_month_label(created)] += 1

    for page in error_pages:
        dt = parse_iso_datetime(str(page.get("created_time") or page.get("last_edited_time") or ""))
        if not dt:
            continue
        key = _month_label(dt)
        pass_state = _extract_five_pass_status(page)
        if pass_state is None:
            continue
        pass_total[key] += 1
        if pass_state:
            pass_done[key] += 1

    pass_note = ""
    if not pass_total:
        pass_note = "未检测到“五遍”字段，当前使用“已应用建议占比”近似"
        for row in suggestions:
            created = parse_iso_datetime(str(row.get("created_at") or ""))
            if not created:
                continue
            key = _month_label(created)
            pass_total[key] += 1
            if str(row.get("status") or "") == "applied":
                pass_done[key] += 1

    pass_rows: list[dict[str, Any]] = []
    for key in sorted(pass_total.keys())[-12:]:
        total = int(pass_total[key])
        done = int(pass_done.get(key, 0))
        rate = round((done / total) * 100, 1) if total else 0.0
        pass_rows.append({"period": key, "done": done, "total": total, "rate": rate})

    week_rows = [{"period": key, "count": int(week_counts[key])} for key in sorted(week_counts.keys())[-12:]]
    month_rows = [{"period": key, "count": int(month_counts[key])} for key in sorted(month_counts.keys())[-12:]]

    return {
        "reason_rows": _limit_sorted_counter(reason_counts, 10),
        "concept_rows": _limit_sorted_counter(concept_counts, 12),
        "pass_rows": pass_rows,
        "pass_note": pass_note,
        "week_rows": week_rows,
        "month_rows": month_rows,
        "generated_at": datetime.now(CN_TZ).isoformat(),
    }


def build_errors_router(
    *,
    cfg: AppConfig,
    store: PostgresStore,
    templates: Jinja2Templates,
    error_workflow: AgentWorkflowService,
    runs_page_size: int,
    sync_cache_ttl_seconds: int,
    pending_cache_ttl_seconds: int,
    get_sync_status_cached: Callable[..., tuple[dict[str, Any], bool]],
    get_error_pending_cached: Callable[..., tuple[int, str, bool]],
    get_knowledge_pending_cached: Callable[..., tuple[int, str, bool]],
    batch_store: BatchTaskStore,
) -> APIRouter:
    router = APIRouter()
    logger = get_logger(__name__)

    def run_error_batch_task(
        task_id: str,
        action_key: str,
        suggestion_ids: list[int],
        reviewer_note: str,
    ) -> None:
        logger.info(
            "Error batch task started: task_id=%s action=%s total=%s",
            task_id,
            action_key,
            len(suggestion_ids),
        )
        success = 0
        failed = 0
        try:
            total = len(suggestion_ids)
            for index, suggestion_id in enumerate(suggestion_ids, start=1):
                row = store.get_agent_suggestion(suggestion_id)
                title = (row["error_title"] if row else "") or ""
                batch_store.set(
                    task_id,
                    {
                        "current_item_id": suggestion_id,
                        "current_item_title": title,
                        "message": f"处理中 {index}/{total}",
                    },
                )
                try:
                    if action_key == "approve":
                        error_workflow.apply_suggestion(suggestion_id, reviewer_note=reviewer_note)
                    elif action_key == "regenerate_apply":
                        result = error_workflow.regenerate_suggestion(
                            suggestion_id,
                            reviewer_note=reviewer_note,
                            auto_apply=True,
                        )
                        new_suggestion_id = int(result["suggestion_id"])
                        refreshed_row = store.get_agent_suggestion(new_suggestion_id)
                        if refreshed_row:
                            batch_store.set(
                                task_id,
                                {
                                    "current_item_id": new_suggestion_id,
                                    "current_item_title": (refreshed_row["error_title"] or "").strip(),
                                },
                            )
                    elif action_key == "reject":
                        error_workflow.reject_suggestion(suggestion_id, reviewer_note=reviewer_note)
                    elif action_key == "pending":
                        store.update_suggestion_status(
                            suggestion_id,
                            status="pending_review",
                            reviewer_note=reviewer_note,
                        )
                    else:
                        raise ValueError("未知批量操作")
                    success += 1
                    batch_store.set(task_id, {"last_error": ""})
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    logger.exception(
                        "Error batch item failed: task_id=%s suggestion_id=%s action=%s",
                        task_id,
                        suggestion_id,
                        action_key,
                    )
                    store.update_suggestion_status(
                        suggestion_id,
                        status="failed",
                        reviewer_note=reviewer_note,
                        failure_reason=str(exc),
                    )
                    batch_store.set(task_id, {"last_error": str(exc)})
                batch_store.set(
                    task_id,
                    {
                        "processed": index,
                        "success": success,
                        "failed": failed,
                    },
                )

            action_label = "批量重生成并回写" if action_key == "regenerate_apply" else "批量操作"
            msg = f"{action_label}完成：成功 {success}，失败 {failed}"
            task = batch_store.get(task_id)
            done_redirect_url = url_with_message(
                str(task.get("back_url") if task else "/suggestions"),
                key="batch_notice",
                message=msg,
            )
            batch_store.set(
                task_id,
                {
                    "status": "completed",
                    "message": msg,
                    "done_redirect_url": done_redirect_url,
                    "finished_at": batch_store.now_iso(),
                },
            )
            logger.info(
                "Error batch task completed: task_id=%s success=%s failed=%s",
                task_id,
                success,
                failed,
            )
            get_error_pending_cached(force=True)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error batch task fatal: task_id=%s action=%s", task_id, action_key)
            batch_store.set(
                task_id,
                {
                    "status": "failed",
                    "last_error": str(exc),
                    "message": f"任务失败：{exc}",
                    "done_redirect_url": url_with_message(
                        "/suggestions",
                        key="batch_error",
                        message=f"批量任务失败：{exc}",
                    ),
                    "finished_at": batch_store.now_iso(),
                },
            )

    @router.get("/errors", response_class=HTMLResponse)
    def errors_dashboard(request: Request) -> HTMLResponse:
        raw_runs_page = request.query_params.get("runs_page", "1").strip()
        try:
            runs_page = max(1, int(raw_runs_page))
        except ValueError:
            runs_page = 1

        runs_total = store.count_workflow_runs()
        runs_total_pages = max(1, (runs_total + runs_page_size - 1) // runs_page_size)
        runs_page = min(runs_page, runs_total_pages)
        runs = store.list_workflow_runs(limit=runs_page_size, offset=(runs_page - 1) * runs_page_size)

        counts = store.suggestion_counts()
        run_error = request.query_params.get("run_error", "").strip()
        sync_error = request.query_params.get("sync_error", "").strip()
        sync_notice = request.query_params.get("sync_notice", "").strip()
        batch_notice = request.query_params.get("batch_notice", "").strip()
        force_refresh = request.query_params.get("refresh", "").strip().lower() in {"1", "true", "all"}

        sync_status, sync_status_from_cache = get_sync_status_cached(force=force_refresh)
        pending_targets, bootstrap_error, pending_from_cache = get_error_pending_cached(force=force_refresh)
        sync_overview = _build_sync_overview(store)
        analytics = _build_error_analytics(store)

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "active_tab": "errors",
                "runs": runs,
                "pending_targets": pending_targets,
                "pending_review": counts.get("pending_review", 0),
                "needs_review": counts.get("needs_review", 0),
                "applied": counts.get("applied", 0),
                "model": cfg.gemini_model,
                "threshold": cfg.confidence_threshold,
                "bootstrap_error": bootstrap_error,
                "run_error": run_error,
                "sync_status": sync_status,
                "sync_error": sync_error,
                "sync_notice": sync_notice,
                "batch_notice": batch_notice,
                "sync_status_from_cache": sync_status_from_cache,
                "pending_from_cache": pending_from_cache,
                "sync_cache_ttl_seconds": sync_cache_ttl_seconds,
                "pending_cache_ttl_seconds": pending_cache_ttl_seconds,
                "runs_page": runs_page,
                "runs_total": runs_total,
                "runs_total_pages": runs_total_pages,
                "sync_runs": sync_overview["runs"],
                "latest_sync_run_id": sync_overview["latest_run_id"],
                "latest_sync_changes": sync_overview["latest_changes"],
                "analytics": analytics,
            },
        )

    @router.post("/dashboard/refresh")
    def refresh_error_dashboard(target: str = Form(default="all")) -> RedirectResponse:
        key = (target or "all").strip().lower()
        logger.info("Manual dashboard refresh requested: target=%s", key)
        refreshed: list[str] = []
        if key in {"all", "sync"}:
            get_sync_status_cached(force=True)
            refreshed.append("同步状态")
        if key in {"all", "pending"}:
            get_error_pending_cached(force=True)
            refreshed.append("待处理统计")
        msg = f"已手动刷新：{', '.join(refreshed) if refreshed else '无'}"
        return RedirectResponse(url=f"/errors?sync_notice={quote(msg)}", status_code=303)

    @router.post("/sync/run")
    def run_sync(
        include_page_content: str | None = Form(default=None),
        content_max_chars: int = Form(default=1600),
        content_max_depth: int = Form(default=2),
        page_size: int = Form(default=100),
    ) -> RedirectResponse:
        logger.info(
            "Manual sync requested: include_page_content=%s content_max_chars=%s content_max_depth=%s page_size=%s",
            bool(include_page_content),
            content_max_chars,
            content_max_depth,
            page_size,
        )
        try:
            token = cfg.notion_token()
            gateway = NotionGateway(token)
            svc = SyncService(config=cfg, gateway=gateway, store=store)
            result = svc.run(
                SyncOptions(
                    include_page_content=bool(include_page_content),
                    content_max_chars=max(200, content_max_chars),
                    content_max_depth=max(1, content_max_depth),
                    page_size=max(10, min(100, page_size)),
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Manual sync failed")
            return RedirectResponse(url=f"/errors?sync_error={quote(str(exc))}", status_code=303)

        get_sync_status_cached(force=True)
        get_error_pending_cached(force=True)
        get_knowledge_pending_cached(force=True)

        db_msg = ", ".join(f"{key}:{value}" for key, value in result.databases.items())
        rel_msg = ", ".join(f"{key}:{value}" for key, value in result.relations.items())
        changed_msg = ", ".join(f"{key}:{value}" for key, value in result.changed.items())
        summary = f"同步完成（run #{result.run_id}）。页面[{db_msg}] 关联[{rel_msg}] 变更[{changed_msg}]"
        return RedirectResponse(url=f"/errors?sync_notice={quote(summary)}", status_code=303)

    @router.post("/workflow/run")
    def run_error_workflow(limit: int = Form(default=20)) -> RedirectResponse:
        actual_limit = None if limit <= 0 else limit
        logger.info("Error workflow trigger requested: limit=%s", actual_limit)
        try:
            summary = error_workflow.run(limit=actual_limit)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error workflow trigger failed")
            return RedirectResponse(url=f"/errors?run_error={quote(str(exc))}", status_code=303)
        get_error_pending_cached(force=True)
        return RedirectResponse(url=f"/runs/{summary.run_id}", status_code=303)

    @router.get("/runs/{run_id}", response_class=HTMLResponse)
    def error_run_detail(request: Request, run_id: int) -> HTMLResponse:
        run = store.get_workflow_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="run not found")
        events = store.list_workflow_events(run_id)
        suggestions = [s for s in store.list_agent_suggestions(limit=500) if s["run_id"] == run_id]
        return templates.TemplateResponse(
            "run_detail.html",
            {
                "request": request,
                "active_tab": "errors",
                "run": run,
                "events": events,
                "suggestions": suggestions,
            },
        )

    @router.get("/suggestions", response_class=HTMLResponse)
    def error_suggestions_page(
        request: Request,
        status: str = Query(default="all"),
    ) -> HTMLResponse:
        rows = store.list_agent_suggestions(status=None if status == "all" else status, limit=500)
        return templates.TemplateResponse(
            "suggestions.html",
            {
                "request": request,
                "active_tab": "errors",
                "suggestions": rows,
                "status": status,
                "batch_notice": request.query_params.get("batch_notice", "").strip(),
                "batch_error": request.query_params.get("batch_error", "").strip(),
            },
        )

    @router.post("/suggestions/batch")
    async def batch_update_error_suggestions(
        action: str = Form(default=""),
        suggestion_ids: list[int] = Form(default=[]),
        reviewer_note: str = Form(default=""),
        status: str = Form(default="all"),
    ) -> RedirectResponse:
        logger.info(
            "Error batch update requested: action=%s count=%s status=%s",
            action,
            len(suggestion_ids),
            status,
        )
        if not suggestion_ids:
            return RedirectResponse(
                url=f"/suggestions?status={quote(status)}&batch_error={quote('请先勾选记录')}",
                status_code=303,
            )

        action_key = (action or "").strip().lower()
        if not action_key:
            return RedirectResponse(
                url=f"/suggestions?status={quote(status)}&batch_error={quote('请选择批量操作')}",
                status_code=303,
            )

        if action_key in {"approve", "regenerate_apply"}:
            task = batch_store.build(
                scope="errors",
                action=action_key,
                suggestion_ids=suggestion_ids,
                back_url=f"/suggestions?status={quote(status)}",
                done_redirect_url=f"/suggestions?status={quote(status)}",
            )
            task_id = batch_store.register(task)
            logger.info("Error batch async task created: task_id=%s count=%s", task_id, len(suggestion_ids))
            run_in_background(run_error_batch_task, task_id, action_key, suggestion_ids, reviewer_note)
            return RedirectResponse(
                url=f"/suggestions/batch/tasks/{quote(task_id)}?status={quote(status)}",
                status_code=303,
            )

        success = 0
        failed = 0
        for suggestion_id in suggestion_ids:
            try:
                if action_key == "reject":
                    error_workflow.reject_suggestion(suggestion_id, reviewer_note=reviewer_note)
                elif action_key == "pending":
                    store.update_suggestion_status(
                        suggestion_id,
                        status="pending_review",
                        reviewer_note=reviewer_note,
                    )
                else:
                    return RedirectResponse(
                        url=f"/suggestions?status={quote(status)}&batch_error={quote('未知批量操作')}",
                        status_code=303,
                    )
                success += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.exception(
                    "Error batch synchronous item failed: suggestion_id=%s action=%s",
                    suggestion_id,
                    action_key,
                )
                store.update_suggestion_status(
                    suggestion_id,
                    status="failed",
                    reviewer_note=reviewer_note,
                    failure_reason=str(exc),
                )

        msg = f"批量操作完成：成功 {success}，失败 {failed}"
        return RedirectResponse(
            url=f"/suggestions?status={quote(status)}&batch_notice={quote(msg)}",
            status_code=303,
        )

    @router.get("/suggestions/batch/tasks/{task_id}", response_class=HTMLResponse)
    def error_batch_task_progress_page(request: Request, task_id: str) -> HTMLResponse:
        task = batch_store.get(task_id)
        if not task or task.get("scope") != "errors":
            raise HTTPException(status_code=404, detail="batch task not found")
        task_title = (
            "错题批量重生成并回写进度"
            if str(task.get("action") or "").strip() == "regenerate_apply"
            else "错题批量回写进度"
        )
        return templates.TemplateResponse(
            "batch_task_progress.html",
            {
                "request": request,
                "active_tab": "errors",
                "task_id": task_id,
                "task_title": task_title,
                "status_url": f"/suggestions/batch/tasks/{quote(task_id)}/status",
                "back_url": task.get("back_url") or "/suggestions",
                "done_redirect_url": task.get("done_redirect_url") or "/suggestions",
            },
        )

    @router.get("/suggestions/batch/tasks/{task_id}/status")
    def error_batch_task_status(task_id: str) -> dict[str, Any]:
        task = batch_store.get(task_id)
        if not task or task.get("scope") != "errors":
            raise HTTPException(status_code=404, detail="batch task not found")
        return task

    @router.get("/suggestions/{suggestion_id}", response_class=HTMLResponse)
    def error_suggestion_detail(request: Request, suggestion_id: int) -> HTMLResponse:
        row = store.get_agent_suggestion(suggestion_id)
        if not row:
            raise HTTPException(status_code=404, detail="suggestion not found")

        source = safe_json(row["source_snapshot_json"], {})
        candidates = safe_json(row["candidates_json"], {})
        model_response = safe_json(row["model_response_json"], {})

        return templates.TemplateResponse(
            "suggestion_detail.html",
            {
                "request": request,
                "active_tab": "errors",
                "row": row,
                "source": source,
                "candidates": candidates,
                "model_response": model_response,
                "selected_similar": safe_json(row["proposed_similar_ids_json"], []),
                "model_response_pretty": json.dumps(model_response, ensure_ascii=False, indent=2),
                "action_notice": request.query_params.get("action_notice", "").strip(),
                "action_error": request.query_params.get("action_error", "").strip(),
            },
        )

    @router.post("/suggestions/{suggestion_id}/save")
    def save_error_suggestion(
        suggestion_id: int,
        proposed_title: str = Form(default=""),
        proposed_resource_id: str = Form(default=""),
        proposed_concept_id: str = Form(default=""),
        proposed_skill_id: str = Form(default=""),
        proposed_mindset_id: str = Form(default=""),
        proposed_similar_ids: str = Form(default=""),
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        logger.info("Error suggestion saved: suggestion_id=%s", suggestion_id)
        similar_ids = [item.strip() for item in proposed_similar_ids.split(",") if item.strip()][:3]
        store.update_suggestion_fields(
            suggestion_id,
            proposed_title=proposed_title.strip() or None,
            proposed_resource_id=proposed_resource_id.strip() or None,
            proposed_concept_id=proposed_concept_id.strip() or None,
            proposed_skill_id=proposed_skill_id.strip() or None,
            proposed_mindset_id=proposed_mindset_id.strip() or None,
            proposed_similar_ids=similar_ids,
            reviewer_note=reviewer_note,
        )
        row = store.get_agent_suggestion(suggestion_id)
        if row and row["status"] == "rejected":
            store.update_suggestion_status(
                suggestion_id,
                status="pending_review",
                reviewer_note=reviewer_note,
            )
        return RedirectResponse(url=f"/suggestions/{suggestion_id}", status_code=303)

    @router.post("/suggestions/{suggestion_id}/approve")
    def approve_error_suggestion(
        suggestion_id: int,
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        try:
            error_workflow.apply_suggestion(suggestion_id, reviewer_note=reviewer_note)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error suggestion approve failed: suggestion_id=%s", suggestion_id)
            store.update_suggestion_status(
                suggestion_id,
                status="failed",
                reviewer_note=reviewer_note,
                failure_reason=str(exc),
                set_reviewed_at=True,
            )
        return RedirectResponse(url=f"/suggestions/{suggestion_id}", status_code=303)

    @router.post("/suggestions/{suggestion_id}/regenerate-apply")
    def regenerate_and_apply_error_suggestion(
        suggestion_id: int,
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        logger.info("Error suggestion regenerate+apply requested: suggestion_id=%s", suggestion_id)
        try:
            result = error_workflow.regenerate_suggestion(
                suggestion_id,
                reviewer_note=reviewer_note,
                auto_apply=True,
            )
            new_suggestion_id = int(result["suggestion_id"])
            return RedirectResponse(
                url=url_with_message(
                    f"/suggestions/{new_suggestion_id}",
                    key="action_notice",
                    message=f"已重新生成并回写 Notion（run #{result['run_id']}）",
                ),
                status_code=303,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error suggestion regenerate+apply failed: suggestion_id=%s", suggestion_id)
            try:
                store.update_suggestion_status(
                    suggestion_id,
                    status="failed",
                    reviewer_note=reviewer_note,
                    failure_reason=str(exc),
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Error suggestion regenerate+apply failure status update also failed: suggestion_id=%s",
                    suggestion_id,
                )
            return RedirectResponse(
                url=url_with_message(
                    f"/suggestions/{suggestion_id}",
                    key="action_error",
                    message=f"重生成并回写失败：{exc}",
                ),
                status_code=303,
            )

    @router.post("/suggestions/{suggestion_id}/reject")
    def reject_error_suggestion(
        suggestion_id: int,
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        logger.info("Error suggestion rejected: suggestion_id=%s", suggestion_id)
        error_workflow.reject_suggestion(suggestion_id, reviewer_note=reviewer_note)
        return RedirectResponse(url=f"/suggestions/{suggestion_id}", status_code=303)

    return router
