from __future__ import annotations

import json
from typing import Any, Callable
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..knowledge_workflow import KnowledgeRunOptions, KnowledgeWorkflowService
from ..logging_utils import get_logger
from ..postgres_store import PostgresStore
from .background import BatchTaskStore, run_in_background
from .helpers import safe_json, url_with_message


def build_knowledge_router(
    *,
    cfg_model: str,
    cfg_threshold: float,
    store: PostgresStore,
    templates: Jinja2Templates,
    knowledge_workflow: KnowledgeWorkflowService,
    runs_page_size: int,
    pending_cache_ttl_seconds: int,
    get_knowledge_pending_cached: Callable[..., tuple[int, str, bool]],
    batch_store: BatchTaskStore,
) -> APIRouter:
    router = APIRouter()
    logger = get_logger(__name__)

    def run_knowledge_batch_task(
        task_id: str,
        action_key: str,
        suggestion_ids: list[int],
        reviewer_note: str,
    ) -> None:
        logger.info(
            "Knowledge batch task started: task_id=%s action=%s total=%s",
            task_id,
            action_key,
            len(suggestion_ids),
        )
        success = 0
        failed = 0
        try:
            total = len(suggestion_ids)
            for index, suggestion_id in enumerate(suggestion_ids, start=1):
                row = store.get_knowledge_suggestion(suggestion_id)
                title = (row["page_title"] if row else "") or ""
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
                        knowledge_workflow.apply_suggestion(suggestion_id, reviewer_note=reviewer_note)
                    elif action_key == "reject":
                        knowledge_workflow.reject_suggestion(suggestion_id, reviewer_note=reviewer_note)
                    elif action_key == "pending":
                        store.update_knowledge_suggestion_status(
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
                        "Knowledge batch item failed: task_id=%s suggestion_id=%s action=%s",
                        task_id,
                        suggestion_id,
                        action_key,
                    )
                    store.update_knowledge_suggestion_status(
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
            msg = f"批量操作完成：成功 {success}，失败 {failed}"
            task = batch_store.get(task_id)
            done_redirect_url = url_with_message(
                str(task.get("back_url") if task else "/knowledge/suggestions"),
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
                "Knowledge batch task completed: task_id=%s success=%s failed=%s",
                task_id,
                success,
                failed,
            )
            get_knowledge_pending_cached(force=True)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Knowledge batch task fatal: task_id=%s action=%s", task_id, action_key)
            batch_store.set(
                task_id,
                {
                    "status": "failed",
                    "last_error": str(exc),
                    "message": f"任务失败：{exc}",
                    "done_redirect_url": url_with_message(
                        "/knowledge/suggestions",
                        key="batch_error",
                        message=f"批量任务失败：{exc}",
                    ),
                    "finished_at": batch_store.now_iso(),
                },
            )

    @router.get("/knowledge", response_class=HTMLResponse)
    def knowledge_dashboard(request: Request) -> HTMLResponse:
        raw_runs_page = request.query_params.get("runs_page", "1").strip()
        try:
            runs_page = max(1, int(raw_runs_page))
        except ValueError:
            runs_page = 1

        runs_total = store.count_knowledge_runs()
        runs_total_pages = max(1, (runs_total + runs_page_size - 1) // runs_page_size)
        runs_page = min(runs_page, runs_total_pages)
        runs = store.list_knowledge_runs(limit=runs_page_size, offset=(runs_page - 1) * runs_page_size)

        counts = store.knowledge_suggestion_counts()
        run_error = request.query_params.get("run_error", "").strip()
        run_notice = request.query_params.get("run_notice", "").strip()
        force_refresh = request.query_params.get("refresh", "").strip().lower() in {"1", "true", "all"}

        pending_targets, bootstrap_error, pending_from_cache = get_knowledge_pending_cached(
            force=force_refresh
        )

        return templates.TemplateResponse(
            "knowledge_index.html",
            {
                "request": request,
                "active_tab": "knowledge",
                "runs": runs,
                "pending_targets": pending_targets,
                "pending_review": counts.get("pending_review", 0),
                "needs_review": counts.get("needs_review", 0),
                "applied": counts.get("applied", 0),
                "model": cfg_model,
                "threshold": cfg_threshold,
                "bootstrap_error": bootstrap_error,
                "run_error": run_error,
                "run_notice": run_notice,
                "pending_from_cache": pending_from_cache,
                "pending_cache_ttl_seconds": pending_cache_ttl_seconds,
                "runs_page": runs_page,
                "runs_total": runs_total,
                "runs_total_pages": runs_total_pages,
            },
        )

    @router.post("/knowledge/dashboard/refresh")
    def refresh_knowledge_dashboard() -> RedirectResponse:
        logger.info("Manual knowledge dashboard refresh requested")
        get_knowledge_pending_cached(force=True)
        return RedirectResponse(url=f"/knowledge?run_notice={quote('已刷新待处理统计')}", status_code=303)

    @router.post("/knowledge/workflow/run")
    def run_knowledge_workflow(limit: int = Form(default=20)) -> RedirectResponse:
        actual_limit = None if limit <= 0 else limit
        logger.info("Knowledge workflow trigger requested: limit=%s", actual_limit)
        try:
            summary = knowledge_workflow.run(KnowledgeRunOptions(limit=actual_limit))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Knowledge workflow trigger failed")
            return RedirectResponse(url=f"/knowledge?run_error={quote(str(exc))}", status_code=303)
        get_knowledge_pending_cached(force=True)
        return RedirectResponse(url=f"/knowledge/runs/{summary.run_id}", status_code=303)

    @router.get("/knowledge/runs/{run_id}", response_class=HTMLResponse)
    def knowledge_run_detail(request: Request, run_id: int) -> HTMLResponse:
        run = store.get_knowledge_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="knowledge run not found")
        events = store.list_knowledge_events(run_id)
        suggestions = store.list_knowledge_suggestions(run_id=run_id, limit=500)
        return templates.TemplateResponse(
            "knowledge_run_detail.html",
            {
                "request": request,
                "active_tab": "knowledge",
                "run": run,
                "events": events,
                "suggestions": suggestions,
            },
        )

    @router.get("/knowledge/suggestions", response_class=HTMLResponse)
    def knowledge_suggestions_page(
        request: Request,
        status: str = Query(default="all"),
        logical_db: str = Query(default="all"),
    ) -> HTMLResponse:
        rows = store.list_knowledge_suggestions(
            status=None if status == "all" else status,
            logical_db=None if logical_db == "all" else logical_db,
            limit=500,
        )
        return templates.TemplateResponse(
            "knowledge_suggestions.html",
            {
                "request": request,
                "active_tab": "knowledge",
                "suggestions": rows,
                "status": status,
                "logical_db": logical_db,
                "batch_notice": request.query_params.get("batch_notice", "").strip(),
                "batch_error": request.query_params.get("batch_error", "").strip(),
            },
        )

    @router.post("/knowledge/suggestions/batch")
    async def batch_update_knowledge_suggestions(
        action: str = Form(default=""),
        suggestion_ids: list[int] = Form(default=[]),
        reviewer_note: str = Form(default=""),
        status: str = Form(default="all"),
        logical_db: str = Form(default="all"),
    ) -> RedirectResponse:
        logger.info(
            "Knowledge batch update requested: action=%s count=%s status=%s logical_db=%s",
            action,
            len(suggestion_ids),
            status,
            logical_db,
        )
        if not suggestion_ids:
            return RedirectResponse(
                url=(
                    f"/knowledge/suggestions?status={quote(status)}"
                    f"&logical_db={quote(logical_db)}&batch_error={quote('请先勾选记录')}"
                ),
                status_code=303,
            )

        action_key = (action or "").strip().lower()
        if not action_key:
            return RedirectResponse(
                url=(
                    f"/knowledge/suggestions?status={quote(status)}"
                    f"&logical_db={quote(logical_db)}&batch_error={quote('请选择批量操作')}"
                ),
                status_code=303,
            )

        if action_key == "approve":
            task = batch_store.build(
                scope="knowledge",
                action=action_key,
                suggestion_ids=suggestion_ids,
                back_url=f"/knowledge/suggestions?status={quote(status)}&logical_db={quote(logical_db)}",
                done_redirect_url=f"/knowledge/suggestions?status={quote(status)}&logical_db={quote(logical_db)}",
            )
            task_id = batch_store.register(task)
            logger.info("Knowledge batch async task created: task_id=%s count=%s", task_id, len(suggestion_ids))
            run_in_background(
                run_knowledge_batch_task, task_id, action_key, suggestion_ids, reviewer_note
            )
            return RedirectResponse(
                url=(
                    f"/knowledge/suggestions/batch/tasks/{quote(task_id)}"
                    f"?status={quote(status)}&logical_db={quote(logical_db)}"
                ),
                status_code=303,
            )

        success = 0
        failed = 0
        for suggestion_id in suggestion_ids:
            try:
                if action_key == "reject":
                    knowledge_workflow.reject_suggestion(suggestion_id, reviewer_note=reviewer_note)
                elif action_key == "pending":
                    store.update_knowledge_suggestion_status(
                        suggestion_id,
                        status="pending_review",
                        reviewer_note=reviewer_note,
                    )
                else:
                    return RedirectResponse(
                        url=(
                            f"/knowledge/suggestions?status={quote(status)}"
                            f"&logical_db={quote(logical_db)}&batch_error={quote('未知批量操作')}"
                        ),
                        status_code=303,
                    )
                success += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.exception(
                    "Knowledge batch synchronous item failed: suggestion_id=%s action=%s",
                    suggestion_id,
                    action_key,
                )
                store.update_knowledge_suggestion_status(
                    suggestion_id,
                    status="failed",
                    reviewer_note=reviewer_note,
                    failure_reason=str(exc),
                )

        msg = f"批量操作完成：成功 {success}，失败 {failed}"
        return RedirectResponse(
            url=(
                f"/knowledge/suggestions?status={quote(status)}"
                f"&logical_db={quote(logical_db)}&batch_notice={quote(msg)}"
            ),
            status_code=303,
        )

    @router.get("/knowledge/suggestions/batch/tasks/{task_id}", response_class=HTMLResponse)
    def knowledge_batch_task_progress_page(request: Request, task_id: str) -> HTMLResponse:
        task = batch_store.get(task_id)
        if not task or task.get("scope") != "knowledge":
            raise HTTPException(status_code=404, detail="batch task not found")
        return templates.TemplateResponse(
            "batch_task_progress.html",
            {
                "request": request,
                "active_tab": "knowledge",
                "task_id": task_id,
                "task_title": "知识库批量回写进度",
                "status_url": f"/knowledge/suggestions/batch/tasks/{quote(task_id)}/status",
                "back_url": task.get("back_url") or "/knowledge/suggestions",
                "done_redirect_url": task.get("done_redirect_url") or "/knowledge/suggestions",
            },
        )

    @router.get("/knowledge/suggestions/batch/tasks/{task_id}/status")
    def knowledge_batch_task_status(task_id: str) -> dict[str, Any]:
        task = batch_store.get(task_id)
        if not task or task.get("scope") != "knowledge":
            raise HTTPException(status_code=404, detail="batch task not found")
        return task

    @router.get("/knowledge/suggestions/{suggestion_id}", response_class=HTMLResponse)
    def knowledge_suggestion_detail(request: Request, suggestion_id: int) -> HTMLResponse:
        row = store.get_knowledge_suggestion(suggestion_id)
        if not row:
            raise HTTPException(status_code=404, detail="knowledge suggestion not found")

        source_snapshot = safe_json(row["source_snapshot_json"], {})
        model_response = safe_json(row["model_response_json"], {})
        source_refs = safe_json(row["source_refs_json"], [])
        return templates.TemplateResponse(
            "knowledge_suggestion_detail.html",
            {
                "request": request,
                "active_tab": "knowledge",
                "row": row,
                "source_snapshot": source_snapshot,
                "source_snapshot_pretty": json.dumps(source_snapshot, ensure_ascii=False, indent=2),
                "source_refs": source_refs,
                "model_response": model_response,
                "model_response_pretty": json.dumps(model_response, ensure_ascii=False, indent=2),
                "action_notice": request.query_params.get("action_notice", "").strip(),
                "action_error": request.query_params.get("action_error", "").strip(),
            },
        )

    @router.post("/knowledge/suggestions/{suggestion_id}/regenerate")
    def regenerate_knowledge_suggestion(
        suggestion_id: int,
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        logger.info("Knowledge suggestion regenerate requested: suggestion_id=%s", suggestion_id)
        try:
            result = knowledge_workflow.regenerate_suggestion(
                suggestion_id,
                reviewer_note=reviewer_note,
                auto_apply=False,
            )
            new_suggestion_id = int(result["suggestion_id"])
            msg = f"已重新生成建议（run #{result['run_id']}）"
            return RedirectResponse(
                url=url_with_message(
                    f"/knowledge/suggestions/{new_suggestion_id}",
                    key="action_notice",
                    message=msg,
                ),
                status_code=303,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Knowledge suggestion regenerate failed: suggestion_id=%s", suggestion_id)
            try:
                store.update_knowledge_suggestion_status(
                    suggestion_id,
                    status="failed",
                    reviewer_note=reviewer_note,
                    failure_reason=str(exc),
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Knowledge suggestion regenerate failure status update also failed: suggestion_id=%s",
                    suggestion_id,
                )
            return RedirectResponse(
                url=url_with_message(
                    f"/knowledge/suggestions/{suggestion_id}",
                    key="action_error",
                    message=f"重生成失败：{exc}",
                ),
                status_code=303,
            )

    @router.post("/knowledge/suggestions/{suggestion_id}/save")
    def save_knowledge_suggestion(
        suggestion_id: int,
        proposed_markdown: str = Form(default=""),
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        logger.info("Knowledge suggestion saved: suggestion_id=%s", suggestion_id)
        store.update_knowledge_suggestion_fields(
            suggestion_id,
            proposed_markdown=proposed_markdown,
            reviewer_note=reviewer_note,
        )
        row = store.get_knowledge_suggestion(suggestion_id)
        if row and row["status"] == "rejected":
            store.update_knowledge_suggestion_status(
                suggestion_id,
                status="pending_review",
                reviewer_note=reviewer_note,
            )
        return RedirectResponse(url=f"/knowledge/suggestions/{suggestion_id}", status_code=303)

    @router.post("/knowledge/suggestions/{suggestion_id}/approve")
    def approve_knowledge_suggestion(
        suggestion_id: int,
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        try:
            knowledge_workflow.apply_suggestion(suggestion_id, reviewer_note=reviewer_note)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Knowledge suggestion approve failed: suggestion_id=%s", suggestion_id)
            store.update_knowledge_suggestion_status(
                suggestion_id,
                status="failed",
                reviewer_note=reviewer_note,
                failure_reason=str(exc),
                set_reviewed_at=True,
            )
        return RedirectResponse(url=f"/knowledge/suggestions/{suggestion_id}", status_code=303)

    @router.post("/knowledge/suggestions/{suggestion_id}/reject")
    def reject_knowledge_suggestion(
        suggestion_id: int,
        reviewer_note: str = Form(default=""),
    ) -> RedirectResponse:
        logger.info("Knowledge suggestion rejected: suggestion_id=%s", suggestion_id)
        knowledge_workflow.reject_suggestion(suggestion_id, reviewer_note=reviewer_note)
        return RedirectResponse(url=f"/knowledge/suggestions/{suggestion_id}", status_code=303)

    return router
