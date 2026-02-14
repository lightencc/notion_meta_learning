from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .config import AppConfig
from .gemini_agent import AgentSuggestion, GeminiMatcherAgent
from .logging_utils import get_logger
from .notion_gateway import NotionGateway
from .notion_ids import normalize_notion_id
from .postgres_store import PostgresStore

_PLACEHOLDER_TITLE_PATTERNS = [
    re.compile(r"^\d+$"),
    re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[-_ ]\d+)?$"),
    re.compile(r"^\d{8}(?:[-_ ]\d+)?$"),
    re.compile(r"^\d{4}-\d{4}-\d+$"),
]
_TITLE_REASON_CUTOFF = re.compile(
    r"(错因|错误原因|错误分析|原因分析|反思|复盘|易错点|订正|更正|解析|答案|思路)",
    flags=re.IGNORECASE,
)
_QUESTION_PREFIX = re.compile(r"^(题型|题目|题干|原题|问题)[:：]\s*")
_TARGET_TITLE_LEN = 12


@dataclass(slots=True)
class WorkflowSummary:
    run_id: int
    target_count: int
    suggestion_count: int
    needs_review_count: int
    failure_count: int


class AgentWorkflowService:
    TARGET_DBS = ("resources", "concepts", "skills", "mindsets")

    def __init__(self, config: AppConfig, store: PostgresStore) -> None:
        self.config = config
        self.store = store
        self._agent: GeminiMatcherAgent | None = None
        self._gateway: NotionGateway | None = None
        self.logger = get_logger(self.__class__.__name__)

    def run(self, *, limit: int | None = None) -> WorkflowSummary:
        run_id = self.store.create_workflow_run()
        self.logger.info("Error workflow started: run_id=%s limit=%s", run_id, limit)
        self.store.add_workflow_event(run_id, "start", "running", "工作流已启动")

        suggestion_count = 0
        needs_review_count = 0
        failure_count = 0

        try:
            relation_props = self._resolve_error_relation_properties()
            targets = self._find_targets(relation_props=relation_props, limit=limit)
            self.store.add_workflow_event(
                run_id,
                "scan_targets",
                "completed",
                f"找到待处理目标 {len(targets)} 条",
                detail={"target_count": len(targets)},
            )

            candidates = self._load_candidates()
            self.store.add_workflow_event(
                run_id,
                "load_candidates",
                "completed",
                "已从 PostgreSQL 加载候选集合",
                detail={
                    "resources": len(candidates["resources"]),
                    "concepts": len(candidates["concepts"]),
                    "skills": len(candidates["skills"]),
                    "mindsets": len(candidates["mindsets"]),
                    "errors": len(candidates["errors"]),
                },
            )

            for idx, target in enumerate(targets, start=1):
                try:
                    result = self._process_one_target(
                        run_id=run_id,
                        relation_props=relation_props,
                        target=target,
                        candidates=candidates,
                    )
                    saved_status = result["status"]
                    suggestion_count += 1
                    if saved_status == "needs_review":
                        needs_review_count += 1
                    self.store.add_workflow_event(
                        run_id,
                        "agent_infer",
                        "completed",
                        f"[{idx}/{len(targets)}] 已处理 {target['page_id']}",
                        detail=result,
                    )
                except Exception as exc:  # noqa: BLE001
                    failure_count += 1
                    self.logger.exception(
                        "Error workflow target failed: run_id=%s index=%s page_id=%s",
                        run_id,
                        idx,
                        target.get("page_id"),
                    )
                    self.store.add_workflow_event(
                        run_id,
                        "agent_infer",
                        "failed",
                        f"[{idx}/{len(targets)}] 处理失败 {target['page_id']}: {exc}",
                    )

            status = "completed"
            summary = (
                f"目标={len(targets)}，建议={suggestion_count}，"
                f"需复核={needs_review_count}，失败={failure_count}"
            )
            self.store.finish_workflow_run(
                run_id,
                status=status,
                target_count=len(targets),
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=failure_count,
                summary=summary,
            )
            self.logger.info(
                "Error workflow completed: run_id=%s targets=%s suggestions=%s needs_review=%s failures=%s",
                run_id,
                len(targets),
                suggestion_count,
                needs_review_count,
                failure_count,
            )
            return WorkflowSummary(
                run_id=run_id,
                target_count=len(targets),
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=failure_count,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("Error workflow fatal failure: run_id=%s", run_id)
            self.store.add_workflow_event(run_id, "fatal", "failed", str(exc))
            self.store.finish_workflow_run(
                run_id,
                status="failed",
                target_count=0,
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=failure_count + 1,
                summary=str(exc),
            )
            raise

    def apply_suggestion(self, suggestion_id: int, reviewer_note: str = "") -> dict[str, Any]:
        suggestion_row = self.store.get_agent_suggestion(suggestion_id)
        if not suggestion_row:
            raise RuntimeError(f"Suggestion {suggestion_id} not found")

        page_id = suggestion_row["error_page_id"]
        relation_props = self._resolve_error_relation_properties()
        title_prop = self._error_title_property_name()
        similar_prop = relation_props.get("errors")

        payload: dict[str, Any] = {}

        title = (suggestion_row["proposed_title"] or "").strip()
        if title:
            payload[title_prop] = {"title": [{"type": "text", "text": {"content": title[:80]}}]}

        selected = {
            "resources": _clean_id(suggestion_row["proposed_resource_id"]),
            "concepts": _clean_id(suggestion_row["proposed_concept_id"]),
            "skills": _clean_id(suggestion_row["proposed_skill_id"]),
            "mindsets": _clean_id(suggestion_row["proposed_mindset_id"]),
        }
        for logical_db, selected_id in selected.items():
            prop = relation_props.get(logical_db)
            if not prop or not selected_id:
                continue
            payload[prop] = {"relation": [{"id": selected_id}]}

        selected_similar = _safe_load_json_list(suggestion_row["proposed_similar_ids_json"])
        selected_similar = [sid for sid in selected_similar if sid and sid != page_id][:3]

        if similar_prop and selected_similar:
            page = self._gateway_client().get_page(page_id)
            existing = page.get("properties", {}).get(similar_prop, {}).get("relation", [])
            existing_ids = [item.get("id") for item in existing if item.get("id")]
            merged = []
            for pid in existing_ids + selected_similar:
                if pid and pid != page_id and pid not in merged:
                    merged.append(pid)
            payload[similar_prop] = {"relation": [{"id": pid} for pid in merged[:50]]}

        if not payload:
            raise RuntimeError("Nothing to update for this suggestion")

        self._gateway_client().update_page_properties(page_id=page_id, properties=payload)
        self.store.update_suggestion_status(
            suggestion_id,
            status="applied",
            reviewer_note=reviewer_note,
            set_reviewed_at=True,
            set_applied_at=True,
        )
        self.logger.info(
            "Error suggestion applied: suggestion_id=%s page_id=%s fields=%s",
            suggestion_id,
            page_id,
            list(payload.keys()),
        )
        return payload

    def reject_suggestion(self, suggestion_id: int, reviewer_note: str = "") -> None:
        self.store.update_suggestion_status(
            suggestion_id,
            status="rejected",
            reviewer_note=reviewer_note,
            set_reviewed_at=True,
        )
        self.logger.info("Error suggestion rejected: suggestion_id=%s", suggestion_id)

    def pending_target_count(self) -> int:
        relation_props = self._resolve_error_relation_properties()
        targets = self._find_targets(relation_props=relation_props, limit=None)
        return len(targets)

    def regenerate_suggestion(
        self,
        suggestion_id: int,
        *,
        reviewer_note: str = "",
        auto_apply: bool = False,
    ) -> dict[str, Any]:
        existing = self.store.get_agent_suggestion(suggestion_id)
        if not existing:
            raise RuntimeError(f"suggestion {suggestion_id} not found")

        page_id = str(existing["error_page_id"])
        run_id = self.store.create_workflow_run()
        self.store.add_workflow_event(
            run_id,
            "start",
            "running",
            f"单条重生成启动：{page_id}",
        )
        self.logger.info(
            "Error single regenerate started: run_id=%s suggestion_id=%s page_id=%s auto_apply=%s",
            run_id,
            suggestion_id,
            page_id,
            auto_apply,
        )

        suggestion_count = 0
        needs_review_count = 0
        try:
            relation_props = self._resolve_error_relation_properties()
            target = self._build_target_for_page(
                page_id=page_id,
                relation_props=relation_props,
            )
            self.store.add_workflow_event(
                run_id,
                "scan_targets",
                "completed",
                f"单条目标：{page_id}",
                detail={"target_count": 1},
            )

            candidates = self._load_candidates()
            self.store.add_workflow_event(
                run_id,
                "load_candidates",
                "completed",
                "已加载候选集合",
                detail={
                    "resources": len(candidates["resources"]),
                    "concepts": len(candidates["concepts"]),
                    "skills": len(candidates["skills"]),
                    "mindsets": len(candidates["mindsets"]),
                    "errors": len(candidates["errors"]),
                },
            )

            generated = self._process_one_target(
                run_id=run_id,
                relation_props=relation_props,
                target=target,
                candidates=candidates,
            )
            new_suggestion_id = int(generated["suggestion_id"])
            suggestion_count = 1
            if generated["status"] == "needs_review":
                needs_review_count = 1
            self.store.add_workflow_event(
                run_id,
                "agent_infer",
                "completed",
                f"已重生成建议：{page_id}",
                detail=generated,
            )

            if reviewer_note:
                self.store.update_suggestion_status(
                    new_suggestion_id,
                    status=generated["status"],
                    reviewer_note=reviewer_note,
                )

            apply_result: dict[str, Any] | None = None
            if auto_apply:
                apply_result = self.apply_suggestion(new_suggestion_id, reviewer_note=reviewer_note)

            summary = (
                f"单条重生成完成：suggestion_id={new_suggestion_id}"
                f"{'，已回写 Notion' if auto_apply else ''}"
            )
            self.store.finish_workflow_run(
                run_id,
                status="completed",
                target_count=1,
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=0,
                summary=summary,
            )
            self.logger.info(
                "Error single regenerate completed: run_id=%s suggestion_id=%s auto_apply=%s",
                run_id,
                new_suggestion_id,
                auto_apply,
            )
            return {
                "run_id": run_id,
                "suggestion_id": new_suggestion_id,
                "status": generated["status"],
                "applied": bool(auto_apply),
                "apply_result": apply_result or {},
            }
        except Exception as exc:  # noqa: BLE001
            self.logger.exception(
                "Error single regenerate failed: run_id=%s suggestion_id=%s",
                run_id,
                suggestion_id,
            )
            self.store.add_workflow_event(run_id, "fatal", "failed", str(exc))
            self.store.finish_workflow_run(
                run_id,
                status="failed",
                target_count=1,
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=1,
                summary=str(exc),
            )
            raise

    def _process_one_target(
        self,
        *,
        run_id: int,
        relation_props: dict[str, str],
        target: dict[str, Any],
        candidates: dict[str, list[dict[str, str]]],
    ) -> dict[str, Any]:
        page_id = target["page_id"]
        error_candidates = [item for item in candidates["errors"] if item["id"] != page_id]

        agent_payload = {
            "task": "link_error_record",
            "constraints": {
                "resource_single": True,
                "concept_single": True,
                "skill_single": True,
                "mindset_single": True,
                "max_similar_errors": 3,
                "title_rule": "new_title must contain only question type and stem, no mistake reason",
            },
            "error_record": {
                "page_id": page_id,
                "title": target["title"],
                "url": target["url"],
                "property_text": target["property_text"],
                "plain_text": target["plain_text"],
                "existing_relations": target["existing_relations"],
                "missing_relations": target["missing_relations"],
            },
            "candidates": {
                "resources": candidates["resources"],
                "concepts": candidates["concepts"],
                "skills": candidates["skills"],
                "mindsets": candidates["mindsets"],
                "similar_errors": error_candidates,
            },
            "output_format": {
                "new_title": "string",
                "resource_id": "string|null",
                "concept_id": "string|null",
                "skill_id": "string|null",
                "mindset_id": "string|null",
                "similar_error_ids": "string[]",
                "confidence": "number(0-1)",
                "reasoning_summary": "string",
            },
        }

        suggestion = self._agent_client().suggest(agent_payload)
        validated = self._validate_suggestion(
            target=target,
            suggestion=suggestion,
            candidates={
                "resources": {item["id"] for item in candidates["resources"]},
                "concepts": {item["id"] for item in candidates["concepts"]},
                "skills": {item["id"] for item in candidates["skills"]},
                "mindsets": {item["id"] for item in candidates["mindsets"]},
                "errors": {item["id"] for item in error_candidates},
            },
        )

        status = validated["status"]
        suggestion_id = self.store.upsert_agent_suggestion(
            run_id=run_id,
            error_page_id=page_id,
            error_title=target["title"],
            status=status,
            confidence=validated["confidence"],
            proposed_title=validated["new_title"],
            proposed_resource_id=validated["resource_id"],
            proposed_concept_id=validated["concept_id"],
            proposed_skill_id=validated["skill_id"],
            proposed_mindset_id=validated["mindset_id"],
            proposed_similar_ids=validated["similar_error_ids"],
            reasoning_summary=validated["reasoning_summary"],
            validation_notes=validated["validation_notes"],
            source_snapshot={
                "page_id": page_id,
                "title": target["title"],
                "url": target["url"],
                "property_text": target["property_text"],
                "plain_text": target["plain_text"],
                "existing_relations": target["existing_relations"],
                "missing_relations": target["missing_relations"],
                "relation_properties": relation_props,
            },
            candidates={
                "resources": candidates["resources"],
                "concepts": candidates["concepts"],
                "skills": candidates["skills"],
                "mindsets": candidates["mindsets"],
                "similar_errors": error_candidates,
            },
            model_response=suggestion.raw_response,
            failure_reason="" if status != "needs_review" else validated["validation_notes"],
        )
        return {
            "suggestion_id": suggestion_id,
            "page_id": page_id,
            "status": status,
            "confidence": validated["confidence"],
            "title": validated["new_title"],
        }

    def _validate_suggestion(
        self,
        *,
        target: dict[str, Any],
        suggestion: AgentSuggestion,
        candidates: dict[str, set[str]],
    ) -> dict[str, Any]:
        notes: list[str] = []

        def keep_if_valid(value: str | None, key: str) -> str | None:
            if not value:
                notes.append(f"missing {key}")
                return None
            if value not in candidates[key]:
                notes.append(f"invalid {key}: {value}")
                return None
            return value

        resource_id = keep_if_valid(_clean_id(suggestion.resource_id), "resources")
        concept_id = keep_if_valid(_clean_id(suggestion.concept_id), "concepts")
        skill_id = keep_if_valid(_clean_id(suggestion.skill_id), "skills")
        mindset_id = keep_if_valid(_clean_id(suggestion.mindset_id), "mindsets")

        similar_ids: list[str] = []
        for sid in suggestion.similar_error_ids:
            clean_sid = _clean_id(sid)
            if not clean_sid or clean_sid == target["page_id"]:
                continue
            if clean_sid not in candidates["errors"]:
                continue
            if clean_sid in similar_ids:
                continue
            similar_ids.append(clean_sid)
            if len(similar_ids) >= 3:
                break

        new_title = self._normalize_question_title(suggestion.new_title)
        if not new_title:
            new_title = self._extract_question_title_from_source(target)
        if not new_title:
            new_title = target["title"]
        confidence = max(0.0, min(1.0, suggestion.confidence))

        status = "pending_review"
        if notes:
            status = "needs_review"
        if confidence < self.config.confidence_threshold:
            status = "needs_review"
            notes.append(
                f"low confidence ({confidence:.2f}) < threshold ({self.config.confidence_threshold:.2f})"
            )

        return {
            "status": status,
            "new_title": new_title,
            "resource_id": resource_id,
            "concept_id": concept_id,
            "skill_id": skill_id,
            "mindset_id": mindset_id,
            "similar_error_ids": similar_ids,
            "confidence": confidence,
            "reasoning_summary": suggestion.reasoning_summary,
            "validation_notes": "; ".join(notes),
        }

    def _find_targets(self, *, relation_props: dict[str, str], limit: int | None) -> list[dict[str, Any]]:
        rows = [row for row in self.store.get_pages("errors") if int(row["archived"]) == 0]
        relations_map = self.store.get_relations_map("errors")

        targets: list[dict[str, Any]] = []
        for row in rows:
            title = (row["title"] or "").strip()
            if not self._is_placeholder_title(title):
                continue

            rels = relations_map.get(row["page_id"], {})
            missing: list[str] = []
            existing: dict[str, list[str]] = {}
            for logical_db in self.TARGET_DBS:
                prop = relation_props.get(logical_db)
                if not prop:
                    continue
                existing_ids = sorted(list(rels.get(prop, set())))
                existing[logical_db] = existing_ids
                if len(existing_ids) < 1:
                    missing.append(logical_db)

            if not missing:
                continue

            targets.append(
                {
                    "page_id": row["page_id"],
                    "title": title,
                    "url": row["url"],
                    "property_text": row["property_text"],
                    "plain_text": row["plain_text"],
                    "existing_relations": existing,
                    "missing_relations": missing,
                }
            )

            if limit is not None and len(targets) >= limit:
                break

        return targets

    def _build_target_for_page(
        self,
        *,
        page_id: str,
        relation_props: dict[str, str],
    ) -> dict[str, Any]:
        row = self.store.get_page_by_id(page_id)
        if not row:
            raise RuntimeError(f"error page not found in PostgreSQL: {page_id}")
        if int(row["archived"]) != 0:
            raise RuntimeError(f"error page is archived: {page_id}")

        rels = self.store.get_relations_map("errors").get(page_id, {})
        missing: list[str] = []
        existing: dict[str, list[str]] = {}
        for logical_db in self.TARGET_DBS:
            prop = relation_props.get(logical_db)
            if not prop:
                continue
            existing_ids = sorted(list(rels.get(prop, set())))
            existing[logical_db] = existing_ids
            if len(existing_ids) < 1:
                missing.append(logical_db)

        return {
            "page_id": row["page_id"],
            "title": (row["title"] or "").strip(),
            "url": row["url"],
            "property_text": row["property_text"],
            "plain_text": row["plain_text"],
            "existing_relations": existing,
            "missing_relations": missing,
        }

    def _load_candidates(self) -> dict[str, list[dict[str, str]]]:
        out: dict[str, list[dict[str, str]]] = {}
        for logical_db in self.TARGET_DBS + ("errors",):
            rows = [row for row in self.store.get_pages(logical_db) if int(row["archived"]) == 0]
            out[logical_db] = [
                {
                    "id": row["page_id"],
                    "title": (row["title"] or "").strip(),
                }
                for row in rows
                if (row["title"] or "").strip()
            ]
        return out

    def _resolve_error_relation_properties(self) -> dict[str, str]:
        row = self.store.get_database_row("errors")
        if not row:
            raise RuntimeError("No 'errors' schema found in PostgreSQL. Run sync first.")

        schema = json.loads(row["schema_json"])
        properties = schema.get("properties", {})

        db_id_to_logical = {
            normalize_notion_id(db_id): logical
            for logical, db_id in self.config.databases.items()
        }
        mapping: dict[str, str] = {}

        for prop_name, prop_schema in properties.items():
            if prop_schema.get("type") != "relation":
                continue
            relation_cfg = prop_schema.get("relation", {})
            target_db_id = normalize_notion_id(relation_cfg.get("database_id", ""))
            logical_db = db_id_to_logical.get(target_db_id)
            if logical_db and logical_db not in mapping:
                mapping[logical_db] = prop_name

        return mapping

    def _error_title_property_name(self) -> str:
        row = self.store.get_database_row("errors")
        if not row:
            raise RuntimeError("No 'errors' schema found in PostgreSQL. Run sync first.")
        return str(row["title_property"])

    def _agent_client(self) -> GeminiMatcherAgent:
        if self._agent is None:
            self._agent = GeminiMatcherAgent(
                api_key=self.config.google_api_key(),
                model=self.config.gemini_model,
                temperature=self.config.temperature,
            )
        return self._agent

    def _gateway_client(self) -> NotionGateway:
        if self._gateway is None:
            self._gateway = NotionGateway(self.config.notion_token())
        return self._gateway

    @staticmethod
    def _is_placeholder_title(title: str) -> bool:
        name = (title or "").strip()
        if not name:
            return True
        return any(pattern.match(name) for pattern in _PLACEHOLDER_TITLE_PATTERNS)

    @staticmethod
    def _normalize_question_title(value: str) -> str:
        text = (value or "").replace("\r", "\n").strip()
        if not text:
            return ""
        text = text.split("\n", 1)[0].strip()
        text = _QUESTION_PREFIX.sub("", text)
        for sep in ("；", ";", "。", "|", "｜"):
            if sep in text:
                text = text.split(sep, 1)[0].strip()
        hit = _TITLE_REASON_CUTOFF.search(text)
        if hit and hit.start() > 0:
            text = text[: hit.start()].strip()
        text = re.sub(r"\s+", " ", text)
        text = text.strip(" -—:：,，。；;()（）[]【】")
        return text[:_TARGET_TITLE_LEN]

    def _extract_question_title_from_source(self, target: dict[str, Any]) -> str:
        candidates: list[str] = []
        property_text = str(target.get("property_text") or "").strip()
        if property_text:
            for row in property_text.splitlines():
                key, _, value = row.partition(":")
                k = key.strip()
                v = value.strip()
                if not v:
                    continue
                if any(token in k for token in ("题目", "题干", "题型", "原题", "问题")):
                    candidates.append(v)
                elif not any(token in k for token in ("错因", "原因", "反思", "解析", "答案")):
                    candidates.append(v)

        plain_text = str(target.get("plain_text") or "").strip()
        if plain_text:
            candidates.extend(part.strip() for part in plain_text.splitlines() if part.strip())

        for raw in candidates:
            normalized = self._normalize_question_title(raw)
            if not normalized:
                continue
            if _TITLE_REASON_CUTOFF.search(normalized):
                continue
            if len(normalized) < 4:
                continue
            return normalized
        return ""


def _safe_load_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [str(item).strip() for item in data if str(item).strip()]


def _clean_id(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
