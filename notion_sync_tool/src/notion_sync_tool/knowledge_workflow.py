from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import AppConfig
from .knowledge_agent import KnowledgeContentAgent, KnowledgeSuggestion
from .logging_utils import get_logger
from .notion_gateway import NotionGateway
from .notion_ids import normalize_notion_id
from .postgres_store import PostgresStore

LESSON_PATTERN = re.compile(r"L(\d{2})", flags=re.IGNORECASE)
RESOURCE_MAPPING_FILENAME = "昂立四年级上_关联关系映射.csv"
RESOURCE_TEMPLATE_NAME = "math_lesson_note.md"
KNOWLEDGE_PROMPT_TEMPLATE_NAME = "knowledge_agent_prompt.md"
TARGET_DBS = ("resources", "concepts", "skills", "mindsets")
TEMPLATE_NAME_BY_DB = {
    "resources": RESOURCE_TEMPLATE_NAME,
    "concepts": "concept_note.md",
    "skills": "skill_note.md",
    "mindsets": "mindset_note.md",
}
CHARS_PER_RESOURCE_NOTE = 2400
MAX_ATTACHMENTS_PER_REQUEST = 6


@dataclass(slots=True)
class KnowledgeRunOptions:
    limit: int | None = 20


@dataclass(slots=True)
class KnowledgeWorkflowSummary:
    run_id: int
    target_count: int
    suggestion_count: int
    needs_review_count: int
    failure_count: int


@dataclass(slots=True)
class KnowledgeTarget:
    logical_db: str
    page_id: str
    title: str
    property_text: str
    lesson_code: str
    source_doc_path: str


class KnowledgeWorkflowService:
    def __init__(self, config: AppConfig, store: PostgresStore) -> None:
        self.config = config
        self.store = store
        self._agent: KnowledgeContentAgent | None = None
        self._gateway: NotionGateway | None = None
        self.logger = get_logger(self.__class__.__name__)

        root = Path(__file__).resolve().parents[3]
        self.repo_root = root
        self.template_dir = Path(
            os.getenv("KNOWLEDGE_TEMPLATE_DIR", str(root / "docs" / "template"))
        ).expanduser()
        self.mapping_csv_path = Path(
            os.getenv(
                "KNOWLEDGE_MAPPING_CSV",
                str(root / "notion_import" / RESOURCE_MAPPING_FILENAME),
            )
        ).expanduser()

    def run(self, options: KnowledgeRunOptions | None = None) -> KnowledgeWorkflowSummary:
        opts = options or KnowledgeRunOptions()
        run_id = self.store.create_knowledge_run()
        self.logger.info("Knowledge workflow started: run_id=%s limit=%s", run_id, opts.limit)
        self.store.add_knowledge_event(run_id, "start", "running", "知识库整理工作流已启动")

        suggestion_count = 0
        needs_review_count = 0
        failure_count = 0

        try:
            templates = self._load_templates()
            prompt_template = self._load_prompt_template()
            mapping_rows = self._load_mapping_rows()
            source_attachments = self._load_source_attachments(mapping_rows)
            targets = self._find_targets(mapping_rows, limit=opts.limit)

            self.store.add_knowledge_event(
                run_id,
                "scan_targets",
                "completed",
                f"找到待整理目标 {len(targets)} 条",
                detail={"target_count": len(targets)},
            )

            for idx, target in enumerate(targets, start=1):
                try:
                    suggestion = self._process_target(
                        run_id=run_id,
                        target=target,
                        mapping_rows=mapping_rows,
                        templates=templates,
                        prompt_template=prompt_template,
                        source_attachments=source_attachments,
                    )
                    suggestion_count += 1
                    if suggestion["status"] == "needs_review":
                        needs_review_count += 1
                    self.store.add_knowledge_event(
                        run_id,
                        "agent_infer",
                        "completed",
                        f"[{idx}/{len(targets)}] 已处理 {target.logical_db}:{target.title}",
                        detail=suggestion,
                    )
                except Exception as exc:  # noqa: BLE001
                    failure_count += 1
                    self.logger.exception(
                        "Knowledge workflow target failed: run_id=%s index=%s logical_db=%s page_id=%s",
                        run_id,
                        idx,
                        target.logical_db,
                        target.page_id,
                    )
                    self.store.add_knowledge_event(
                        run_id,
                        "agent_infer",
                        "failed",
                        f"[{idx}/{len(targets)}] 处理失败 {target.logical_db}:{target.title}: {exc}",
                    )

            summary = (
                f"目标={len(targets)}，建议={suggestion_count}，"
                f"需复核={needs_review_count}，失败={failure_count}"
            )
            self.store.finish_knowledge_run(
                run_id,
                status="completed",
                target_count=len(targets),
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=failure_count,
                summary=summary,
            )
            self.logger.info(
                "Knowledge workflow completed: run_id=%s targets=%s suggestions=%s needs_review=%s failures=%s",
                run_id,
                len(targets),
                suggestion_count,
                needs_review_count,
                failure_count,
            )
            return KnowledgeWorkflowSummary(
                run_id=run_id,
                target_count=len(targets),
                suggestion_count=suggestion_count,
                needs_review_count=needs_review_count,
                failure_count=failure_count,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("Knowledge workflow fatal failure: run_id=%s", run_id)
            self.store.add_knowledge_event(run_id, "fatal", "failed", str(exc))
            self.store.finish_knowledge_run(
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
        row = self.store.get_knowledge_suggestion(suggestion_id)
        if not row:
            raise RuntimeError(f"knowledge suggestion {suggestion_id} not found")
        markdown = (row["proposed_markdown"] or "").strip()
        if not markdown:
            raise RuntimeError("suggestion markdown is empty")
        result = self._gateway_client().replace_page_with_markdown(row["page_id"], markdown)
        self.store.update_knowledge_suggestion_status(
            suggestion_id,
            status="applied",
            reviewer_note=reviewer_note,
            set_reviewed_at=True,
            set_applied_at=True,
        )
        self.logger.info(
            "Knowledge suggestion applied: suggestion_id=%s logical_db=%s page_id=%s deleted_blocks=%s appended_blocks=%s",
            suggestion_id,
            row["logical_db"],
            row["page_id"],
            result.get("deleted_blocks"),
            result.get("appended_blocks"),
        )
        return result

    def reject_suggestion(self, suggestion_id: int, reviewer_note: str = "") -> None:
        self.store.update_knowledge_suggestion_status(
            suggestion_id,
            status="rejected",
            reviewer_note=reviewer_note,
            set_reviewed_at=True,
        )
        self.logger.info("Knowledge suggestion rejected: suggestion_id=%s", suggestion_id)

    def pending_target_count(self) -> int:
        targets = self._find_targets(self._load_mapping_rows(), limit=None)
        return len(targets)

    def regenerate_suggestion(
        self,
        suggestion_id: int,
        *,
        reviewer_note: str = "",
        auto_apply: bool = False,
    ) -> dict[str, Any]:
        existing = self.store.get_knowledge_suggestion(suggestion_id)
        if not existing:
            raise RuntimeError(f"knowledge suggestion {suggestion_id} not found")

        logical_db = str(existing["logical_db"])
        page_id = str(existing["page_id"])
        run_id = self.store.create_knowledge_run()
        self.store.add_knowledge_event(
            run_id,
            "start",
            "running",
            f"单条重生成启动：{logical_db}:{page_id}",
        )
        self.logger.info(
            "Knowledge single regenerate started: run_id=%s suggestion_id=%s logical_db=%s page_id=%s auto_apply=%s",
            run_id,
            suggestion_id,
            logical_db,
            page_id,
            auto_apply,
        )

        try:
            templates = self._load_templates()
            prompt_template = self._load_prompt_template()
            mapping_rows = self._load_mapping_rows()
            source_attachments = self._load_source_attachments(mapping_rows)
            target = self._build_target_for_page(
                logical_db=logical_db,
                page_id=page_id,
                mapping_rows=mapping_rows,
            )

            self.store.add_knowledge_event(
                run_id,
                "scan_targets",
                "completed",
                f"单条目标：{target.logical_db}:{target.title}",
                detail={"target_count": 1},
            )

            generated = self._process_target(
                run_id=run_id,
                target=target,
                mapping_rows=mapping_rows,
                templates=templates,
                prompt_template=prompt_template,
                source_attachments=source_attachments,
            )
            new_suggestion_id = int(generated["suggestion_id"])
            self.store.add_knowledge_event(
                run_id,
                "agent_infer",
                "completed",
                f"已重生成建议：{target.logical_db}:{target.title}",
                detail=generated,
            )

            refreshed = self.store.get_knowledge_suggestion(new_suggestion_id)
            if refreshed and reviewer_note:
                self.store.update_knowledge_suggestion_fields(
                    new_suggestion_id,
                    proposed_markdown=str(refreshed["proposed_markdown"] or ""),
                    reviewer_note=reviewer_note,
                )

            apply_result: dict[str, Any] | None = None
            if auto_apply:
                apply_result = self.apply_suggestion(new_suggestion_id, reviewer_note=reviewer_note)

            summary = (
                f"单条重生成完成：suggestion_id={new_suggestion_id}"
                f"{'，已回写 Notion' if auto_apply else ''}"
            )
            self.store.finish_knowledge_run(
                run_id,
                status="completed",
                target_count=1,
                suggestion_count=1,
                needs_review_count=1 if generated["status"] == "needs_review" else 0,
                failure_count=0,
                summary=summary,
            )
            self.logger.info(
                "Knowledge single regenerate completed: run_id=%s suggestion_id=%s auto_apply=%s",
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
                "Knowledge single regenerate failed: run_id=%s suggestion_id=%s",
                run_id,
                suggestion_id,
            )
            self.store.add_knowledge_event(run_id, "fatal", "failed", str(exc))
            self.store.finish_knowledge_run(
                run_id,
                status="failed",
                target_count=1,
                suggestion_count=0,
                needs_review_count=0,
                failure_count=1,
                summary=str(exc),
            )
            raise

    def _process_target(
        self,
        *,
        run_id: int,
        target: KnowledgeTarget,
        mapping_rows: list[dict[str, Any]],
        templates: dict[str, str],
        prompt_template: str,
        source_attachments: dict[str, str],
    ) -> dict[str, Any]:
        template = templates[TEMPLATE_NAME_BY_DB[target.logical_db]]
        payload = self._build_payload(
            target=target,
            mapping_rows=mapping_rows,
            template=template,
            prompt_template=prompt_template,
            source_attachments=source_attachments,
        )
        attachment_paths = [
            str(item).strip() for item in payload.get("source_attachments", []) if str(item).strip()
        ]
        agent_result = self._agent_client().suggest(payload, attachment_paths=attachment_paths)
        validated = self._validate_suggestion(target=target, suggestion=agent_result, payload=payload)

        suggestion_id = self.store.upsert_knowledge_suggestion(
            run_id=run_id,
            logical_db=target.logical_db,
            page_id=target.page_id,
            page_title=target.title,
            lesson_code=target.lesson_code,
            source_doc_path=target.source_doc_path,
            source_refs=validated["source_refs"],
            status=validated["status"],
            confidence=validated["confidence"],
            proposed_markdown=validated["content_markdown"],
            reasoning_summary=validated["reasoning_summary"],
            validation_notes=validated["validation_notes"],
            source_snapshot=payload,
            model_response=agent_result.raw_response,
            failure_reason="" if validated["status"] != "needs_review" else validated["validation_notes"],
        )
        return {
            "suggestion_id": suggestion_id,
            "logical_db": target.logical_db,
            "page_id": target.page_id,
            "title": target.title,
            "status": validated["status"],
            "confidence": validated["confidence"],
        }

    def _validate_suggestion(
        self,
        *,
        target: KnowledgeTarget,
        suggestion: KnowledgeSuggestion,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        notes: list[str] = []
        markdown = (suggestion.content_markdown or "").strip()
        if not markdown:
            notes.append("agent 未返回 markdown 内容")
        confidence = max(0.0, min(1.0, float(suggestion.confidence)))
        if confidence < self.config.confidence_threshold:
            notes.append(
                f"low confidence ({confidence:.2f}) < threshold ({self.config.confidence_threshold:.2f})"
            )
        source_refs = [item for item in suggestion.source_refs if item]
        if not source_refs:
            fallback_refs = payload.get("source_refs", [])
            source_refs = [str(item) for item in fallback_refs if str(item).strip()]
            if not source_refs:
                notes.append("缺少来源引用")
        expected_attachments = [
            str(item).strip() for item in payload.get("source_attachments", []) if str(item).strip()
        ]
        if expected_attachments and not suggestion.uploaded_attachments:
            notes.append("来源文档附件未成功上传")
        if suggestion.attachment_errors:
            notes.append(f"附件上传异常 {len(suggestion.attachment_errors)} 项")

        status = "needs_review" if notes else "pending_review"
        return {
            "status": status,
            "content_markdown": markdown,
            "confidence": confidence,
            "reasoning_summary": suggestion.reasoning_summary,
            "source_refs": source_refs,
            "validation_notes": "; ".join(notes),
            "target": target.title,
        }

    def _build_payload(
        self,
        *,
        target: KnowledgeTarget,
        mapping_rows: list[dict[str, Any]],
        template: str,
        prompt_template: str,
        source_attachments: dict[str, str],
    ) -> dict[str, Any]:
        if target.logical_db == "resources":
            mapping_row = self._find_mapping_for_resource(
                title=target.title,
                lesson_code=target.lesson_code,
                mapping_rows=mapping_rows,
            )
            lesson_code = target.lesson_code or _normalize_lesson_code(
                str(mapping_row.get("课次") if mapping_row else "")
            )
            doc_path = target.source_doc_path or str(mapping_row.get("文档路径") if mapping_row else "").strip()
            doc_attachment = source_attachments.get(lesson_code, "")
            linked_knowledge = self._mapping_related_knowledge(mapping_row)
            relation_context = self._relation_context_for_page(target.page_id)

            source_refs: list[str] = []
            if doc_path:
                source_refs.append(doc_path)
            if lesson_code:
                source_refs.append(lesson_code)
            source_refs.append(target.title)
            for key in ("concepts", "skills", "mindsets"):
                source_refs.extend(item.get("title", "") for item in linked_knowledge.get(key, []))
            attachment_paths = _unique_list([doc_attachment] if doc_attachment else [])[
                :MAX_ATTACHMENTS_PER_REQUEST
            ]

            return {
                "task": "generate_resource_page_content",
                "target": {
                    "logical_db": target.logical_db,
                    "page_id": target.page_id,
                    "title": target.title,
                    "lesson_code": lesson_code,
                    "property_text": target.property_text,
                },
                "template_markdown": template,
                "prompt_template": prompt_template,
                "source_refs": _unique_list(source_refs),
                "source_attachments": attachment_paths,
                "source_context": {
                    "doc_path": doc_path,
                    "doc_attachment_path": doc_attachment,
                    "linked_knowledge": linked_knowledge,
                    "existing_relations": relation_context,
                },
                "constraints": {
                    "must_include_source": True,
                    "language": "zh-CN",
                },
            }

        related_lessons = self._find_related_lessons(
            logical_db=target.logical_db,
            title=target.title,
            mapping_rows=mapping_rows,
        )
        related_resources = self._related_resources_for_target(
            target=target,
            mapping_rows=mapping_rows,
            related_lessons=related_lessons,
            source_attachments=source_attachments,
        )
        lesson_contexts: list[dict[str, Any]] = []
        source_refs: list[str] = []
        attachment_paths: list[str] = []
        for resource in related_resources:
            lesson_code = str(resource.get("lesson_code") or "").strip()
            resource_title = str(resource.get("resource_title") or "").strip()
            doc_path = str(resource.get("doc_path") or "").strip()
            doc_attachment = str(resource.get("doc_attachment") or "").strip()
            resource_note = str(resource.get("resource_note") or "")
            lesson_contexts.append(
                {
                    "lesson_code": lesson_code,
                    "resource_title": resource_title,
                    "doc_path": doc_path,
                    "doc_attachment_path": doc_attachment,
                    "resource_note": resource_note[:CHARS_PER_RESOURCE_NOTE],
                }
            )
            if resource_title:
                source_refs.append(resource_title)
            if doc_path:
                source_refs.append(doc_path)
            if lesson_code:
                source_refs.append(lesson_code)
            if doc_attachment:
                attachment_paths.append(doc_attachment)

        return {
            "task": f"generate_{target.logical_db}_page_content",
            "target": {
                "logical_db": target.logical_db,
                "page_id": target.page_id,
                "title": target.title,
                "lesson_codes": related_lessons,
                "property_text": target.property_text,
            },
            "template_markdown": template,
            "prompt_template": prompt_template,
            "source_refs": _unique_list(source_refs),
            "source_attachments": _unique_list(attachment_paths)[:MAX_ATTACHMENTS_PER_REQUEST],
            "source_context": {
                "related_resources": lesson_contexts,
                "existing_relations": self._relation_context_for_page(target.page_id),
            },
            "constraints": {
                "must_include_source": True,
                "language": "zh-CN",
                "if_missing_data_mark_pending": True,
            },
        }

    def _find_targets(self, mapping_rows: list[dict[str, Any]], limit: int | None) -> list[KnowledgeTarget]:
        mapping_by_title = {
            str(row.get("资料库名称") or "").strip(): row
            for row in mapping_rows
            if str(row.get("资料库名称") or "").strip()
        }
        out: list[KnowledgeTarget] = []

        for logical_db in TARGET_DBS:
            rows = [row for row in self.store.get_pages(logical_db) if int(row["archived"]) == 0]
            for row in rows:
                title = (row["title"] or "").strip()
                if not title:
                    continue
                if (row["plain_text"] or "").strip():
                    continue
                existing = self.store.get_knowledge_suggestion_by_page(logical_db, row["page_id"])
                if existing and existing["status"] in {"pending_review", "needs_review", "applied"}:
                    continue

                lesson_code = _extract_lesson_code(title)
                source_doc_path = ""
                if logical_db == "resources":
                    mapped = mapping_by_title.get(title)
                    if mapped:
                        source_doc_path = str(mapped.get("文档路径") or "").strip()
                        if not lesson_code:
                            lesson_code = _normalize_lesson_code(str(mapped.get("课次") or ""))

                out.append(
                    KnowledgeTarget(
                        logical_db=logical_db,
                        page_id=row["page_id"],
                        title=title,
                        property_text=(row["property_text"] or "").strip(),
                        lesson_code=lesson_code,
                        source_doc_path=source_doc_path,
                    )
                )
                if limit is not None and len(out) >= limit:
                    return out
        return out

    def _build_target_for_page(
        self,
        *,
        logical_db: str,
        page_id: str,
        mapping_rows: list[dict[str, Any]],
    ) -> KnowledgeTarget:
        row = self.store.get_page_by_id(page_id)
        if not row:
            raise RuntimeError(f"page not found in PostgreSQL: {page_id}")
        if str(row["logical_db"]) != logical_db:
            raise RuntimeError(
                f"page logical_db mismatch: expected={logical_db} actual={row['logical_db']}"
            )

        title = str(row["title"] or "").strip()
        lesson_code = _extract_lesson_code(title)
        source_doc_path = ""
        if logical_db == "resources":
            mapped = self._find_mapping_for_resource(
                title=title,
                lesson_code=lesson_code,
                mapping_rows=mapping_rows,
            )
            if mapped:
                source_doc_path = str(mapped.get("文档路径") or "").strip()
                if not lesson_code:
                    lesson_code = _normalize_lesson_code(str(mapped.get("课次") or ""))

        return KnowledgeTarget(
            logical_db=logical_db,
            page_id=page_id,
            title=title,
            property_text=str(row["property_text"] or "").strip(),
            lesson_code=lesson_code,
            source_doc_path=source_doc_path,
        )

    def _find_related_lessons(
        self,
        *,
        logical_db: str,
        title: str,
        mapping_rows: list[dict[str, Any]],
    ) -> list[str]:
        key_by_db = {
            "concepts": "概念 concept",
            "skills": "技能名",
            "mindsets": "思想名",
        }
        key = key_by_db.get(logical_db, "")
        if not key:
            return []

        out: list[str] = []
        title_norm = _normalize_text(title)
        for row in mapping_rows:
            lesson = _normalize_lesson_code(str(row.get("课次") or ""))
            if not lesson:
                continue
            values = _split_csv_list(str(row.get(key) or ""))
            values_norm = {_normalize_text(item) for item in values if item}
            if title_norm in values_norm:
                out.append(lesson)
        return sorted(_unique_list(out))

    def _find_mapping_for_resource(
        self,
        *,
        title: str,
        lesson_code: str,
        mapping_rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        title_norm = _normalize_text(title)
        for row in mapping_rows:
            mapped_title = str(row.get("资料库名称") or "").strip()
            if mapped_title and _normalize_text(mapped_title) == title_norm:
                return row
        if lesson_code:
            return _find_mapping_by_lesson(mapping_rows, lesson_code)
        return None

    def _find_page_by_title(self, logical_db: str, title: str) -> dict[str, Any] | None:
        title_norm = _normalize_text(title)
        if not title_norm:
            return None
        for row in self.store.get_pages(logical_db):
            if int(row["archived"]) == 1:
                continue
            row_title = str(row["title"] or "").strip()
            if row_title and _normalize_text(row_title) == title_norm:
                return dict(row)
        return None

    def _mapping_related_knowledge(self, mapping_row: dict[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {"concepts": [], "skills": [], "mindsets": []}
        if not mapping_row:
            return out
        key_map = {
            "concepts": "概念 concept",
            "skills": "技能名",
            "mindsets": "思想名",
        }
        for logical_db, key in key_map.items():
            names = _split_csv_list(str(mapping_row.get(key) or ""))
            for name in names:
                page = self._find_page_by_title(logical_db, name)
                out[logical_db].append(
                    {
                        "title": name,
                        "page_id": str(page.get("page_id") if page else ""),
                    }
                )
        return out

    def _resolve_relation_properties(self, logical_db: str) -> dict[str, str]:
        row = self.store.get_database_row(logical_db)
        if not row:
            return {}
        schema_raw = str(row["schema_json"] or "").strip()
        if not schema_raw:
            return {}
        try:
            schema = json.loads(schema_raw)
        except Exception:  # noqa: BLE001
            return {}
        properties = schema.get("properties", {})
        db_id_to_logical = {
            normalize_notion_id(db_id): db_name for db_name, db_id in self.config.databases.items()
        }
        out: dict[str, str] = {}
        for prop_name, prop_schema in properties.items():
            if prop_schema.get("type") != "relation":
                continue
            relation_cfg = prop_schema.get("relation", {})
            target_db_id = normalize_notion_id(str(relation_cfg.get("database_id") or ""))
            target_logical = db_id_to_logical.get(target_db_id)
            if target_logical and target_logical not in out:
                out[target_logical] = str(prop_name)
        return out

    def _relation_context_for_page(self, page_id: str) -> dict[str, list[dict[str, str]]]:
        out: dict[str, list[dict[str, str]]] = {
            "resources": [],
            "concepts": [],
            "skills": [],
            "mindsets": [],
            "errors": [],
        }
        relations = self.store.get_relations_by_from_page(page_id)
        for rel in relations:
            target = self.store.get_page_by_id(str(rel["to_page_id"]))
            if not target:
                continue
            logical_db = str(target["logical_db"])
            out.setdefault(logical_db, []).append(
                {
                    "page_id": str(target["page_id"]),
                    "title": str(target["title"] or "").strip(),
                }
            )
        for key, values in list(out.items()):
            values = [item for item in values if item.get("title")]
            values.sort(key=lambda item: item.get("title", ""))
            out[key] = values
        return out

    def _resource_note_for_page(self, page_id: str) -> str:
        suggestion = self.store.get_knowledge_suggestion_by_page("resources", page_id)
        if suggestion:
            markdown = str(suggestion["proposed_markdown"] or "").strip()
            if markdown:
                return markdown
        page = self.store.get_page_by_id(page_id)
        if not page:
            return ""
        for key in ("plain_text", "text_blob", "property_text"):
            text = str(page[key] or "").strip()
            if text:
                return text
        return ""

    def _related_resources_for_target(
        self,
        *,
        target: KnowledgeTarget,
        mapping_rows: list[dict[str, Any]],
        related_lessons: list[str],
        source_attachments: dict[str, str],
    ) -> list[dict[str, Any]]:
        resources_by_id: dict[str, dict[str, Any]] = {}
        resource_rows = [row for row in self.store.get_pages("resources") if int(row["archived"]) == 0]

        def add_resource_page(page: dict[str, Any]) -> None:
            page_id = str(page.get("page_id") or "").strip()
            if not page_id:
                return
            if page_id in resources_by_id:
                return
            title = str(page.get("title") or "").strip()
            lesson_code = _extract_lesson_code(title)
            mapping = self._find_mapping_for_resource(
                title=title,
                lesson_code=lesson_code,
                mapping_rows=mapping_rows,
            )
            if not lesson_code and mapping:
                lesson_code = _normalize_lesson_code(str(mapping.get("课次") or ""))
            doc_path = str(mapping.get("文档路径") if mapping else "").strip()
            resources_by_id[page_id] = {
                "page_id": page_id,
                "resource_title": title,
                "lesson_code": lesson_code,
                "doc_path": doc_path,
                "doc_attachment": source_attachments.get(lesson_code, ""),
                "resource_note": self._resource_note_for_page(page_id),
            }

        # 1) 优先使用库间真实关联（resources -> 当前目标）
        backlinks = self.store.get_relations_by_to_page(target.page_id, from_logical_db="resources")
        for rel in backlinks:
            page = self.store.get_page_by_id(str(rel["from_page_id"]))
            if page:
                add_resource_page(dict(page))

        # 2) 使用当前记录正向关联（当前目标 -> resources）
        relation_props = self._resolve_relation_properties(target.logical_db)
        resource_prop = relation_props.get("resources")
        if resource_prop:
            for rel in self.store.get_relations_by_from_page(target.page_id):
                if str(rel["property_name"]) != resource_prop:
                    continue
                page = self.store.get_page_by_id(str(rel["to_page_id"]))
                if page and str(page["logical_db"]) == "resources":
                    add_resource_page(dict(page))

        # 3) 回退到 CSV 课次映射
        for lesson in related_lessons:
            mapping = _find_mapping_by_lesson(mapping_rows, lesson)
            if not mapping:
                continue
            mapped_title = str(mapping.get("资料库名称") or "").strip()
            found = None
            if mapped_title:
                found = self._find_page_by_title("resources", mapped_title)
            if not found:
                for row in resource_rows:
                    row_title = str(row["title"] or "")
                    if _extract_lesson_code(row_title) == lesson:
                        found = dict(row)
                        break
            if found:
                add_resource_page(found)

        out = list(resources_by_id.values())
        out.sort(key=lambda item: (item.get("lesson_code") or "ZZZ", item.get("resource_title") or ""))
        return out

    def _load_templates(self) -> dict[str, str]:
        names = sorted(set(TEMPLATE_NAME_BY_DB.values()))
        out: dict[str, str] = {}
        for name in names:
            path = self.template_dir / name
            if not path.exists():
                raise RuntimeError(f"模板文件不存在: {path}")
            out[name] = path.read_text(encoding="utf-8").strip()
        return out

    def _load_prompt_template(self) -> str:
        path = self.template_dir / KNOWLEDGE_PROMPT_TEMPLATE_NAME
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8").strip()

    def _load_mapping_rows(self) -> list[dict[str, Any]]:
        if not self.mapping_csv_path.exists():
            raise RuntimeError(f"关联映射 CSV 不存在: {self.mapping_csv_path}")
        with self.mapping_csv_path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            rows = [dict(row) for row in reader]
        if not rows:
            raise RuntimeError("关联映射 CSV 为空")
        return rows

    def _load_source_attachments(self, mapping_rows: list[dict[str, Any]]) -> dict[str, str]:
        out: dict[str, str] = {}
        for row in mapping_rows:
            lesson = _normalize_lesson_code(str(row.get("课次") or ""))
            if not lesson:
                continue
            doc_path = str(row.get("文档路径") or "").strip()
            if not doc_path:
                continue
            path = _resolve_source_path(doc_path, base_dir=self.repo_root)
            if not path.exists():
                continue
            if lesson in out:
                continue
            out[lesson] = str(path)
        return out

    def _agent_client(self) -> KnowledgeContentAgent:
        if self._agent is None:
            self._agent = KnowledgeContentAgent(
                api_key=self.config.google_api_key(),
                model=self.config.gemini_model,
                temperature=self.config.temperature,
            )
        return self._agent

    def _gateway_client(self) -> NotionGateway:
        if self._gateway is None:
            self._gateway = NotionGateway(self.config.notion_token())
        return self._gateway


def _normalize_lesson_code(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.upper().startswith("L") and len(raw) >= 3:
        return raw.upper()[:3]
    if raw.isdigit():
        return f"L{int(raw):02d}"
    match = LESSON_PATTERN.search(raw)
    if match:
        return f"L{int(match.group(1)):02d}"
    return ""


def _extract_lesson_code(title: str) -> str:
    match = LESSON_PATTERN.search(title or "")
    if not match:
        return ""
    return f"L{int(match.group(1)):02d}"


def _split_csv_list(value: str) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    parts = re.split(r"[,，、;；]", raw)
    return [item.strip() for item in parts if item and item.strip()]


def _normalize_text(value: str) -> str:
    text = (value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    return text


def _resolve_source_path(raw_path: str, *, base_dir: Path) -> Path:
    path = Path(str(raw_path or "").strip()).expanduser()
    if path.is_absolute():
        return path
    candidate = (base_dir / path).resolve()
    if candidate.exists():
        return candidate
    return path.resolve()


def _find_mapping_by_lesson(rows: list[dict[str, Any]], lesson_code: str) -> dict[str, Any] | None:
    for row in rows:
        if _normalize_lesson_code(str(row.get("课次") or "")) == lesson_code:
            return row
    return None


def _unique_list(values: list[str]) -> list[str]:
    out: list[str] = []
    for item in values:
        text = str(item).strip()
        if not text or text in out:
            continue
        out.append(text)
    return out
