from __future__ import annotations

import json
from typing import Any

from .base import cn_now_iso


class KnowledgeWorkflowStoreMixin:
    def create_knowledge_run(self) -> int:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(
                """
                INSERT INTO knowledge_runs(status, started_at)
                VALUES(?, ?)
                RETURNING run_id
                """,
                ("running", now),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to create knowledge run")
            return int(row["run_id"])

    def finish_knowledge_run(
        self,
        run_id: int,
        *,
        status: str,
        target_count: int,
        suggestion_count: int,
        needs_review_count: int,
        failure_count: int,
        summary: str,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                UPDATE knowledge_runs
                SET status = ?, finished_at = ?, target_count = ?, suggestion_count = ?,
                    needs_review_count = ?, failure_count = ?, summary = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    now,
                    target_count,
                    suggestion_count,
                    needs_review_count,
                    failure_count,
                    summary,
                    run_id,
                ),
            )

    def add_knowledge_event(
        self,
        run_id: int,
        step: str,
        status: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                INSERT INTO knowledge_events(run_id, step, status, message, detail_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    step,
                    status,
                    message,
                    json.dumps(detail or {}, ensure_ascii=False),
                    now,
                ),
            )

    def count_knowledge_runs(self) -> int:
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute("SELECT COUNT(*) AS cnt FROM knowledge_runs").fetchone()
        return int(row["cnt"]) if row else 0

    def list_knowledge_runs(self, limit: int = 30, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM knowledge_runs
                ORDER BY run_id DESC
                LIMIT ?
                OFFSET ?
                """,
                (limit, max(0, offset)),
            ).fetchall()

    def get_knowledge_run(self, run_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM knowledge_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()

    def list_knowledge_events(self, run_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM knowledge_events
                WHERE run_id = ?
                ORDER BY event_id ASC
                """,
                (run_id,),
            ).fetchall()

    def upsert_knowledge_suggestion(
        self,
        *,
        run_id: int,
        logical_db: str,
        page_id: str,
        page_title: str,
        lesson_code: str,
        source_doc_path: str,
        source_refs: list[str],
        status: str,
        confidence: float | None,
        proposed_markdown: str,
        reasoning_summary: str,
        validation_notes: str,
        source_snapshot: dict[str, Any],
        model_response: dict[str, Any],
        reviewer_note: str = "",
        failure_reason: str = "",
    ) -> int:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(
                """
                INSERT INTO knowledge_suggestions(
                  run_id, logical_db, page_id, page_title, lesson_code, source_doc_path, source_refs_json,
                  status, confidence, proposed_markdown, reasoning_summary, validation_notes,
                  source_snapshot_json, model_response_json, reviewer_note, failure_reason,
                  created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(logical_db, page_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  page_title = excluded.page_title,
                  lesson_code = excluded.lesson_code,
                  source_doc_path = excluded.source_doc_path,
                  source_refs_json = excluded.source_refs_json,
                  status = excluded.status,
                  confidence = excluded.confidence,
                  proposed_markdown = excluded.proposed_markdown,
                  reasoning_summary = excluded.reasoning_summary,
                  validation_notes = excluded.validation_notes,
                  source_snapshot_json = excluded.source_snapshot_json,
                  model_response_json = excluded.model_response_json,
                  reviewer_note = excluded.reviewer_note,
                  failure_reason = excluded.failure_reason,
                  updated_at = excluded.updated_at
                RETURNING suggestion_id
                """,
                (
                    run_id,
                    logical_db,
                    page_id,
                    page_title,
                    lesson_code,
                    source_doc_path,
                    json.dumps(source_refs, ensure_ascii=False),
                    status,
                    confidence,
                    proposed_markdown,
                    reasoning_summary,
                    validation_notes,
                    json.dumps(source_snapshot, ensure_ascii=False),
                    json.dumps(model_response, ensure_ascii=False),
                    reviewer_note,
                    failure_reason,
                    now,
                    now,
                ),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to read upserted knowledge suggestion")
            return int(row["suggestion_id"])

    def list_knowledge_suggestions(
        self,
        *,
        status: str | None = None,
        logical_db: str | None = None,
        run_id: int | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if logical_db:
            where.append("logical_db = ?")
            params.append(logical_db)
        if run_id is not None:
            where.append("run_id = ?")
            params.append(run_id)

        sql = "SELECT * FROM knowledge_suggestions"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, max(0, offset)])

        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(sql, params).fetchall()

    def count_knowledge_suggestions(
        self,
        *,
        status: str | None = None,
        logical_db: str | None = None,
    ) -> int:
        where: list[str] = []
        params: list[Any] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if logical_db:
            where.append("logical_db = ?")
            params.append(logical_db)
        sql = "SELECT COUNT(*) AS cnt FROM knowledge_suggestions"
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(sql, params).fetchone()
        return int(row["cnt"]) if row else 0

    def get_knowledge_suggestion(self, suggestion_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM knowledge_suggestions WHERE suggestion_id = ?",
                (suggestion_id,),
            ).fetchone()

    def get_knowledge_suggestion_by_page(self, logical_db: str, page_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM knowledge_suggestions
                WHERE logical_db = ? AND page_id = ?
                """,
                (logical_db, page_id),
            ).fetchone()

    def knowledge_suggestion_counts(self) -> dict[str, int]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM knowledge_suggestions
                GROUP BY status
                """
            ).fetchall()
        return {row["status"]: row["cnt"] for row in rows}

    def update_knowledge_suggestion_fields(
        self,
        suggestion_id: int,
        *,
        proposed_markdown: str,
        reviewer_note: str,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                UPDATE knowledge_suggestions
                SET proposed_markdown = ?, reviewer_note = ?, updated_at = ?
                WHERE suggestion_id = ?
                """,
                (
                    proposed_markdown,
                    reviewer_note.strip(),
                    now,
                    suggestion_id,
                ),
            )

    def update_knowledge_suggestion_status(
        self,
        suggestion_id: int,
        *,
        status: str,
        reviewer_note: str = "",
        failure_reason: str = "",
        set_reviewed_at: bool = False,
        set_applied_at: bool = False,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            reviewed_at = now if set_reviewed_at else None
            applied_at = now if set_applied_at else None
            conn.execute(
                """
                UPDATE knowledge_suggestions
                SET status = ?, reviewer_note = ?, failure_reason = ?, updated_at = ?,
                    reviewed_at = COALESCE(?, reviewed_at),
                    applied_at = COALESCE(?, applied_at)
                WHERE suggestion_id = ?
                """,
                (
                    status,
                    reviewer_note.strip(),
                    failure_reason.strip(),
                    now,
                    reviewed_at,
                    applied_at,
                    suggestion_id,
                ),
            )
