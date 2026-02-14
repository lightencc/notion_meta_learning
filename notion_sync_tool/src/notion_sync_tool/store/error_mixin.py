from __future__ import annotations

import json
from typing import Any

from .base import cn_now_iso


class ErrorWorkflowStoreMixin:
    def create_workflow_run(self) -> int:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(
                """
                INSERT INTO workflow_runs(status, started_at)
                VALUES(?, ?)
                RETURNING run_id
                """,
                ("running", now),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to create workflow run")
            return int(row["run_id"])

    def finish_workflow_run(
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
                UPDATE workflow_runs
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

    def add_workflow_event(
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
                INSERT INTO workflow_events(run_id, step, status, message, detail_json, created_at)
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

    def count_workflow_runs(self) -> int:
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute("SELECT COUNT(*) AS cnt FROM workflow_runs").fetchone()
        return int(row["cnt"]) if row else 0

    def list_workflow_runs(self, limit: int = 30, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM workflow_runs
                ORDER BY run_id DESC
                LIMIT ?
                OFFSET ?
                """,
                (limit, max(0, offset)),
            ).fetchall()

    def get_workflow_run(self, run_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM workflow_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()

    def list_workflow_events(self, run_id: int) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM workflow_events
                WHERE run_id = ?
                ORDER BY event_id ASC
                """,
                (run_id,),
            ).fetchall()

    def upsert_agent_suggestion(
        self,
        *,
        run_id: int,
        error_page_id: str,
        error_title: str,
        status: str,
        confidence: float | None,
        proposed_title: str | None,
        proposed_resource_id: str | None,
        proposed_concept_id: str | None,
        proposed_skill_id: str | None,
        proposed_mindset_id: str | None,
        proposed_similar_ids: list[str],
        reasoning_summary: str,
        validation_notes: str,
        source_snapshot: dict[str, Any],
        candidates: dict[str, Any],
        model_response: dict[str, Any],
        reviewer_note: str = "",
        failure_reason: str = "",
    ) -> int:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(
                """
                INSERT INTO agent_suggestions(
                  run_id, error_page_id, error_title, status, confidence, proposed_title,
                  proposed_resource_id, proposed_concept_id, proposed_skill_id, proposed_mindset_id,
                  proposed_similar_ids_json, reasoning_summary, validation_notes, source_snapshot_json,
                  candidates_json, model_response_json, reviewer_note, failure_reason, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(error_page_id) DO UPDATE SET
                  run_id = excluded.run_id,
                  error_title = excluded.error_title,
                  status = excluded.status,
                  confidence = excluded.confidence,
                  proposed_title = excluded.proposed_title,
                  proposed_resource_id = excluded.proposed_resource_id,
                  proposed_concept_id = excluded.proposed_concept_id,
                  proposed_skill_id = excluded.proposed_skill_id,
                  proposed_mindset_id = excluded.proposed_mindset_id,
                  proposed_similar_ids_json = excluded.proposed_similar_ids_json,
                  reasoning_summary = excluded.reasoning_summary,
                  validation_notes = excluded.validation_notes,
                  source_snapshot_json = excluded.source_snapshot_json,
                  candidates_json = excluded.candidates_json,
                  model_response_json = excluded.model_response_json,
                  reviewer_note = excluded.reviewer_note,
                  failure_reason = excluded.failure_reason,
                  updated_at = excluded.updated_at
                RETURNING suggestion_id
                """,
                (
                    run_id,
                    error_page_id,
                    error_title,
                    status,
                    confidence,
                    proposed_title,
                    proposed_resource_id,
                    proposed_concept_id,
                    proposed_skill_id,
                    proposed_mindset_id,
                    json.dumps(proposed_similar_ids, ensure_ascii=False),
                    reasoning_summary,
                    validation_notes,
                    json.dumps(source_snapshot, ensure_ascii=False),
                    json.dumps(candidates, ensure_ascii=False),
                    json.dumps(model_response, ensure_ascii=False),
                    reviewer_note,
                    failure_reason,
                    now,
                    now,
                ),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to read upserted suggestion")
            return int(row["suggestion_id"])

    def list_agent_suggestions(
        self,
        *,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            if status:
                return conn.execute(
                    """
                    SELECT * FROM agent_suggestions
                    WHERE status = ?
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (status, limit),
                ).fetchall()
            return conn.execute(
                """
                SELECT * FROM agent_suggestions
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def get_agent_suggestion(self, suggestion_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM agent_suggestions WHERE suggestion_id = ?",
                (suggestion_id,),
            ).fetchone()

    def suggestion_counts(self) -> dict[str, int]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS cnt
                FROM agent_suggestions
                GROUP BY status
                """
            ).fetchall()
        return {row["status"]: row["cnt"] for row in rows}

    def update_suggestion_fields(
        self,
        suggestion_id: int,
        *,
        proposed_title: str | None,
        proposed_resource_id: str | None,
        proposed_concept_id: str | None,
        proposed_skill_id: str | None,
        proposed_mindset_id: str | None,
        proposed_similar_ids: list[str],
        reviewer_note: str,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                UPDATE agent_suggestions
                SET proposed_title = ?, proposed_resource_id = ?, proposed_concept_id = ?,
                    proposed_skill_id = ?, proposed_mindset_id = ?, proposed_similar_ids_json = ?,
                    reviewer_note = ?, updated_at = ?
                WHERE suggestion_id = ?
                """,
                (
                    proposed_title,
                    proposed_resource_id,
                    proposed_concept_id,
                    proposed_skill_id,
                    proposed_mindset_id,
                    json.dumps(proposed_similar_ids, ensure_ascii=False),
                    reviewer_note.strip(),
                    now,
                    suggestion_id,
                ),
            )

    def update_suggestion_status(
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
                UPDATE agent_suggestions
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
