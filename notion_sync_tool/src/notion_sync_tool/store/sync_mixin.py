from __future__ import annotations

import json
from typing import Any

from .base import StoredPage, cn_now_iso


class SyncStoreMixin:
    def create_sync_run(
        self,
        *,
        incremental: bool,
        include_page_content: bool,
        page_size: int,
    ) -> int:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(
                """
                INSERT INTO sync_runs(status, started_at, incremental, include_page_content, page_size)
                VALUES(?, ?, ?, ?, ?)
                RETURNING run_id
                """,
                ("running", now, 1 if incremental else 0, 1 if include_page_content else 0, page_size),
            ).fetchone()
            if not row:
                raise RuntimeError("Failed to create sync run")
            return int(row["run_id"])

    def finish_sync_run(
        self,
        run_id: int,
        *,
        status: str,
        database_count: int,
        page_count: int,
        relation_count: int,
        changed_count: int,
        summary: str,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                UPDATE sync_runs
                SET status = ?, finished_at = ?, database_count = ?, page_count = ?,
                    relation_count = ?, changed_count = ?, summary = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    now,
                    database_count,
                    page_count,
                    relation_count,
                    changed_count,
                    summary,
                    run_id,
                ),
            )

    def add_sync_event(
        self,
        run_id: int,
        *,
        step: str,
        status: str,
        message: str,
        logical_db: str = "",
        detail: dict[str, Any] | None = None,
    ) -> None:
        now = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                INSERT INTO sync_events(run_id, logical_db, step, status, message, detail_json, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    logical_db.strip(),
                    step,
                    status,
                    message,
                    json.dumps(detail or {}, ensure_ascii=False),
                    now,
                ),
            )

    def list_sync_runs(self, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM sync_runs
                ORDER BY run_id DESC
                LIMIT ?
                OFFSET ?
                """,
                (limit, max(0, offset)),
            ).fetchall()

    def get_sync_run(self, run_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM sync_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()

    def list_sync_events(
        self,
        run_id: int,
        *,
        step: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            if step:
                return conn.execute(
                    """
                    SELECT * FROM sync_events
                    WHERE run_id = ? AND step = ?
                    ORDER BY event_id ASC
                    LIMIT ?
                    """,
                    (run_id, step, limit),
                ).fetchall()
            return conn.execute(
                """
                SELECT * FROM sync_events
                WHERE run_id = ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()

    def get_page_edit_times(
        self,
        logical_db: str,
        page_ids: list[str],
    ) -> dict[str, str]:
        ids = [str(item).strip() for item in page_ids if str(item).strip()]
        if not ids:
            return {}
        placeholders = ", ".join("?" for _ in ids)
        sql = (
            "SELECT page_id, last_edited_time FROM pages "
            f"WHERE logical_db = ? AND page_id IN ({placeholders})"
        )
        with self._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(sql, [logical_db, *ids]).fetchall()
        out: dict[str, str] = {}
        for row in rows:
            key = str(row.get("page_id") or "").strip()
            value = str(row.get("last_edited_time") or "").strip()
            if key:
                out[key] = value
        return out

    def replace_database_snapshot(
        self,
        logical_db: str,
        database_id: str,
        title_property: str,
        schema_json: dict[str, Any],
        pages: list[StoredPage],
        relations: list[tuple[str, str, str]],
    ) -> None:
        self.upsert_database_snapshot(
            logical_db=logical_db,
            database_id=database_id,
            title_property=title_property,
            schema_json=schema_json,
            pages=pages,
            relations=relations,
            full_replace=True,
        )

    def upsert_database_snapshot(
        self,
        logical_db: str,
        database_id: str,
        title_property: str,
        schema_json: dict[str, Any],
        pages: list[StoredPage],
        relations: list[tuple[str, str, str]],
        *,
        full_replace: bool,
    ) -> None:
        synced_at = cn_now_iso()
        with self._connect() as conn:  # type: ignore[attr-defined]
            conn.execute(
                """
                INSERT INTO databases(logical_db, database_id, title_property, schema_json, synced_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(logical_db) DO UPDATE SET
                  database_id=excluded.database_id,
                  title_property=excluded.title_property,
                  schema_json=excluded.schema_json,
                  synced_at=excluded.synced_at
                """,
                (logical_db, database_id, title_property, json.dumps(schema_json, ensure_ascii=False), synced_at),
            )

            if full_replace:
                conn.execute("DELETE FROM relations WHERE from_logical_db = ?", (logical_db,))
                conn.execute("DELETE FROM pages WHERE logical_db = ?", (logical_db,))

            if pages:
                conn.executemany(
                    """
                    INSERT INTO pages(
                      page_id, logical_db, database_id, title, property_text, plain_text, text_blob,
                      properties_json, page_json, url, created_time, last_edited_time, archived, synced_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(page_id) DO UPDATE SET
                      logical_db=excluded.logical_db,
                      database_id=excluded.database_id,
                      title=excluded.title,
                      property_text=excluded.property_text,
                      plain_text=excluded.plain_text,
                      text_blob=excluded.text_blob,
                      properties_json=excluded.properties_json,
                      page_json=excluded.page_json,
                      url=excluded.url,
                      created_time=excluded.created_time,
                      last_edited_time=excluded.last_edited_time,
                      archived=excluded.archived,
                      synced_at=excluded.synced_at
                    """,
                    [
                        (
                            p.page_id,
                            p.logical_db,
                            p.database_id,
                            p.title,
                            p.property_text,
                            p.plain_text,
                            p.text_blob,
                            p.properties_json,
                            p.page_json,
                            p.url,
                            p.created_time,
                            p.last_edited_time,
                            p.archived,
                            synced_at,
                        )
                        for p in pages
                    ],
                )
                if not full_replace:
                    changed_page_ids = [p.page_id for p in pages]
                    conn.executemany(
                        "DELETE FROM relations WHERE from_logical_db = ? AND from_page_id = ?",
                        [(logical_db, page_id) for page_id in changed_page_ids],
                    )

            if relations:
                conn.executemany(
                    """
                    INSERT INTO relations(from_page_id, from_logical_db, property_name, to_page_id, synced_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(from_page_id, property_name, to_page_id) DO UPDATE SET
                      synced_at=excluded.synced_at
                    """,
                    [(from_id, logical_db, prop_name, to_id, synced_at) for from_id, prop_name, to_id in relations],
                )

    def latest_page_edited_time(self, logical_db: str) -> str | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            row = conn.execute(
                "SELECT MAX(last_edited_time) AS latest FROM pages WHERE logical_db = ?",
                (logical_db,),
            ).fetchone()
        if not row:
            return None
        value = row.get("latest")
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def get_database_row(self, logical_db: str) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute("SELECT * FROM databases WHERE logical_db = ?", (logical_db,)).fetchone()

    def list_database_snapshots(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(
                """
                SELECT
                  d.logical_db,
                  d.database_id,
                  d.title_property,
                  d.synced_at,
                  COUNT(p.page_id) AS page_count,
                  MAX(p.last_edited_time) AS latest_page_edited_time
                FROM databases d
                LEFT JOIN pages p ON p.logical_db = d.logical_db
                GROUP BY d.logical_db, d.database_id, d.title_property, d.synced_at
                ORDER BY d.logical_db
                """
            ).fetchall()
        return {str(row["logical_db"]): row for row in rows}

    def get_pages(self, logical_db: str) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM pages WHERE logical_db = ? ORDER BY LOWER(title), page_id",
                (logical_db,),
            ).fetchall()

    def get_page_by_id(self, page_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute("SELECT * FROM pages WHERE page_id = ?", (page_id,)).fetchone()

    def get_relations_by_from_page(self, page_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                "SELECT * FROM relations WHERE from_page_id = ? ORDER BY property_name, to_page_id",
                (page_id,),
            ).fetchall()

    def get_relations_by_to_page(
        self,
        to_page_id: str,
        *,
        from_logical_db: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            if from_logical_db:
                return conn.execute(
                    """
                    SELECT * FROM relations
                    WHERE to_page_id = ? AND from_logical_db = ?
                    ORDER BY from_logical_db, property_name, from_page_id
                    """,
                    (to_page_id, from_logical_db),
                ).fetchall()
            return conn.execute(
                """
                SELECT * FROM relations
                WHERE to_page_id = ?
                ORDER BY from_logical_db, property_name, from_page_id
                """,
                (to_page_id,),
            ).fetchall()

    def get_relations_map(self, logical_db: str) -> dict[str, dict[str, set[str]]]:
        out: dict[str, dict[str, set[str]]] = {}
        with self._connect() as conn:  # type: ignore[attr-defined]
            rows = conn.execute(
                "SELECT from_page_id, property_name, to_page_id FROM relations WHERE from_logical_db = ?",
                (logical_db,),
            ).fetchall()
        for row in rows:
            page_map = out.setdefault(row["from_page_id"], {})
            page_map.setdefault(row["property_name"], set()).add(row["to_page_id"])
        return out

    def search_pages_by_title_substring(
        self, logical_db: str, query: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        q = f"%{query}%"
        with self._connect() as conn:  # type: ignore[attr-defined]
            return conn.execute(
                """
                SELECT * FROM pages
                WHERE logical_db = ? AND title LIKE ?
                ORDER BY LENGTH(title), LOWER(title)
                LIMIT ?
                """,
                (logical_db, q, limit),
            ).fetchall()

    def stats(self) -> dict[str, Any]:
        with self._connect() as conn:  # type: ignore[attr-defined]
            db_counts = conn.execute(
                "SELECT logical_db, COUNT(*) AS cnt FROM pages GROUP BY logical_db ORDER BY logical_db"
            ).fetchall()
            rel_counts = conn.execute(
                "SELECT from_logical_db AS logical_db, COUNT(*) AS cnt FROM relations GROUP BY from_logical_db ORDER BY from_logical_db"
            ).fetchall()
        return {
            "pages": {row["logical_db"]: row["cnt"] for row in db_counts},
            "relations": {row["logical_db"]: row["cnt"] for row in rel_counts},
        }
