from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:  # noqa: BLE001
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]
    _PSYCOPG_IMPORT_ERROR = exc
else:
    _PSYCOPG_IMPORT_ERROR = None

CN_TZ = timezone(timedelta(hours=8))


def cn_now_iso() -> str:
    return datetime.now(CN_TZ).isoformat()


@dataclass(slots=True)
class StoredPage:
    page_id: str
    logical_db: str
    database_id: str
    title: str
    property_text: str
    plain_text: str
    text_blob: str
    properties_json: str
    page_json: str
    url: str
    created_time: str | None
    last_edited_time: str | None
    archived: int


def _to_pg_query(query: str) -> str:
    return query.replace("?", "%s")


class _CompatPgConnection:
    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self._conn = conn

    def __enter__(self) -> _CompatPgConnection:
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool | None:  # type: ignore[no-untyped-def]
        return self._conn.__exit__(exc_type, exc, tb)

    def execute(self, query: str, params: Iterable[Any] | None = None):  # type: ignore[no-untyped-def]
        return self._conn.execute(_to_pg_query(query), tuple(params or ()))

    def executemany(self, query: str, params_seq: Iterable[Iterable[Any]]):  # type: ignore[no-untyped-def]
        with self._conn.cursor() as cur:
            cur.executemany(_to_pg_query(query), params_seq)


class PostgresStoreBase:
    def __init__(self, dsn: str, schema: str = "notion_sync") -> None:
        if psycopg is None or dict_row is None:
            raise RuntimeError(
                "Missing psycopg dependency. Install with: pip install 'psycopg[binary]>=3.2.0' "
                f"(original error: {_PSYCOPG_IMPORT_ERROR})"
            )
        self.dsn = (dsn or "").strip()
        if not self.dsn:
            raise RuntimeError("PostgreSQL DSN is required")
        self.schema = self._validate_schema(schema)
        self._ensure_schema()

    @staticmethod
    def _validate_schema(schema: str) -> str:
        value = (schema or "").strip() or "notion_sync"
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise RuntimeError(f"Invalid PostgreSQL schema name: {value}")
        return value

    def _connect(self) -> _CompatPgConnection:
        last_exc: Exception | None = None
        for attempt in range(20):
            try:
                conn = psycopg.connect(self.dsn, autocommit=False, row_factory=dict_row)  # type: ignore[arg-type]
                conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
                conn.execute(f'SET search_path TO "{self.schema}"')
                return _CompatPgConnection(conn)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                msg = str(exc).lower()
                transient = (
                    "could not connect" in msg
                    or "connection refused" in msg
                    or "the database system is starting up" in msg
                    or "timeout" in msg
                )
                if not transient or attempt == 19:
                    raise
                time.sleep(0.15)
        if last_exc:
            raise last_exc
        raise RuntimeError("failed to open postgres database")

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            ddl_statements = [
                """
                CREATE TABLE IF NOT EXISTS databases (
                  logical_db TEXT PRIMARY KEY,
                  database_id TEXT NOT NULL,
                  title_property TEXT NOT NULL,
                  schema_json TEXT NOT NULL,
                  synced_at TEXT NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS pages (
                  page_id TEXT PRIMARY KEY,
                  logical_db TEXT NOT NULL,
                  database_id TEXT NOT NULL,
                  title TEXT NOT NULL DEFAULT '',
                  property_text TEXT NOT NULL DEFAULT '',
                  plain_text TEXT NOT NULL DEFAULT '',
                  text_blob TEXT NOT NULL DEFAULT '',
                  properties_json TEXT NOT NULL,
                  page_json TEXT NOT NULL,
                  url TEXT NOT NULL DEFAULT '',
                  created_time TEXT,
                  last_edited_time TEXT,
                  archived INTEGER NOT NULL DEFAULT 0,
                  synced_at TEXT NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_pages_db ON pages(logical_db)",
                "CREATE INDEX IF NOT EXISTS idx_pages_title ON pages(title)",
                """
                CREATE TABLE IF NOT EXISTS relations (
                  from_page_id TEXT NOT NULL,
                  from_logical_db TEXT NOT NULL,
                  property_name TEXT NOT NULL,
                  to_page_id TEXT NOT NULL,
                  synced_at TEXT NOT NULL,
                  PRIMARY KEY (from_page_id, property_name, to_page_id)
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_rel_from ON relations(from_page_id)",
                "CREATE INDEX IF NOT EXISTS idx_rel_to ON relations(to_page_id)",
                """
                CREATE TABLE IF NOT EXISTS workflow_runs (
                  run_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  status TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  finished_at TEXT,
                  target_count BIGINT NOT NULL DEFAULT 0,
                  suggestion_count BIGINT NOT NULL DEFAULT 0,
                  needs_review_count BIGINT NOT NULL DEFAULT 0,
                  failure_count BIGINT NOT NULL DEFAULT 0,
                  summary TEXT NOT NULL DEFAULT ''
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS workflow_events (
                  event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  run_id BIGINT NOT NULL,
                  step TEXT NOT NULL,
                  status TEXT NOT NULL,
                  message TEXT NOT NULL,
                  detail_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_events_run ON workflow_events(run_id, event_id)",
                """
                CREATE TABLE IF NOT EXISTS agent_suggestions (
                  suggestion_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  run_id BIGINT NOT NULL,
                  error_page_id TEXT NOT NULL UNIQUE,
                  error_title TEXT NOT NULL,
                  status TEXT NOT NULL,
                  confidence REAL,
                  proposed_title TEXT,
                  proposed_resource_id TEXT,
                  proposed_concept_id TEXT,
                  proposed_skill_id TEXT,
                  proposed_mindset_id TEXT,
                  proposed_similar_ids_json TEXT NOT NULL DEFAULT '[]',
                  reasoning_summary TEXT NOT NULL DEFAULT '',
                  validation_notes TEXT NOT NULL DEFAULT '',
                  source_snapshot_json TEXT NOT NULL,
                  candidates_json TEXT NOT NULL,
                  model_response_json TEXT NOT NULL,
                  reviewer_note TEXT NOT NULL DEFAULT '',
                  failure_reason TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  reviewed_at TEXT,
                  applied_at TEXT
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_suggestions_status ON agent_suggestions(status, updated_at)",
                "CREATE INDEX IF NOT EXISTS idx_suggestions_run ON agent_suggestions(run_id)",
                """
                CREATE TABLE IF NOT EXISTS knowledge_runs (
                  run_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  status TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  finished_at TEXT,
                  target_count BIGINT NOT NULL DEFAULT 0,
                  suggestion_count BIGINT NOT NULL DEFAULT 0,
                  needs_review_count BIGINT NOT NULL DEFAULT 0,
                  failure_count BIGINT NOT NULL DEFAULT 0,
                  summary TEXT NOT NULL DEFAULT ''
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS knowledge_events (
                  event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  run_id BIGINT NOT NULL,
                  step TEXT NOT NULL,
                  status TEXT NOT NULL,
                  message TEXT NOT NULL,
                  detail_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_knowledge_events_run ON knowledge_events(run_id, event_id)",
                """
                CREATE TABLE IF NOT EXISTS knowledge_suggestions (
                  suggestion_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  run_id BIGINT NOT NULL,
                  logical_db TEXT NOT NULL,
                  page_id TEXT NOT NULL,
                  page_title TEXT NOT NULL,
                  lesson_code TEXT NOT NULL DEFAULT '',
                  source_doc_path TEXT NOT NULL DEFAULT '',
                  source_refs_json TEXT NOT NULL DEFAULT '[]',
                  status TEXT NOT NULL,
                  confidence REAL,
                  proposed_markdown TEXT NOT NULL,
                  reasoning_summary TEXT NOT NULL DEFAULT '',
                  validation_notes TEXT NOT NULL DEFAULT '',
                  source_snapshot_json TEXT NOT NULL,
                  model_response_json TEXT NOT NULL,
                  reviewer_note TEXT NOT NULL DEFAULT '',
                  failure_reason TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  reviewed_at TEXT,
                  applied_at TEXT,
                  UNIQUE(logical_db, page_id)
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_knowledge_suggestions_status ON knowledge_suggestions(status, updated_at)",
                "CREATE INDEX IF NOT EXISTS idx_knowledge_suggestions_run ON knowledge_suggestions(run_id)",
                """
                CREATE TABLE IF NOT EXISTS sync_runs (
                  run_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  status TEXT NOT NULL,
                  started_at TEXT NOT NULL,
                  finished_at TEXT,
                  incremental INTEGER NOT NULL DEFAULT 1,
                  include_page_content INTEGER NOT NULL DEFAULT 1,
                  page_size INTEGER NOT NULL DEFAULT 100,
                  database_count BIGINT NOT NULL DEFAULT 0,
                  page_count BIGINT NOT NULL DEFAULT 0,
                  relation_count BIGINT NOT NULL DEFAULT 0,
                  changed_count BIGINT NOT NULL DEFAULT 0,
                  summary TEXT NOT NULL DEFAULT ''
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS sync_events (
                  event_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                  run_id BIGINT NOT NULL,
                  logical_db TEXT NOT NULL DEFAULT '',
                  step TEXT NOT NULL,
                  status TEXT NOT NULL,
                  message TEXT NOT NULL,
                  detail_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(run_id DESC)",
                "CREATE INDEX IF NOT EXISTS idx_sync_events_run ON sync_events(run_id, event_id)",
            ]
            for stmt in ddl_statements:
                conn.execute(stmt)
