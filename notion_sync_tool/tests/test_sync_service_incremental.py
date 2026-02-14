from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from notion_sync_tool.sync_service import SyncOptions, SyncService


class _FakeGateway:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_database(self, database_id: str) -> dict[str, Any]:
        return {
            "id": database_id,
            "properties": {
                "名称": {"id": "title", "type": "title"},
            },
        }

    def get_database_properties(
        self, database_id: str, database_obj: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], str | None]:
        _ = database_id, database_obj
        return {"名称": {"id": "title", "type": "title"}}, None

    def query_database_all(
        self, database_id: str, page_size: int = 100, edited_after: str | None = None
    ) -> list[dict[str, Any]]:
        self.calls.append(
            {
                "database_id": database_id,
                "page_size": page_size,
                "edited_after": edited_after,
            }
        )
        return [
            {
                "id": "p1",
                "url": "https://example.com/p1",
                "created_time": "2026-01-01T00:00:00.000Z",
                "last_edited_time": "2026-01-02T00:00:00.000Z",
                "archived": False,
                "properties": {
                    "名称": {"type": "title", "title": [{"plain_text": "标题1"}]},
                },
            }
        ]

    def get_page_plain_text(self, page_id: str, max_chars: int = 1500, max_depth: int = 2) -> str:
        _ = page_id, max_chars, max_depth
        return "plain"


class _FakeStore:
    def __init__(self, latest: str | None) -> None:
        self.latest = latest
        self.upserts: list[dict[str, Any]] = []
        self.sync_events: list[dict[str, Any]] = []
        self.sync_finished: dict[str, Any] = {}

    def latest_page_edited_time(self, logical_db: str) -> str | None:
        _ = logical_db
        return self.latest

    def create_sync_run(
        self,
        *,
        incremental: bool,
        include_page_content: bool,
        page_size: int,
    ) -> int:
        _ = incremental, include_page_content, page_size
        return 1

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
        self.sync_events.append(
            {
                "run_id": run_id,
                "step": step,
                "status": status,
                "message": message,
                "logical_db": logical_db,
                "detail": detail or {},
            }
        )

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
        self.sync_finished = {
            "run_id": run_id,
            "status": status,
            "database_count": database_count,
            "page_count": page_count,
            "relation_count": relation_count,
            "changed_count": changed_count,
            "summary": summary,
        }

    def get_page_edit_times(self, logical_db: str, page_ids: list[str]) -> dict[str, str]:
        _ = logical_db, page_ids
        return {}

    def upsert_database_snapshot(self, **kwargs: Any) -> None:  # noqa: ANN401
        self.upserts.append(kwargs)


def _make_service(latest: str | None) -> tuple[SyncService, _FakeGateway, _FakeStore]:
    cfg = SimpleNamespace(databases={"resources": "db_1"})
    gateway = _FakeGateway()
    store = _FakeStore(latest=latest)
    return SyncService(config=cfg, gateway=gateway, store=store), gateway, store


def test_sync_service_uses_incremental_when_latest_exists() -> None:
    svc, gateway, store = _make_service("2026-01-01T00:00:00.000Z")
    svc.run(SyncOptions(incremental=True))

    assert gateway.calls[0]["edited_after"] == "2026-01-01T00:00:00.000Z"
    assert store.upserts[0]["full_replace"] is False


def test_sync_service_falls_back_to_full_replace_when_no_snapshot() -> None:
    svc, gateway, store = _make_service(None)
    svc.run(SyncOptions(incremental=True))

    assert gateway.calls[0]["edited_after"] is None
    assert store.upserts[0]["full_replace"] is True
