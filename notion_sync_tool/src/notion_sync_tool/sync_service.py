from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .config import AppConfig
from .logging_utils import get_logger
from .notion_gateway import NotionGateway
from .notion_helpers import (
    extract_property_text,
    extract_relation_rows,
    extract_title_from_page,
    first_title_property_name,
)
from .postgres_store import (
    PostgresStore,
    StoredPage,
)


@dataclass(slots=True)
class SyncOptions:
    include_page_content: bool = True
    content_max_chars: int = 1600
    content_max_depth: int = 2
    page_size: int = 100
    incremental: bool = True


@dataclass(slots=True)
class SyncStats:
    databases: dict[str, int]
    relations: dict[str, int]
    changed: dict[str, int]
    run_id: int


class SyncService:
    def __init__(self, config: AppConfig, gateway: NotionGateway, store: PostgresStore) -> None:
        self.config = config
        self.gateway = gateway
        self.store = store
        self.logger = get_logger(self.__class__.__name__)

    def run(self, options: SyncOptions | None = None) -> SyncStats:
        opts = options or SyncOptions()
        run_id = self.store.create_sync_run(
            incremental=opts.incremental,
            include_page_content=opts.include_page_content,
            page_size=opts.page_size,
        )
        self.store.add_sync_event(
            run_id,
            step="start",
            status="running",
            message="同步任务已启动",
            detail={
                "include_page_content": bool(opts.include_page_content),
                "content_max_chars": int(opts.content_max_chars),
                "content_max_depth": int(opts.content_max_depth),
                "page_size": int(opts.page_size),
                "incremental": bool(opts.incremental),
            },
        )
        self.logger.info(
            "Sync started: run_id=%s include_page_content=%s content_max_chars=%s content_max_depth=%s page_size=%s incremental=%s",
            run_id,
            opts.include_page_content,
            opts.content_max_chars,
            opts.content_max_depth,
            opts.page_size,
            opts.incremental,
        )
        db_page_counts: dict[str, int] = {}
        db_rel_counts: dict[str, int] = {}
        db_changed_counts: dict[str, int] = {}
        total_changed = 0

        try:
            for logical_db, database_id in self.config.databases.items():
                self.logger.info("Pulling schema: logical_db=%s database_id=%s", logical_db, database_id)
                schema = self.gateway.get_database(database_id)
                schema_props, data_source_id = self.gateway.get_database_properties(
                    database_id, database_obj=schema
                )
                title_property = first_title_property_name(schema_props)

                merged_schema = dict(schema)
                merged_schema["properties"] = schema_props
                if data_source_id:
                    merged_schema["_schema_source"] = "data_source"
                    merged_schema["_data_source_id"] = data_source_id
                else:
                    merged_schema["_schema_source"] = "database"

                last_edited_after = (
                    self.store.latest_page_edited_time(logical_db) if opts.incremental else None
                )
                is_full_replace = not bool(last_edited_after)
                if last_edited_after:
                    self.logger.info(
                        "Pulling pages incrementally: logical_db=%s on_or_after=%s",
                        logical_db,
                        last_edited_after,
                    )
                else:
                    self.logger.info("Pulling pages full snapshot: logical_db=%s", logical_db)

                pages = self.gateway.query_database_all(
                    database_id=database_id,
                    page_size=opts.page_size,
                    edited_after=last_edited_after,
                )

                stored_pages: list[StoredPage] = []
                relation_rows: list[tuple[str, str, str]] = []
                old_edit_times = self.store.get_page_edit_times(
                    logical_db, [str(item.get("id") or "").strip() for item in pages]
                )

                for idx, page in enumerate(pages, start=1):
                    title = extract_title_from_page(page, title_property)
                    properties = page.get("properties", {})
                    property_text = extract_property_text(properties)

                    plain_text = ""
                    if opts.include_page_content:
                        plain_text = self.gateway.get_page_plain_text(
                            page_id=page["id"],
                            max_chars=opts.content_max_chars,
                            max_depth=opts.content_max_depth,
                        )

                    text_blob_parts = [title, property_text, plain_text]
                    text_blob = "\n".join(part for part in text_blob_parts if part).strip()

                    stored_pages.append(
                        StoredPage(
                            page_id=page["id"],
                            logical_db=logical_db,
                            database_id=database_id,
                            title=title,
                            property_text=property_text,
                            plain_text=plain_text,
                            text_blob=text_blob,
                            properties_json=json.dumps(properties, ensure_ascii=False),
                            page_json=json.dumps(page, ensure_ascii=False),
                            url=(page.get("url") or "").strip(),
                            created_time=page.get("created_time"),
                            last_edited_time=page.get("last_edited_time"),
                            archived=1 if page.get("archived") else 0,
                        )
                    )
                    relation_rows.extend(extract_relation_rows(page))

                    if idx % 50 == 0:
                        self.logger.info(
                            "Sync progress: logical_db=%s processed=%s total=%s",
                            logical_db,
                            idx,
                            len(pages),
                        )

                changed_pages: list[dict[str, Any]] = []
                for page in stored_pages:
                    prev_edited = old_edit_times.get(page.page_id, "")
                    current_edited = str(page.last_edited_time or "").strip()
                    if not prev_edited:
                        change_type = "inserted"
                    elif prev_edited != current_edited:
                        change_type = "updated"
                    else:
                        change_type = "touched"
                    if is_full_replace or change_type != "touched":
                        changed_pages.append(
                            {
                                "page_id": page.page_id,
                                "title": page.title,
                                "change_type": change_type,
                                "last_edited_time": current_edited,
                            }
                        )

                self.store.upsert_database_snapshot(
                    logical_db=logical_db,
                    database_id=database_id,
                    title_property=title_property,
                    schema_json=merged_schema,
                    pages=stored_pages,
                    relations=relation_rows,
                    full_replace=is_full_replace,
                )
                db_page_counts[logical_db] = len(stored_pages)
                db_rel_counts[logical_db] = len(relation_rows)
                db_changed_counts[logical_db] = len(changed_pages)
                total_changed += len(changed_pages)
                self.logger.info(
                    "Sync completed for database: logical_db=%s pages=%s relations=%s changed=%s",
                    logical_db,
                    len(stored_pages),
                    len(relation_rows),
                    len(changed_pages),
                )
                truncated_changes = changed_pages[:200]
                self.store.add_sync_event(
                    run_id,
                    logical_db=logical_db,
                    step="database_synced",
                    status="completed",
                    message=(
                        f"{logical_db} 同步完成：页面 {len(stored_pages)}，"
                        f"关联 {len(relation_rows)}，变更 {len(changed_pages)}"
                    ),
                    detail={
                        "mode": "full" if is_full_replace else "incremental",
                        "last_edited_after": last_edited_after or "",
                        "page_count": len(stored_pages),
                        "relation_count": len(relation_rows),
                        "changed_count": len(changed_pages),
                        "changed_pages": truncated_changes,
                        "omitted_changed_pages": max(0, len(changed_pages) - len(truncated_changes)),
                    },
                )

            summary = (
                f"同步完成：库 {len(db_page_counts)}，页面 {sum(db_page_counts.values())}，"
                f"关联 {sum(db_rel_counts.values())}，变更 {total_changed}"
            )
            self.store.add_sync_event(
                run_id,
                step="finish",
                status="completed",
                message=summary,
                detail={
                    "databases": db_page_counts,
                    "relations": db_rel_counts,
                    "changed": db_changed_counts,
                },
            )
            self.store.finish_sync_run(
                run_id,
                status="completed",
                database_count=len(db_page_counts),
                page_count=sum(db_page_counts.values()),
                relation_count=sum(db_rel_counts.values()),
                changed_count=total_changed,
                summary=summary,
            )
            self.logger.info(
                "Sync finished: run_id=%s databases=%s relations=%s changed=%s",
                run_id,
                db_page_counts,
                db_rel_counts,
                db_changed_counts,
            )
            return SyncStats(
                databases=db_page_counts,
                relations=db_rel_counts,
                changed=db_changed_counts,
                run_id=run_id,
            )
        except Exception as exc:  # noqa: BLE001
            self.store.add_sync_event(
                run_id,
                step="fatal",
                status="failed",
                message=str(exc),
            )
            self.store.finish_sync_run(
                run_id,
                status="failed",
                database_count=len(db_page_counts),
                page_count=sum(db_page_counts.values()),
                relation_count=sum(db_rel_counts.values()),
                changed_count=total_changed,
                summary=str(exc),
            )
            self.logger.exception("Sync failed: run_id=%s", run_id)
            raise
