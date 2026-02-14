from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from ..config import AppConfig
from ..logging_utils import get_logger
from ..notion_gateway import NotionGateway
from ..postgres_store import PostgresStore

CN_TZ = timezone(timedelta(hours=8))


def safe_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def parse_iso_datetime(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def max_datetime(items: list[datetime | None]) -> datetime | None:
    values = [item for item in items if item is not None]
    if not values:
        return None
    return max(values)


def datetime_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def format_time_cn(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    dt = parse_iso_datetime(text)
    if not dt:
        return text
    return dt.astimezone(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def status_to_zh(value: Any) -> str:
    key = str(value or "").strip()
    mapping = {
        "running": "运行中",
        "completed": "已完成",
        "failed": "失败",
        "pending_review": "待复核",
        "needs_review": "需人工复核",
        "applied": "已应用",
        "rejected": "已驳回",
        "up_to_date": "最新",
        "needs_sync": "需同步",
        "unknown": "未知",
        "all": "全部",
        "resources": "资料库",
        "concepts": "概念库",
        "skills": "技能库",
        "mindsets": "思想库",
    }
    return mapping.get(key, key)


def step_to_zh(value: Any) -> str:
    key = str(value or "").strip()
    mapping = {
        "start": "启动",
        "scan_targets": "扫描目标",
        "load_candidates": "加载候选集",
        "agent_infer": "模型推理",
        "fatal": "致命错误",
    }
    return mapping.get(key, key)


def url_with_message(url: str, *, key: str, message: str) -> str:
    if not message:
        return url
    sep = "&" if "?" in url else "?"
    from urllib.parse import quote

    return f"{url}{sep}{key}={quote(message)}"


def build_sync_status(cfg: AppConfig, store: PostgresStore) -> dict[str, Any]:
    logger = get_logger(__name__)
    snapshots = store.list_database_snapshots()
    rows: list[dict[str, Any]] = []
    has_updates = False
    global_error = ""
    gateway: NotionGateway | None = None

    try:
        token = cfg.notion_token()
        gateway = NotionGateway(token)
    except Exception as exc:  # noqa: BLE001
        global_error = str(exc)
        logger.exception("Build sync status failed to initialize Notion gateway")

    for logical_db, database_id in cfg.databases.items():
        snap = snapshots.get(logical_db)
        row = {
            "logical_db": logical_db,
            "database_id": database_id,
            "local_synced_at": snap["synced_at"] if snap else None,
            "local_page_count": int(snap["page_count"]) if snap else 0,
            "local_latest_page_edited": snap["latest_page_edited_time"] if snap else None,
            "remote_page_count": None,
            "remote_latest_edited": None,
            "status": "needs_sync" if not snap else "unknown",
            "note": "从未同步" if not snap else "",
        }

        if not gateway:
            if not snap:
                has_updates = True
            if global_error and snap:
                row["note"] = global_error
            rows.append(row)
            continue

        try:
            schema = gateway.get_database(database_id)
            gateway.get_default_data_source_id(database_id, database_obj=schema)
            remote_pages = gateway.query_database_all(database_id=database_id, page_size=100)

            remote_page_count = len(remote_pages)
            remote_latest_page = max_datetime(
                [parse_iso_datetime(item.get("last_edited_time")) for item in remote_pages]
            )
            schema_last_edited = parse_iso_datetime(schema.get("last_edited_time"))
            remote_latest = max_datetime([remote_latest_page, schema_last_edited])

            row["remote_page_count"] = remote_page_count
            row["remote_latest_edited"] = datetime_to_iso(remote_latest)

            local_synced_at = parse_iso_datetime(row["local_synced_at"])
            needs_sync = False
            reasons: list[str] = []

            if not snap:
                needs_sync = True
                reasons.append("从未同步")
            if snap and remote_page_count != row["local_page_count"]:
                needs_sync = True
                reasons.append("页面数量变化")
            if snap and remote_latest and (not local_synced_at or remote_latest > local_synced_at):
                needs_sync = True
                reasons.append("远端在上次同步后有更新")

            if needs_sync:
                row["status"] = "needs_sync"
                row["note"] = "; ".join(reasons)
                has_updates = True
            else:
                row["status"] = "up_to_date"
                row["note"] = "已是最新"
        except Exception as exc:  # noqa: BLE001
            row["status"] = "unknown"
            row["note"] = str(exc)
            logger.exception("Build sync status failed for logical_db=%s", logical_db)

        rows.append(row)

    return {
        "checked_at": datetime.now(CN_TZ).isoformat(),
        "rows": rows,
        "has_updates": has_updates,
        "global_error": global_error,
    }

