from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from ..agent_workflow import AgentWorkflowService
from ..config import load_config
from ..knowledge_workflow import KnowledgeWorkflowService
from ..logging_utils import get_logger, reset_request_id, set_request_id, setup_logging
from ..postgres_store import PostgresStore
from .background import BatchTaskStore
from .helpers import build_sync_status, format_time_cn, status_to_zh, step_to_zh
from .routes_errors import build_errors_router
from .routes_knowledge import build_knowledge_router


def create_app() -> FastAPI:
    load_dotenv()
    config_path = os.getenv("NOTION_SYNC_TOOL_CONFIG", "./config.toml")
    cfg = load_config(Path(config_path))
    setup_logging(log_dir=cfg.log_dir)
    logger = get_logger(__name__)
    logger.info(
        "Web app initializing: config=%s postgres_schema=%s",
        config_path,
        cfg.postgres_schema,
    )
    store = PostgresStore(cfg.postgres_dsn, schema=cfg.postgres_schema)
    error_workflow = AgentWorkflowService(config=cfg, store=store)
    knowledge_workflow = KnowledgeWorkflowService(config=cfg, store=store)

    templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
    templates.env.filters["tz8"] = format_time_cn
    templates.env.filters["status_zh"] = status_to_zh
    templates.env.filters["step_zh"] = step_to_zh

    app = FastAPI(title="Notion 学习库工作台")

    @app.middleware("http")
    async def access_log_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
        request_id = request.headers.get("x-request-id", "").strip() or uuid.uuid4().hex[:12]
        token = set_request_id(request_id)
        start = time.perf_counter()
        try:
            response = await call_next(request)
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "HTTP %s %s?%s -> %s (%.1f ms)",
                request.method,
                request.url.path,
                request.url.query,
                response.status_code,
                elapsed_ms,
            )
            response.headers["X-Request-ID"] = request_id
            return response
        except Exception:  # noqa: BLE001
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.exception(
                "HTTP %s %s?%s -> 500 (%.1f ms)",
                request.method,
                request.url.path,
                request.url.query,
                elapsed_ms,
            )
            raise
        finally:
            reset_request_id(token)

    sync_cache_ttl_seconds = int(os.getenv("SYNC_STATUS_CACHE_SECONDS", "300"))
    pending_cache_ttl_seconds = int(os.getenv("PENDING_TARGETS_CACHE_SECONDS", "120"))
    runs_page_size = max(1, int(os.getenv("RUNS_PAGE_SIZE", "10")))
    max_batch_tasks = max(50, int(os.getenv("BATCH_TASK_HISTORY_LIMIT", "200")))

    dashboard_cache: dict[str, Any] = {
        "sync_status": None,
        "sync_status_at": 0.0,
        "error_pending": None,
        "error_pending_at": 0.0,
        "error_pending_error": "",
        "knowledge_pending": None,
        "knowledge_pending_at": 0.0,
        "knowledge_pending_error": "",
    }

    batch_store = BatchTaskStore(max_history=max_batch_tasks)

    def get_sync_status_cached(*, force: bool = False) -> tuple[dict[str, Any], bool]:
        now = time.time()
        is_fresh = (
            dashboard_cache["sync_status"] is not None
            and (now - float(dashboard_cache["sync_status_at"])) < sync_cache_ttl_seconds
        )
        if not force and is_fresh:
            return dashboard_cache["sync_status"], True
        sync_status = build_sync_status(cfg, store)
        dashboard_cache["sync_status"] = sync_status
        dashboard_cache["sync_status_at"] = now
        return sync_status, False

    def get_error_pending_cached(*, force: bool = False) -> tuple[int, str, bool]:
        now = time.time()
        is_fresh = (
            dashboard_cache["error_pending"] is not None
            and (now - float(dashboard_cache["error_pending_at"])) < pending_cache_ttl_seconds
        )
        if not force and is_fresh:
            return (
                int(dashboard_cache["error_pending"]),
                str(dashboard_cache["error_pending_error"] or ""),
                True,
            )
        try:
            pending = error_workflow.pending_target_count()
            err = ""
        except Exception as exc:  # noqa: BLE001
            pending = 0
            err = str(exc)
            logger.exception("Failed to compute error pending target count")
        dashboard_cache["error_pending"] = pending
        dashboard_cache["error_pending_at"] = now
        dashboard_cache["error_pending_error"] = err
        return pending, err, False

    def get_knowledge_pending_cached(*, force: bool = False) -> tuple[int, str, bool]:
        now = time.time()
        is_fresh = (
            dashboard_cache["knowledge_pending"] is not None
            and (now - float(dashboard_cache["knowledge_pending_at"])) < pending_cache_ttl_seconds
        )
        if not force and is_fresh:
            return (
                int(dashboard_cache["knowledge_pending"]),
                str(dashboard_cache["knowledge_pending_error"] or ""),
                True,
            )
        try:
            pending = knowledge_workflow.pending_target_count()
            err = ""
        except Exception as exc:  # noqa: BLE001
            pending = 0
            err = str(exc)
            logger.exception("Failed to compute knowledge pending target count")
        dashboard_cache["knowledge_pending"] = pending
        dashboard_cache["knowledge_pending_at"] = now
        dashboard_cache["knowledge_pending_error"] = err
        return pending, err, False

    @app.get("/", response_class=RedirectResponse)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/knowledge", status_code=303)

    app.include_router(
        build_errors_router(
            cfg=cfg,
            store=store,
            templates=templates,
            error_workflow=error_workflow,
            runs_page_size=runs_page_size,
            sync_cache_ttl_seconds=sync_cache_ttl_seconds,
            pending_cache_ttl_seconds=pending_cache_ttl_seconds,
            get_sync_status_cached=get_sync_status_cached,
            get_error_pending_cached=get_error_pending_cached,
            get_knowledge_pending_cached=get_knowledge_pending_cached,
            batch_store=batch_store,
        )
    )
    app.include_router(
        build_knowledge_router(
            cfg_model=cfg.gemini_model,
            cfg_threshold=cfg.confidence_threshold,
            store=store,
            templates=templates,
            knowledge_workflow=knowledge_workflow,
            runs_page_size=runs_page_size,
            pending_cache_ttl_seconds=pending_cache_ttl_seconds,
            get_knowledge_pending_cached=get_knowledge_pending_cached,
            batch_store=batch_store,
        )
    )

    return app
