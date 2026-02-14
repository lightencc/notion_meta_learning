from __future__ import annotations

import contextvars
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

_REQUEST_ID: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")


class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = _REQUEST_ID.get("-")
        return True


def set_request_id(value: str) -> contextvars.Token[str]:
    text = (value or "").strip() or "-"
    return _REQUEST_ID.set(text)


def reset_request_id(token: contextvars.Token[str]) -> None:
    _REQUEST_ID.reset(token)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def setup_logging(*, log_dir: Path | str | None = None, level: str | None = None) -> Path:
    root = logging.getLogger()
    if getattr(root, "_notion_sync_logging_ready", False):
        configured_dir = getattr(root, "_notion_sync_log_dir", None)
        if isinstance(configured_dir, Path):
            return configured_dir
        if isinstance(configured_dir, str):
            return Path(configured_dir)
        return Path("./data/logs").resolve()

    resolved_level = _resolve_level(level)
    resolved_dir = _resolve_log_dir(log_dir)
    resolved_dir.mkdir(parents=True, exist_ok=True)

    root.setLevel(resolved_level)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] [req=%(request_id)s] %(message)s"
    )
    req_filter = _RequestIdFilter()

    handlers: list[logging.Handler] = []

    console = logging.StreamHandler()
    console.setLevel(resolved_level)
    console.setFormatter(formatter)
    console.addFilter(req_filter)
    handlers.append(console)

    file_all = RotatingFileHandler(
        resolved_dir / "app.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_all.setLevel(resolved_level)
    file_all.setFormatter(formatter)
    file_all.addFilter(req_filter)
    handlers.append(file_all)

    file_err = RotatingFileHandler(
        resolved_dir / "error.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_err.setLevel(logging.WARNING)
    file_err.setFormatter(formatter)
    file_err.addFilter(req_filter)
    handlers.append(file_err)

    for handler in handlers:
        root.addHandler(handler)

    root._notion_sync_logging_ready = True  # type: ignore[attr-defined]
    root._notion_sync_log_dir = resolved_dir  # type: ignore[attr-defined]
    root._notion_sync_log_level = resolved_level  # type: ignore[attr-defined]

    # Reduce noisy third-party logs; keep application logs focused on business operations.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging initialized: dir=%s level=%s",
        str(resolved_dir),
        logging.getLevelName(resolved_level),
    )
    return resolved_dir


def _resolve_level(value: str | None) -> int:
    text = (value or os.getenv("NOTION_SYNC_LOG_LEVEL", "INFO")).strip().upper()
    mapping = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
    }
    return mapping.get(text, logging.INFO)


def _resolve_log_dir(value: Path | str | None) -> Path:
    if value is None:
        raw = os.getenv("NOTION_SYNC_LOG_DIR", "./data/logs").strip()
        return Path(raw).expanduser().resolve()
    if isinstance(value, Path):
        return value.expanduser().resolve()
    return Path(str(value)).expanduser().resolve()
