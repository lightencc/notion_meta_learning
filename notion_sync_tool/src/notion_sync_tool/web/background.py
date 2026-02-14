from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from ..logging_utils import get_logger

CN_TZ = timezone(timedelta(hours=8))
logger = get_logger(__name__)
_running_jobs: set[asyncio.Task[Any]] = set()
_running_jobs_lock = threading.Lock()


class BatchTaskStore:
    def __init__(self, *, max_history: int = 200) -> None:
        self._max_history = max(50, max_history)
        self._tasks: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def now_iso() -> str:
        return datetime.now(CN_TZ).isoformat()

    def build(
        self,
        *,
        scope: str,
        action: str,
        suggestion_ids: list[int],
        back_url: str,
        done_redirect_url: str,
    ) -> dict[str, Any]:
        now = self.now_iso()
        return {
            "task_id": f"{scope}-{uuid.uuid4().hex[:12]}",
            "scope": scope,
            "action": action,
            "status": "running",
            "total": len(suggestion_ids),
            "processed": 0,
            "success": 0,
            "failed": 0,
            "current_item_id": None,
            "current_item_title": "",
            "last_error": "",
            "message": "任务已启动",
            "started_at": now,
            "updated_at": now,
            "finished_at": None,
            "back_url": back_url,
            "done_redirect_url": done_redirect_url,
        }

    def register(self, task: dict[str, Any]) -> str:
        with self._lock:
            self._tasks[task["task_id"]] = task
            completed = [
                item
                for item in self._tasks.values()
                if item.get("status") in {"completed", "failed"}
            ]
            if len(completed) > self._max_history:
                completed.sort(key=lambda item: item.get("updated_at", ""))
                for item in completed[: len(completed) - self._max_history]:
                    self._tasks.pop(str(item.get("task_id")), None)
            return str(task["task_id"])

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            return dict(task)

    def set(self, task_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return None
            patch["updated_at"] = self.now_iso()
            task.update(patch)
            return dict(task)


def run_in_background(func: Any, *args: Any) -> None:
    """Schedule a sync callable on the default thread pool, managed by asyncio."""
    loop = asyncio.get_running_loop()
    task = loop.create_task(asyncio.to_thread(func, *args))
    with _running_jobs_lock:
        _running_jobs.add(task)

    def _on_done(done_task: asyncio.Task[Any]) -> None:
        with _running_jobs_lock:
            _running_jobs.discard(done_task)
        try:
            done_task.result()
        except Exception:  # noqa: BLE001
            logger.exception("Background job failed unexpectedly")

    task.add_done_callback(_on_done)
