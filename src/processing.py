#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import threading
from typing import Any, Iterable

from .logger import Logger


class ProcessingBase(ABC):
    """Shared queue/worker execution skeleton for timer-driven jobs."""

    def __init__(
        self,
        *,
        job_name: str,
        section_cfg: dict[str, Any],
        logger: Logger,
    ) -> None:
        self._job_name = job_name
        self._cfg = section_cfg
        self._logger = logger

        self.enabled = bool(section_cfg.get("enabled", False))
        self._workers = max(1, int(section_cfg.get("workers", 1)))
        self._queue_size = max(0, int(section_cfg.get("queue_size", 0)))
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "enabled": self.enabled,
            "running": False,
            "worker_mode": "thread",
            "workers": self._workers,
            "queue_depth": 0,
            "queued_objects": 0,
            "active_workers": 0,
            "processed": 0,
            "failed": 0,
            "current_table": "",
            "last_error": "",
            "last_run_started_at": "",
            "last_run_completed_at": "",
        }
        self._executor: ThreadPoolExecutor | None = None
        self._executor_lock = threading.Lock()

    def run(self, ctx: dict[str, Any]) -> None:
        if not self.enabled:
            return

        started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._set_status(
            running=True,
            queue_depth=0,
            queued_objects=0,
            active_workers=0,
            processed=0,
            failed=0,
            current_table="",
            last_error="",
            last_run_started_at=started_at,
        )

        items = list(self.fetch_items(ctx))
        self._set_status(queue_depth=len(items), queued_objects=len(items))

        if items:
            executor = self._ensure_executor()
            queue_limit = self._queue_size if self._queue_size > 0 else len(items)
            submit_window = threading.Semaphore(max(1, min(queue_limit, len(items))))
            futures = []

            for item in items:
                submit_window.acquire()
                future = executor.submit(self._worker_once, item, ctx)
                future.add_done_callback(lambda _f, sem=submit_window: sem.release())
                futures.append(future)

            for future in futures:
                future.result()

        completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._set_status(
            running=False,
            queue_depth=0,
            active_workers=0,
            current_table="",
            last_run_completed_at=completed_at,
        )

    def _worker_once(self, item: Any, ctx: dict[str, Any]) -> None:
        self._adjust_active_workers(1)

        try:
            self.process_item(item, ctx)
            self._increment_status("processed")
        except Exception as exc:
            self._increment_status("failed")
            self._set_status(last_error=str(exc))
            self._logger.exception("[%s] worker failed error=%s", self._job_name, str(exc))
        finally:
            self._adjust_active_workers(-1)
            self._decrement_queue_depth()

    @abstractmethod
    def fetch_items(self, ctx: dict[str, Any]) -> Iterable[Any]:
        """Query and return iterable items to enqueue for this turn."""

    @abstractmethod
    def process_item(self, item: Any, ctx: dict[str, Any]) -> None:
        """Process a single queued item inside worker threads."""

    def get_status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._status)

    def close(self) -> None:
        with self._executor_lock:
            if self._executor is not None:
                self._executor.shutdown(wait=True)
                self._executor = None

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            self._status.update(updates)

    def _increment_status(self, key: str) -> None:
        with self._status_lock:
            self._status[key] = int(self._status.get(key, 0)) + 1

    def _adjust_active_workers(self, delta: int) -> None:
        with self._status_lock:
            current = int(self._status.get("active_workers", 0))
            self._status["active_workers"] = max(0, current + delta)

    def _decrement_queue_depth(self) -> None:
        with self._status_lock:
            current = int(self._status.get("queue_depth", 0))
            self._status["queue_depth"] = max(0, current - 1)

    def _ensure_executor(self) -> ThreadPoolExecutor:
        with self._executor_lock:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=self._workers,
                    thread_name_prefix=f"{self._job_name}-worker",
                )
            return self._executor
