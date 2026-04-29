#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import queue
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

        stop_token = object()
        items = list(self.fetch_items(ctx))
        queue_limit = self._queue_size if self._queue_size > 0 else max(len(items), self._workers, 1)
        work_q: queue.Queue[Any] = queue.Queue(maxsize=queue_limit)

        for item in items:
            work_q.put(item)
        for _ in range(self._workers):
            work_q.put(stop_token)

        self._set_status(queue_depth=work_q.qsize(), queued_objects=len(items))

        workers = [
            threading.Thread(
                target=self._worker_loop,
                args=(work_q, stop_token, ctx),
                daemon=True,
                name=f"{self._job_name}-worker-{i}",
            )
            for i in range(self._workers)
        ]

        for th in workers:
            th.start()

        work_q.join()

        for th in workers:
            th.join(timeout=1.0)

        completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._set_status(
            running=False,
            queue_depth=0,
            active_workers=0,
            current_table="",
            last_run_completed_at=completed_at,
        )

    def _worker_loop(self, work_q: queue.Queue[Any], stop_token: object, ctx: dict[str, Any]) -> None:
        while True:
            item = work_q.get()
            if item is stop_token:
                work_q.task_done()
                return

            self._adjust_active_workers(1)
            self._set_status(queue_depth=work_q.qsize())

            try:
                self.process_item(item, ctx)
                self._increment_status("processed")
            except Exception as exc:
                self._increment_status("failed")
                self._set_status(last_error=str(exc))
                self._logger.exception("[%s] worker failed", self._job_name)
            finally:
                self._adjust_active_workers(-1)
                self._set_status(queue_depth=work_q.qsize())
                work_q.task_done()

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
        return

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
