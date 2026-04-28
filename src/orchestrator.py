#!/usr/bin/env python3
from __future__ import annotations

import threading
import time
from typing import Any, Callable

from .logger import Logger


class _PeriodicJobRunner:
    def __init__(
        self,
        name: str,
        cfg: dict[str, Any],
        logger: Logger,
        callback: Callable[[dict[str, Any]], None],
    ) -> None:
        self._name = name
        self._logger = logger
        self._callback = callback

        self.enabled = bool(cfg.get("enabled", False))
        self.interval_sec = max(1, int(cfg.get("interval_sec", 300)))
        self.run_on_active_only = bool(cfg.get("run_on_active_only", True))
        self.options = dict(cfg)

        self._running = False
        self._active = False
        self._thread: threading.Thread | None = None
        self._next_run_at = 0.0
        self._lock = threading.Lock()

    def start(self) -> None:
        if not self.enabled or self._running:
            return

        self._running = True
        self._next_run_at = time.monotonic() + self.interval_sec
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"job-{self._name}",
        )
        self._thread.start()
        self._logger.info(
            "[Orchestrator] job=%s started interval=%ds active_only=%s",
            self._name,
            self.interval_sec,
            self.run_on_active_only,
        )

    def set_active(self, is_active: bool) -> None:
        self._active = is_active

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        if self.enabled:
            self._logger.info("[Orchestrator] job=%s stopped", self._name)

    def _loop(self) -> None:
        while self._running:
            now = time.monotonic()
            if now < self._next_run_at:
                time.sleep(min(1.0, self._next_run_at - now))
                continue

            self._next_run_at = now + self.interval_sec

            if self.run_on_active_only and not self._active:
                self._logger.debug(
                    "[Orchestrator] job=%s skipped (standby)",
                    self._name,
                )
                continue

            if not self._lock.acquire(blocking=False):
                self._logger.warning(
                    "[Orchestrator] job=%s previous run still in progress, skip",
                    self._name,
                )
                continue

            try:
                ctx = {
                    "job_name": self._name,
                    "scheduled_at": time.time(),
                    "interval_sec": self.interval_sec,
                    "options": self.options,
                }
                self._callback(ctx)
            except Exception:
                self._logger.exception("[Orchestrator] job=%s run failed", self._name)
            finally:
                self._lock.release()


class AppOrchestrator:
    def __init__(
        self,
        cfg: dict[str, Any],
        logger: Logger,
        processing_callback: Callable[[dict[str, Any]], None],
        sync_callback: Callable[[dict[str, Any]], None],
        etc_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        pipeline_cfg = cfg.get("pipeline", {})

        self._processing = _PeriodicJobRunner(
            name="processing",
            cfg=pipeline_cfg.get("processing", {}),
            logger=logger,
            callback=processing_callback,
        )
        self._sync = _PeriodicJobRunner(
            name="sync",
            cfg=pipeline_cfg.get("sync", {}),
            logger=logger,
            callback=sync_callback,
        )
        self._etc = None
        if etc_callback is not None:
            self._etc = _PeriodicJobRunner(
                name="etc",
                cfg=pipeline_cfg.get("etc", {}),
                logger=logger,
                callback=etc_callback,
            )

    def start(self) -> None:
        self._processing.start()
        self._sync.start()
        if self._etc:
            self._etc.start()

    def set_active(self, is_active: bool) -> None:
        self._processing.set_active(is_active)
        self._sync.set_active(is_active)
        if self._etc:
            self._etc.set_active(is_active)

    def stop(self) -> None:
        self._processing.stop()
        self._sync.stop()
        if self._etc:
            self._etc.stop()