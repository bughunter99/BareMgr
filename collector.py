#!/usr/bin/env python3
"""
collector.py — 주기 수집기 기반 클래스.

· BaseCollector: 주기적으로 collect()를 실행하는 스레드 기반 골격.
· active 상태일 때만 collect()를 호출한다.
· collect()가 반환한 (table, rows)를 Store에 저장하고 Replicator로 복제한다.
"""

import threading
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from store import Store
    from logger import Logger


class BaseCollector(ABC):
    """
    서브클래스는 collect()만 구현하면 된다.

    collect()는 (table: str, rows: list[dict]) 리스트를 반환한다.
    여러 테이블에 나눠 저장할 수 있도록 리스트 형태로 반환한다.
    """

    def __init__(
        self,
        name: str,
        interval_sec: int,
        store: "Store",
        logger: "Logger",
        on_collect: Callable[[str, list[dict]], None] | None = None,
    ) -> None:
        self.name = name
        self.interval_sec = interval_sec
        self.store = store
        self.logger = logger
        # on_collect(table, rows) → Replicator 등 외부 콜백
        self.on_collect = on_collect
        self._active = False
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ── 서브클래스 구현 ───────────────────────────────────────────────
    @abstractmethod
    def collect(self) -> list[tuple[str, list[dict]]]:
        """
        데이터를 가져와 [(table_name, rows), ...] 또는
        [(table_name, rows, jid), ...] 형태로 반환한다.
        active 상태일 때만 호출된다.
        """

    # ── 활성화 제어 ──────────────────────────────────────────────────
    def set_active(self, active: bool) -> None:
        with self._lock:
            self._active = active

    @property
    def is_active(self) -> bool:
        with self._lock:
            return self._active

    # ── 생명 주기 ─────────────────────────────────────────────────────
    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"collector-{self.name}",
        )
        self._thread.start()
        self.logger.info("[Collector:%s] started (interval=%ds)", self.name, self.interval_sec)

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(2, self.interval_sec * 0.1))
        self.logger.info("[Collector:%s] stopped", self.name)

    # ── 내부 루프 ─────────────────────────────────────────────────────
    def _loop(self) -> None:
        while self._running:
            if self.is_active:
                try:
                    results = self.collect()
                    for item in results:
                        if len(item) == 3:
                            table, rows, jid = item
                        else:
                            table, rows = item
                            jid = None
                        if rows:
                            with self.logger.job_context(jid=jid, prefix=self.name.upper()) as active_jid:
                                started_at = time.perf_counter()
                                self.store.upsert_many(table, rows)
                                elapsed = time.perf_counter() - started_at
                                self.logger.info(
                                    "[Collector:%s] ACTIVE local insert done table=%s rows=%d elapsed=%.3fs",
                                    self.name,
                                    table,
                                    len(rows),
                                    elapsed,
                                )
                                if self.on_collect:
                                    self.on_collect(table, rows)
                except Exception:
                    self.logger.exception("[Collector:%s] collect error", self.name)
            else:
                self.logger.debug("[Collector:%s] standby, skip", self.name)

            # interval을 1초씩 쪼개서 대기 → 빠른 stop/active 전환 반응
            for _ in range(self.interval_sec):
                if not self._running:
                    break
                time.sleep(1)
