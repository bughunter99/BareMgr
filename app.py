#!/usr/bin/env python3
"""
app.py — Active/Standby 통합 앱.

동작 흐름
─────────
1. FailoverNode가 active/standby를 결정한다.
2. active 전환 시:
   - 모든 Collector를 활성화
   - Replicator.start_publisher() → 수집 데이터를 Standby에 전송
   - Replicator.stop_subscriber()
3. standby 전환 시:
   - 모든 Collector를 비활성화
   - Replicator.start_subscriber() → Active에서 오는 복제 데이터 수신
   - Replicator.stop_publisher()
4. 외부에서 stop 명령 수신 시 graceful shutdown.

신규 수집기 추가 방법
──────────────────────
  1. BaseCollector를 상속해 collect() 구현
  2. App._build_collectors()에 등록
"""

import signal
import threading
import time
from typing import Any

from collector import BaseCollector
from config_loader import load_config
from failover import Failovernode
from failover_db import FailoverNode_db
from failover_zmq import FailoverNode_zmq
from logger import Logger
from oracle_collector import OracleCollector
from replicator import Replicator
from splunk_collector import SplunkCollector
from store import Store


class App:
    def __init__(self, config_file: str) -> None:
        self._cfg: dict[str, Any] = load_config(config_file)
        logging_cfg = self._cfg.get("logging", {})

        # ── 로거 ─────────────────────────────────────────────────────
        self._logger = Logger(
            name=f"app.{self._cfg['node_id']}",
            log_base=logging_cfg.get("log_base", self._cfg.get("log_base", "logs/app")),
        )

        # ── 저장소 ───────────────────────────────────────────────────
        self._store = Store(self._cfg["sqlite"]["path"])

        # ── 복제기 ───────────────────────────────────────────────────
        self._replicator = Replicator(
            cfg=self._cfg["replication"],
            store=self._store,
            logger=self._logger,
        )

        # ── 수집기 목록 ─────────────────────────────────────────────
        self._collectors: list[BaseCollector] = self._build_collectors()

        # ── Failover 노드 ────────────────────────────────────────────
        self._node: Failovernode = self._build_failover_node(config_file)

        # 상태 전환 감시 스레드
        self._prev_active: bool | None = None
        self._watcher_thread: threading.Thread | None = None
        self._running = False

    # ── 수집기 등록 ──────────────────────────────────────────────────
    def _build_collectors(self) -> list[BaseCollector]:
        collectors: list[BaseCollector] = []
        col_cfg = self._cfg.get("collectors", {})

        if col_cfg.get("oracle", {}).get("enabled"):
            collectors.append(
                OracleCollector(
                    cfg=col_cfg["oracle"],
                    store=self._store,
                    logger=self._logger,
                    on_collect=self._on_collect,
                )
            )

        if col_cfg.get("splunk", {}).get("enabled"):
            collectors.append(
                SplunkCollector(
                    cfg=col_cfg["splunk"],
                    store=self._store,
                    logger=self._logger,
                    on_collect=self._on_collect,
                )
            )

        return collectors

    def _build_failover_node(self, config_file: str) -> Failovernode:
        failover_cfg = self._cfg.get("failover", {})
        backend = str(failover_cfg.get("backend", "zmq")).strip().lower()

        if backend == "zmq":
            return FailoverNode_zmq(config_file=config_file, logger=self._logger)
        if backend == "db":
            return FailoverNode_db(config_file=config_file, logger=self._logger)

        raise ValueError(f"Unsupported failover.backend: {backend}")

    # ── 수집 콜백 (active일 때 peer로 복제) ─────────────────────────
    def _on_collect(self, table: str, rows: list[dict]) -> None:
        if self._node.isActive:
            self._replicator.publish(table, rows)

    # ── Active/Standby 전환 처리 ─────────────────────────────────────
    def _apply_role(self, is_active: bool) -> None:
        if is_active:
            self._logger.info("[App] → ACTIVE: starting collectors and publisher")
            self._replicator.stop_subscriber()
            self._replicator.start_publisher()
            for c in self._collectors:
                c.set_active(True)
        else:
            self._logger.info("[App] → STANDBY: stopping collectors and starting subscriber")
            for c in self._collectors:
                c.set_active(False)
            self._replicator.stop_publisher()
            self._replicator.start_subscriber()

    def _watch_role(self) -> None:
        """FailoverNode의 isActive 변화를 감지해 역할 전환."""
        while self._running:
            current = self._node.isActive
            if current != self._prev_active:
                self._apply_role(current)
                self._prev_active = current
            time.sleep(0.5)

    # ── 생명 주기 ─────────────────────────────────────────────────────
    def start(self) -> None:
        self._logger.info(
            "[App] starting node_id=%s weight=%s",
            self._cfg["node_id"],
            self._cfg["weight"],
        )
        self._running = True

        # 수집기 스레드 시작 (처음엔 standby 상태이므로 collect 건너뜀)
        for c in self._collectors:
            c.start()

        # Standby 상태로 시작: 복제 수신기를 켜두기
        self._replicator.start_subscriber()

        # 역할 감시 스레드
        self._watcher_thread = threading.Thread(
            target=self._watch_role,
            daemon=True,
            name="role-watcher",
        )
        self._watcher_thread.start()

        # OS 시그널 처리 (graceful shutdown)
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        self._logger.info("[App] started. press Ctrl+C or send SIGTERM to stop.")

        # FailoverNode.start()는 블로킹 루프 → 여기서 실행
        try:
            self._node.start()
        finally:
            self.stop()

    def _handle_signal(self, signum, _frame) -> None:
        self._logger.info("[App] received signal %d, stopping...", signum)
        # stop()이 node.stop()을 호출하고 start()의 finally도 stop()을 호출하지만
        # _running 플래그로 이중 실행을 막는다.
        self.stop()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._logger.info("[App] shutting down...")

        # watcher 스레드 종료 대기
        if self._watcher_thread and self._watcher_thread.is_alive():
            self._watcher_thread.join(timeout=2.0)

        # FailoverNode 블로킹 루프 종료
        self._node.stop()

        for c in self._collectors:
            c.stop()

        self._replicator.close()
        self._store.close()
        self._logger.info("[App] shutdown complete")
        self._logger.stop()

    # ── 컨텍스트 매니저 ──────────────────────────────────────────────
    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.stop()
