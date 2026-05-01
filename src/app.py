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
import re
from typing import Any

from .basecollector import BaseCollector
from .config_loader import load_config
from .db_registry import build_registry
from .etcmanager import EtcManager
from .failovernode import FailoverNode
from .failovernodedb import FailoverNodeDb
from .failovernodezmq import FailoverNodeZmq
from .logger import Logger
from .oracleconnectionmanager import OracleConnectionManager
from .oraclecollector import OracleCollector
from .oracle_driver import init_oracle_client_from_config
from .apporchestrator import AppOrchestrator
from .businessmanager import BusinessManager
from .replicator import Replicator
from .splunkcollector import SplunkCollector
from .store import Store
from .syncmanager import SyncManager


class App:
    def __init__(self, config_file: str) -> None:
        self._cfg: dict[str, Any] = load_config(config_file)
        logging_cfg = self._cfg.get("logging", {})

        # ── 로거 ─────────────────────────────────────────────────────
        self._logger = Logger(
            name=f"app.{self._cfg['node_id']}",
            log_base=logging_cfg.get("log_base", self._cfg.get("log_base", "logs/app")),
        )
        init_oracle_client_from_config(self._cfg, logger=self._logger)
        self._oracle_connection_manager = OracleConnectionManager(self._logger)
        self._db_registry = build_registry(self._cfg)

        # ── 저장소 ───────────────────────────────────────────────────
        node_id = str(self._cfg.get("node_id", "")).strip()
        m = re.match(r"^node(\d+)$", node_id, re.IGNORECASE)
        default_sqlite_base = f"data/app{m.group(1)}" if m else f"data/{node_id or 'app'}"
        sqlite_base_dir = str(
            (self._cfg.get("sqlite", {}) or {}).get("path", default_sqlite_base)
        ).strip()
        self._store = Store(
            sqlite_base_dir,
            logger=self._logger,
            sqlite_cfg=self._cfg.get("sqlite", {}),
            replication_cfg=self._cfg.get("replication", {}),
            sqlite_connections=self._cfg.get("sqlite_connections", []),
        )
        self._store.initialize_registered_ddls()

        # ── 복제기 ───────────────────────────────────────────────────
        _replicator = Replicator(
            cfg=self._cfg["replication"],
            store=self._store,
            logger=self._logger,
        )
        self._business_manager = BusinessManager(
            cfg=self._cfg,
            store=self._store,
            logger=self._logger,
            connection_manager=self._oracle_connection_manager,
        )
        self._sync_manager = SyncManager(
            cfg=self._cfg,
            logger=self._logger,
            connection_manager=self._oracle_connection_manager,
        )
        self._etc_manager = EtcManager(
            cfg=self._cfg,
            store=self._store,
            logger=self._logger,
            connection_manager=self._oracle_connection_manager,
        )

        # ── 수집기 목록 ─────────────────────────────────────────────
        _collectors: list[BaseCollector] = self._build_collectors()

        # ── Failover 노드 ────────────────────────────────────────────
        self._node: FailoverNode = self._build_failover_node(config_file)
        self._node.set_status_provider(self._build_failover_runtime_status)

        # ── 주기 작업 오케스트레이터 ─────────────────────────────────
        self._orchestrator = AppOrchestrator(
            cfg=self._cfg,
            logger=self._logger,
            processing_callback=self._business_manager.run,
            sync_callback=self._sync_manager.run,
            etc_callback=self._etc_manager.run,
            collectors=_collectors,
            replicator=_replicator,
        )

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
                    connection_manager=self._oracle_connection_manager,
                    db_registry=self._db_registry,
                )
            )

        # splunk은 최상위 레벨에서 읽고, 없으면 collectors.splunk fallback
        splunk_cfg = self._cfg.get("splunk", {}) or col_cfg.get("splunk", {})
        if splunk_cfg.get("enabled"):
            collectors.append(
                SplunkCollector(
                    cfg=splunk_cfg,
                    store=self._store,
                    logger=self._logger,
                    on_collect=self._on_collect,
                )
            )

        return collectors

    def _build_failover_node(self, config_file: str) -> FailoverNode:
        failover_cfg = self._cfg.get("failover", {})
        backend = str(failover_cfg.get("backend", "zmq")).strip().lower()

        if backend == "zmq":
            return FailoverNodeZmq(config_file=config_file, logger=self._logger)
        if backend == "db":
            return FailoverNodeDb(config_file=config_file, logger=self._logger)

        raise ValueError(f"Unsupported failover.backend: {backend}")

    # ── 수집 콜백 (active일 때 peer로 복제) ─────────────────────────
    def _on_collect(self, table: str, rows: list[dict]) -> None:
        if self._node.isActive:
            self._orchestrator.publish(table, rows)

    # ── Active/Standby 전환 처리 ─────────────────────────────────────
    def _apply_role(self, is_active: bool) -> None:
        if is_active:
            self._logger.info("[App] → ACTIVE: starting collectors and publisher")
        else:
            self._logger.info("[App] → STANDBY: stopping collectors and starting subscriber")
        self._orchestrator.set_active(is_active)

    def _watch_role(self) -> None:
        """FailoverNode의 isActive 변화를 감지해 역할 전환."""
        while self._running:
            current = self._node.isActive
            if current != self._prev_active:
                self._apply_role(current)
                self._prev_active = current
            time.sleep(0.5)

    def _build_failover_runtime_status(self) -> str:
        collector_parts = []
        for collector in self._orchestrator.collectors:
            status = collector.get_status()
            collector_parts.append(
                f"{status['name']}:{'active' if status['active'] else 'standby'}/{ 'alive' if status['thread_alive'] else 'dead'}"
            )

        processing_status = self._business_manager.get_status()
        sync_status = self._sync_manager.get_status()
        etc_status = self._etc_manager.get_status()

        return (
            f"main_queue={processing_status.get('queue_depth', 0)} "
            f"processing=running:{processing_status.get('running', False)}/mode:{processing_status.get('worker_mode')}/"
            f"workers:{processing_status.get('workers')}/active:{processing_status.get('active_workers', 0)}/"
            f"queued:{processing_status.get('queued_objects', 0)} "
            f"collectors=[{'; '.join(collector_parts) if collector_parts else 'none'}] "
            f"sync=running:{sync_status.get('running', False)}/workers:{sync_status.get('workers')}/"
            f"table:{sync_status.get('current_table', '') or '-'} "
            f"etc=active_tasks:{etc_status.get('active_tasks', 0)}/workers:{etc_status.get('workers')}/"
            f"last_task:{etc_status.get('last_task', '') or '-'}"
        )

    # ── 생명 주기 ─────────────────────────────────────────────────────
    def start(self) -> None:
        self._logger.info(
            "[App] starting node_id=%s weight=%s",
            self._cfg["node_id"],
            self._cfg["weight"],
        )
        self._running = True

        # 수집기·복제기·processing/sync 스케줄러 시작
        # (수집기는 처음엔 standby 상태이므로 collect 건너뜀)
        self._orchestrator.start()

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

        self._orchestrator.stop()
        self._business_manager.close()
        self._etc_manager.close()
        self._oracle_connection_manager.close_all()
        self._store.close()
        self._logger.info("[App] shutdown complete")
        self._logger.stop()

    # ── 컨텍스트 매니저 ──────────────────────────────────────────────
    def __enter__(self):
        return self

    def __exit__(self, *_) -> None:
        self.stop()
