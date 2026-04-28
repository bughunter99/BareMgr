#!/usr/bin/env python3
from __future__ import annotations

from abc import ABC, abstractmethod
import threading
import time

from .config_loader import load_config
from .logger import Logger


class Failovernode(ABC):
    def __init__(self, config_file: str, logger: Logger | None = None):
        self.config = load_config(config_file)
        failover_cfg = self.config.get("failover", {})
        logging_cfg = self.config.get("logging", {})

        self.node_id = self.config["node_id"]
        self.weight = self.config["weight"]
        self.heartbeat_interval = failover_cfg.get(
            "heartbeat_interval",
            self.config.get("heartbeat_interval", 2),
        )
        self.fail_after_missed_heartbeats = failover_cfg.get("fail_after_missed_heartbeats", 2)
        self.status_interval = failover_cfg.get("status_interval", 5)
        self.log_base = logging_cfg.get("log_base", self.config.get("log_base", "logs/failover"))

        self.isActive = False
        self.peers_state: dict[str, dict] = {}
        self.running = True
        self._stopping = False
        self._stop_lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._status_provider = None
        self._owns_logger = logger is None
        self.logger = logger or Logger(
            name=f"failover.{self.node_id}",
            log_base=self.log_base,
        )

    def set_status_provider(self, provider) -> None:
        self._status_provider = provider

    def start(self) -> None:
        self._threads = self._build_threads()
        for th in self._threads:
            th.start()

        self.logger.info("[%s] Failover node started with weight=%s", self.node_id, self.weight)

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def _set_active(self, is_active: bool) -> None:
        old_status = self.isActive
        self.isActive = is_active

        if self.isActive != old_status:
            status = "ACTIVE" if self.isActive else "STANDBY"
            self.logger.info(
                "[%s] Status changed to %s (weight=%s)",
                self.node_id,
                status,
                self.weight,
            )

    def _status_reporter(self) -> None:
        while self.running:
            role = "ACTIVE" if self.isActive else "STANDBY"
            peer_status = self._format_peer_status()
            runtime_status = ""
            if self._status_provider is not None:
                try:
                    runtime_status = str(self._status_provider()).strip()
                except Exception:
                    self.logger.exception("[%s] status provider error", self.node_id)
                    runtime_status = "status_provider=error"

            if runtime_status:
                self.logger.info(
                    "[%s] runtime={%s} role=%s self_weight=%s peer_status={%s}",
                    self.node_id,
                    runtime_status,
                    role,
                    self.weight,
                    peer_status,
                )
            else:
                self.logger.info(
                    "[%s] role=%s self_weight=%s peer_status={%s}",
                    self.node_id,
                    role,
                    self.weight,
                    peer_status,
                )

            for _ in range(self.status_interval):
                if not self.running:
                    break
                time.sleep(1)

    def stop(self) -> None:
        with self._stop_lock:
            if self._stopping:
                return
            self._stopping = True
            self.running = False

        current = threading.current_thread()
        for th in self._threads:
            if th is current:
                continue
            if th.is_alive():
                th.join(timeout=1.0)

        self._cleanup()
        self.logger.info("[%s] Stopped", self.node_id)
        if self._owns_logger:
            self.logger.stop()

    @abstractmethod
    def _build_threads(self) -> list[threading.Thread]:
        raise NotImplementedError

    @abstractmethod
    def _format_peer_status(self) -> str:
        raise NotImplementedError

    def _cleanup(self) -> None:
        pass


FailoverNode = Failovernode
