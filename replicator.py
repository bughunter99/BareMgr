#!/usr/bin/env python3
"""
replicator.py — Active→Standby 데이터 복제기.

구조
────
  [Active 노드]                    [Standby 노드]
  Replicator.publish(table, rows)
    └─ ZMQ PUSH ──────────────► ZMQ PULL → store.replicate_from()

· Active: Replicator.start_publisher() 호출 → PUSH 소켓으로 데이터 전송.
· Standby: Replicator.start_subscriber() 호출 → PULL 소켓으로 수신 후 Store 저장.
· 역할 전환 시 stop_publisher() / stop_subscriber()로 소켓을 닫고 반대 역할로 전환한다.

PUSH/PULL 패턴을 사용하는 이유
· PUB/SUB은 연결 전에 발행된 메시지가 유실될 수 있음.
· PUSH → N개의 PULL로 라운드로빈 전송. Standby가 여러 대여도 각 노드가 동일 데이터를 받으려면
  Active는 각 Standby마다 별도 PUSH 소켓을 맺는다.
"""

import json
import threading
from typing import TYPE_CHECKING

import zmq

if TYPE_CHECKING:
    from store import Store
    from logger import Logger


class Replicator:
    def __init__(
        self,
        cfg: dict,
        store: "Store",
        logger: "Logger",
    ) -> None:
        """
        cfg: config["replication"]
            {
                "port": 5556,
                "peers": ["192.168.1.10:5556", "192.168.1.11:5556"]
            }
        """
        self._port: int = cfg["port"]
        self._peers: list[str] = cfg.get("peers", [])
        self._store = store
        self._logger = logger
        self._ctx = zmq.Context()

        # Publisher (Active)
        self._pub_socks: list[zmq.Socket] = []
        self._pub_lock = threading.Lock()

        # Subscriber (Standby)
        self._sub_sock: zmq.Socket | None = None
        self._sub_thread: threading.Thread | None = None
        self._sub_running = False

    # ── Publisher (Active 역할) ──────────────────────────────────────
    def start_publisher(self) -> None:
        """Active 전환 시 호출. 각 peer에 PUSH 소켓을 연결한다."""
        with self._pub_lock:
            if self._pub_socks:
                return  # 이미 시작됨
            for peer in self._peers:
                sock = self._ctx.socket(zmq.PUSH)
                sock.setsockopt(zmq.LINGER, 0)
                sock.setsockopt(zmq.SNDHWM, 1000)
                sock.connect(f"tcp://{peer}")
                self._pub_socks.append(sock)
        self._logger.info("[Replicator] publisher started → peers=%s", self._peers)

    def stop_publisher(self) -> None:
        with self._pub_lock:
            for sock in self._pub_socks:
                sock.close(0)
            self._pub_socks.clear()
        self._logger.info("[Replicator] publisher stopped")

    def publish(self, table: str, rows: list[dict]) -> None:
        """수집된 데이터를 모든 peer에 전송한다."""
        if not rows:
            return
        payload = self._store.serialize(table, rows)
        with self._pub_lock:
            for sock in self._pub_socks:
                try:
                    sock.send(payload, flags=zmq.NOBLOCK)
                except zmq.Again:
                    self._logger.warning(
                        "[Replicator] send buffer full, dropped %d rows for %s",
                        len(rows), table,
                    )
                except zmq.error.ZMQError:
                    self._logger.exception("[Replicator] send error")

    # ── Subscriber (Standby 역할) ────────────────────────────────────
    def start_subscriber(self) -> None:
        """Standby 전환 시 호출. PULL 소켓으로 Active의 데이터를 수신한다."""
        if self._sub_running:
            return
        self._sub_running = True
        self._sub_sock = self._ctx.socket(zmq.PULL)
        self._sub_sock.setsockopt(zmq.LINGER, 0)
        self._sub_sock.setsockopt(zmq.RCVTIMEO, 500)
        self._sub_sock.bind(f"tcp://*:{self._port}")
        self._sub_thread = threading.Thread(
            target=self._recv_loop,
            daemon=True,
            name="replicator-sub",
        )
        self._sub_thread.start()
        self._logger.info("[Replicator] subscriber started on port=%d", self._port)

    def stop_subscriber(self) -> None:
        self._sub_running = False
        if self._sub_thread and self._sub_thread.is_alive():
            self._sub_thread.join(timeout=2.0)
        if self._sub_sock:
            self._sub_sock.close(0)
            self._sub_sock = None
        self._logger.info("[Replicator] subscriber stopped")

    def _recv_loop(self) -> None:
        while self._sub_running:
            try:
                payload = self._sub_sock.recv()
                self._store.replicate_from(payload)
                data = json.loads(payload)
                self._logger.debug(
                    "[Replicator] replicated %d rows → %s",
                    len(data.get("rows", [])), data.get("table"),
                )
            except zmq.Again:
                pass  # RCVTIMEO 만료 → 루프 계속
            except zmq.error.ContextTerminated:
                break
            except Exception:
                self._logger.exception("[Replicator] recv error")

    # ── 전체 종료 ────────────────────────────────────────────────────
    def close(self) -> None:
        self.stop_publisher()
        self.stop_subscriber()
        self._ctx.term()
