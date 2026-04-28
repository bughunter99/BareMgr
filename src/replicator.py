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
import queue
import threading
import time
from typing import TYPE_CHECKING

import zmq

if TYPE_CHECKING:
    from .store import Store
    from .logger import Logger


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
        self._chunk_rows: int = int(cfg.get("chunk_rows", 1000))
        self._send_hwm: int = int(cfg.get("send_hwm", 200))
        self._recv_hwm: int = int(cfg.get("recv_hwm", 200))
        self._subscriber_queue_size: int = int(cfg.get("subscriber_queue_size", 200))
        self._store = store
        self._logger = logger
        self._ctx = zmq.Context()

        # Publisher (Active)
        self._pub_socks: list[zmq.Socket] = []
        self._pub_lock = threading.Lock()

        # Subscriber (Standby)
        self._sub_sock: zmq.Socket | None = None
        self._sub_thread: threading.Thread | None = None
        self._sub_store_thread: threading.Thread | None = None
        self._sub_running = False
        self._sub_queue: queue.Queue[dict | None] = queue.Queue(maxsize=self._subscriber_queue_size)

    # ── Publisher (Active 역할) ──────────────────────────────────────
    def start_publisher(self) -> None:
        """Active 전환 시 호출. 각 peer에 PUSH 소켓을 연결한다."""
        with self._pub_lock:
            if self._pub_socks:
                return  # 이미 시작됨
            for peer in self._peers:
                sock = self._ctx.socket(zmq.PUSH)
                sock.setsockopt(zmq.LINGER, 0)
                sock.setsockopt(zmq.SNDHWM, self._send_hwm)
                sock.connect(f"tcp://{peer}")
                self._pub_socks.append(sock)
        self._logger.info(
            "[Replicator] publisher started → peers=%s chunk_rows=%d send_hwm=%d",
            self._peers,
            self._chunk_rows,
            self._send_hwm,
        )

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
        started_at = time.perf_counter()
        chunk_count = max(1, (len(rows) + self._chunk_rows - 1) // self._chunk_rows)
        peer_count = 0
        sent_chunks = 0
        with self._pub_lock:
            for chunk_index in range(chunk_count):
                chunk_rows = rows[chunk_index * self._chunk_rows:(chunk_index + 1) * self._chunk_rows]
                payload = self._store.serialize(
                    table,
                    chunk_rows,
                    metadata={
                        "chunk_index": chunk_index,
                        "chunk_count": chunk_count,
                        "row_count": len(chunk_rows),
                    },
                )
                for sock in self._pub_socks:
                    try:
                        sock.send(payload, flags=zmq.NOBLOCK)
                        peer_count = len(self._pub_socks)
                        sent_chunks += 1
                    except zmq.Again:
                        self._logger.warning(
                            "[Replicator] send buffer full, dropped table=%s chunk=%d/%d rows=%d",
                            table,
                            chunk_index + 1,
                            chunk_count,
                            len(chunk_rows),
                        )
                    except zmq.error.ZMQError:
                        self._logger.exception("[Replicator] send error")
        elapsed = time.perf_counter() - started_at
        self._logger.info(
            "[Replicator] ACTIVE published table=%s rows=%d chunks=%d peers=%d sent=%d elapsed=%.3fs",
            table,
            len(rows),
            chunk_count,
            peer_count,
            sent_chunks,
            elapsed,
        )

    # ── Subscriber (Standby 역할) ────────────────────────────────────
    def start_subscriber(self) -> None:
        """Standby 전환 시 호출. PULL 소켓으로 Active의 데이터를 수신한다."""
        if self._sub_running:
            return
        self._sub_running = True
        self._sub_queue = queue.Queue(maxsize=self._subscriber_queue_size)
        self._sub_sock = self._ctx.socket(zmq.PULL)
        self._sub_sock.setsockopt(zmq.LINGER, 0)
        self._sub_sock.setsockopt(zmq.RCVHWM, self._recv_hwm)
        self._sub_sock.setsockopt(zmq.RCVTIMEO, 500)
        self._sub_sock.bind(f"tcp://*:{self._port}")
        self._sub_thread = threading.Thread(
            target=self._recv_loop,
            daemon=True,
            name="replicator-sub",
        )
        self._sub_store_thread = threading.Thread(
            target=self._store_loop,
            daemon=True,
            name="replicator-store",
        )
        self._sub_thread.start()
        self._sub_store_thread.start()
        self._logger.info(
            "[Replicator] subscriber started on port=%d recv_hwm=%d queue_size=%d",
            self._port,
            self._recv_hwm,
            self._subscriber_queue_size,
        )

    def stop_subscriber(self) -> None:
        self._sub_running = False
        if self._sub_thread and self._sub_thread.is_alive():
            self._sub_thread.join(timeout=2.0)
        try:
            self._sub_queue.put_nowait(None)
        except queue.Full:
            pass
        if self._sub_store_thread and self._sub_store_thread.is_alive():
            self._sub_store_thread.join(timeout=5.0)
        if self._sub_sock:
            self._sub_sock.close(0)
            self._sub_sock = None
        self._logger.info("[Replicator] subscriber stopped")

    def _recv_loop(self) -> None:
        while self._sub_running:
            try:
                recv_started = time.perf_counter()
                payload = self._sub_sock.recv()
                data = json.loads(payload)
                table = data.get("table")
                rows = data.get("rows", [])
                chunk_index = int(data.get("chunk_index", 0)) + 1
                chunk_count = int(data.get("chunk_count", 1))
                jid = self._logger.new_jid(prefix="REP")
                data["_jid"] = jid
                with self._logger.job_context(jid=jid, prefix="REP"):
                    self._logger.info(
                        "[Replicator] STANDBY received table=%s rows=%d chunk=%d/%d",
                        table,
                        len(rows),
                        chunk_index,
                        chunk_count,
                    )
                data["_received_elapsed"] = time.perf_counter() - recv_started
                data["_enqueued_at"] = time.perf_counter()
                self._sub_queue.put(data, timeout=1.0)
                with self._logger.job_context(jid=jid, prefix="REP"):
                    self._logger.info(
                        "[Replicator] STANDBY enqueued table=%s rows=%d queue_depth=%d",
                        table,
                        len(rows),
                        self._sub_queue.qsize(),
                    )
            except queue.Full:
                self._logger.warning("[Replicator] subscriber queue full, dropping payload")
            except zmq.Again:
                pass  # RCVTIMEO 만료 → 루프 계속
            except zmq.error.ContextTerminated:
                break
            except Exception:
                self._logger.exception("[Replicator] recv error")

    def _store_loop(self) -> None:
        while self._sub_running or not self._sub_queue.empty():
            try:
                data = self._sub_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if data is None:
                break

            try:
                table = data.get("table")
                rows = data.get("rows", [])
                chunk_index = int(data.get("chunk_index", 0)) + 1
                chunk_count = int(data.get("chunk_count", 1))
                recv_elapsed = float(data.get("_received_elapsed", 0.0))
                enqueued_at = float(data.get("_enqueued_at", time.perf_counter()))
                jid = data.get("_jid")
                queue_wait = time.perf_counter() - enqueued_at
                with self._logger.job_context(jid=jid, prefix="REP"):
                    store_started = time.perf_counter()
                    self._store.replicate_message(data)
                    store_elapsed = time.perf_counter() - store_started
                    self._logger.info(
                        "[Replicator] STANDBY insert done table=%s rows=%d chunk=%d/%d recv_elapsed=%.3fs queue_wait=%.3fs store_elapsed=%.3fs queue_depth=%d",
                        table,
                        len(rows),
                        chunk_index,
                        chunk_count,
                        recv_elapsed,
                        queue_wait,
                        store_elapsed,
                        self._sub_queue.qsize(),
                    )
            except Exception:
                self._logger.exception("[Replicator] store error")

    # ── 전체 종료 ────────────────────────────────────────────────────
    def close(self) -> None:
        self.stop_publisher()
        self.stop_subscriber()
        self._ctx.term()
