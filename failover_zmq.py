#!/usr/bin/env python3
import threading
import time

import zmq

from config_loader import load_config
from logger import Logger


class FailoverNode:
    def __init__(self, config_file: str, logger: Logger | None = None):
        self.config = load_config(config_file)
        failover_cfg = self.config.get("failover", {})
        logging_cfg = self.config.get("logging", {})

        self.node_id = self.config["node_id"]
        self.weight = self.config["weight"]
        # Backward compatibility:
        # - old flat keys: peers/port/heartbeat_interval/log_base
        # - new grouped keys: failover.*, logging.log_base
        self.peers = failover_cfg.get("peers", self.config.get("peers", []))
        self.port = failover_cfg.get("port", self.config.get("port", 5555))
        self.heartbeat_interval = failover_cfg.get(
            "heartbeat_interval",
            self.config.get("heartbeat_interval", 2),
        )
        self.log_base = logging_cfg.get("log_base", self.config.get("log_base", "logs/failover"))

        self.isActive = False
        self.peers_state = {}
        self.ctx = zmq.Context()
        self.running = True
        self._stopping = False
        self._stop_lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._peer_reachable: dict[str, bool] = {}
        self._owns_logger = logger is None
        self.logger = logger or Logger(
            name=f"failover.{self.node_id}",
            log_base=self.log_base,
        )

    def start(self) -> None:
        """Start heartbeat server and client threads."""
        self._threads = [
            threading.Thread(target=self._heartbeat_server, daemon=True, name="hb-server"),
            threading.Thread(target=self._heartbeat_sender, daemon=True, name="hb-sender"),
            threading.Thread(target=self._leader_election, daemon=True, name="leader-election"),
        ]
        for th in self._threads:
            th.start()

        self.logger.info("[%s] Failover node started with weight=%s", self.node_id, self.weight)

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def _heartbeat_server(self) -> None:
        """Receive heartbeats and control commands from peers."""
        sock = self.ctx.socket(zmq.REP)
        sock.setsockopt(zmq.LINGER, 0)
        sock.bind(f"tcp://*:{self.port}")

        try:
            while self.running:
                try:
                    data = sock.recv_json(flags=zmq.NOBLOCK)
                    msg_type = data.get("type", "heartbeat")

                    if msg_type == "stop":
                        sock.send_json({"status": "stopping", "node_id": self.node_id})
                        self.logger.info("[%s] Received remote stop command", self.node_id)
                        self.stop()
                        break

                    if msg_type != "heartbeat":
                        sock.send_json({"status": "error", "reason": "unsupported_type"})
                        self.logger.warning(
                            "[%s] unsupported message type=%s",
                            self.node_id,
                            msg_type,
                        )
                        continue

                    peer_id = data["node_id"]
                    self.peers_state[peer_id] = {
                        "weight": data["weight"],
                        "timestamp": time.time(),
                    }
                    sock.send_json({"status": "ok"})
                except zmq.Again:
                    time.sleep(0.1)
                except zmq.error.ContextTerminated:
                    break
                except Exception:
                    self.logger.exception("[%s] heartbeat server error", self.node_id)
                    time.sleep(0.1)
        finally:
            sock.close(0)

    def _heartbeat_sender(self) -> None:
        """Send heartbeats to peers."""
        while self.running:
            for peer_addr in self.peers:
                endpoint = f"tcp://{peer_addr}"
                sock = self.ctx.socket(zmq.REQ)
                sock.setsockopt(zmq.LINGER, 0)
                sock.setsockopt(zmq.RCVTIMEO, 1000)
                sock.setsockopt(zmq.SNDTIMEO, 1000)

                try:
                    sock.connect(endpoint)
                    sock.send_json(
                        {
                            "type": "heartbeat",
                            "node_id": self.node_id,
                            "weight": self.weight,
                        }
                    )
                    sock.recv_json()
                    if self._peer_reachable.get(peer_addr) is not True:
                        self.logger.info("[%s] heartbeat restored peer=%s", self.node_id, peer_addr)
                    self._peer_reachable[peer_addr] = True
                except zmq.error.ContextTerminated:
                    sock.close(0)
                    return
                except Exception:
                    if self._peer_reachable.get(peer_addr, True):
                        self.logger.warning(
                            "[%s] heartbeat failed peer=%s",
                            self.node_id,
                            peer_addr,
                        )
                    self._peer_reachable[peer_addr] = False
                finally:
                    sock.close(0)

            time.sleep(self.heartbeat_interval)

    def _leader_election(self) -> None:
        """Check if this node should be active."""
        while self.running:
            now = time.time()
            stale = [
                peer_id
                for peer_id, state in self.peers_state.items()
                if now - state["timestamp"] > self.heartbeat_interval * 3
            ]
            for peer_id in stale:
                del self.peers_state[peer_id]

            max_weight = self.weight
            is_leader = True

            for peer_id, state in self.peers_state.items():
                if state["weight"] > max_weight:
                    max_weight = state["weight"]
                    is_leader = False
                elif state["weight"] == max_weight and peer_id > self.node_id:
                    is_leader = False

            old_status = self.isActive
            self.isActive = is_leader

            if self.isActive != old_status:
                status = "ACTIVE" if self.isActive else "STANDBY"
                self.logger.info(
                    "[%s] Status changed to %s (weight=%s)",
                    self.node_id,
                    status,
                    self.weight,
                )

            time.sleep(self.heartbeat_interval)

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

        self.ctx.term()
        self.logger.info("[%s] Stopped", self.node_id)
        if self._owns_logger:
            self.logger.stop()


def send_stop(endpoint: str, timeout_ms: int = 3000) -> dict:
    """Send stop command to a running failover node and return response."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
    sock.setsockopt(zmq.SNDTIMEO, timeout_ms)
    try:
        sock.connect(f"tcp://{endpoint}")
        sock.send_json({"type": "stop"})
        return sock.recv_json()
    finally:
        sock.close(0)
        ctx.term()


if __name__ == "__main__":
    node = FailoverNode("config.json")
    node.start()
