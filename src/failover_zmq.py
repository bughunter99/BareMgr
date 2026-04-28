#!/usr/bin/env python3
from __future__ import annotations

import threading
import time

import zmq

from .failover import Failovernode
from .logger import Logger


class FailoverNode_zmq(Failovernode):
    def __init__(self, config_file: str, logger: Logger | None = None):
        super().__init__(config_file=config_file, logger=logger)
        failover_cfg = self.config.get("failover", {})
        zmq_cfg = failover_cfg.get("zmq", {})
        # Backward compatibility:
        # - old flat keys: failover.peers/failover.port
        # - new grouped keys: failover.zmq.peers/failover.zmq.port
        self.peers = zmq_cfg.get(
            "peers",
            failover_cfg.get("peers", self.config.get("peers", [])),
        )
        self.port = zmq_cfg.get(
            "port",
            failover_cfg.get("port", self.config.get("port", 5555)),
        )
        self.ctx = zmq.Context()
        self._peer_reachable: dict[str, bool] = {}

    def _build_threads(self) -> list[threading.Thread]:
        return [
            threading.Thread(target=self._heartbeat_server, daemon=True, name="hb-server"),
            threading.Thread(target=self._heartbeat_sender, daemon=True, name="hb-sender"),
            threading.Thread(target=self._leader_election, daemon=True, name="leader-election"),
            threading.Thread(target=self._status_reporter, daemon=True, name="status-reporter"),
        ]

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
                if now - state["timestamp"] > self.heartbeat_interval * self.fail_after_missed_heartbeats
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

            self._set_active(is_leader)

            time.sleep(self.heartbeat_interval)

    def _format_peer_status(self) -> str:
        peer_status = ", ".join(
            f"{peer_id}:{state['weight']}"
            for peer_id, state in sorted(self.peers_state.items())
        )
        return peer_status or "none"

    def _cleanup(self) -> None:
        self.ctx.term()


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
    node = FailoverNode_zmq("config.json")
    node.start()


FailoverNode = FailoverNode_zmq
