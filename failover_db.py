#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
import re
import threading
import time

from failover import Failovernode
from logger import Logger


class FailoverNode_db(Failovernode):
    def __init__(self, config_file: str, logger: Logger | None = None):
        super().__init__(config_file=config_file, logger=logger)
        failover_cfg = self.config.get("failover", {})
        db_cfg = failover_cfg.get("db", {})
        oracle_cfg = self.config.get("collectors", {}).get("oracle", {})

        self._dsn = db_cfg.get("dsn") or oracle_cfg.get("dsn")
        if not self._dsn:
            raise ValueError("failover.db.dsn or collectors.oracle.dsn is required for DB failover")

        self._table = self._normalize_table_name(db_cfg.get("table", "FAILOVER_NODES"))
        self._ensure_table_enabled = db_cfg.get("ensure_table", True)
        self._conn = None
        self._db_lock = threading.Lock()

    def _build_threads(self) -> list[threading.Thread]:
        if self._ensure_table_enabled:
            self._ensure_table()

        return [
            threading.Thread(target=self._heartbeat_writer, daemon=True, name="db-heartbeat-writer"),
            threading.Thread(target=self._leader_election, daemon=True, name="db-leader-election"),
            threading.Thread(target=self._status_reporter, daemon=True, name="status-reporter"),
        ]

    def _heartbeat_writer(self) -> None:
        while self.running:
            try:
                self._write_heartbeat()
            except Exception:
                self.logger.exception("[%s] db heartbeat write error", self.node_id)
                self._reset_conn()
            time.sleep(self.heartbeat_interval)

    def _leader_election(self) -> None:
        while self.running:
            try:
                self.peers_state = self._read_peer_state()
                max_weight = self.weight
                is_leader = True

                for peer_id, state in self.peers_state.items():
                    if state["weight"] > max_weight:
                        max_weight = state["weight"]
                        is_leader = False
                    elif state["weight"] == max_weight and peer_id > self.node_id:
                        is_leader = False

                self._set_active(is_leader)
            except Exception:
                self.logger.exception("[%s] db leader election error", self.node_id)
                self._reset_conn()

            time.sleep(self.heartbeat_interval)

    def _format_peer_status(self) -> str:
        peer_status = ", ".join(
            f"{peer_id}:{state['weight']}:{state['status']}"
            for peer_id, state in sorted(self.peers_state.items())
        )
        return peer_status or "none"

    def _cleanup(self) -> None:
        try:
            self._delete_heartbeat()
        except Exception:
            self.logger.exception("[%s] db heartbeat delete error", self.node_id)
        self._close_conn()

    def _normalize_table_name(self, table_name: str) -> str:
        normalized = str(table_name).strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", normalized):
            raise ValueError(f"Invalid Oracle table name for failover.db.table: {table_name}")
        return normalized

    def _get_conn(self):
        import cx_Oracle  # type: ignore[import]

        with self._db_lock:
            if self._conn is None:
                self._conn = cx_Oracle.connect(self._dsn, threaded=True)
            return self._conn

    def _close_conn(self) -> None:
        with self._db_lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _reset_conn(self) -> None:
        self._close_conn()

    def _ensure_table(self) -> None:
        import cx_Oracle  # type: ignore[import]

        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE {self._table} (
                    node_id VARCHAR2(128) PRIMARY KEY,
                    weight NUMBER NOT NULL,
                    status VARCHAR2(16) NOT NULL,
                    last_seen TIMESTAMP(6) NOT NULL,
                    updated_at TIMESTAMP(6) NOT NULL
                )
                """
            )
            conn.commit()
            self.logger.info("[%s] ensured failover table created: %s", self.node_id, self._table)
        except cx_Oracle.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if getattr(error_obj, "code", None) != 955:
                raise
        finally:
            cursor.close()

    def _write_heartbeat(self) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                MERGE INTO {self._table} tgt
                USING (
                    SELECT :node_id AS node_id, :weight AS weight, :status AS status
                    FROM dual
                ) src
                ON (tgt.node_id = src.node_id)
                WHEN MATCHED THEN UPDATE SET
                    tgt.weight = src.weight,
                    tgt.status = src.status,
                    tgt.last_seen = SYSTIMESTAMP,
                    tgt.updated_at = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                    node_id,
                    weight,
                    status,
                    last_seen,
                    updated_at
                ) VALUES (
                    src.node_id,
                    src.weight,
                    src.status,
                    SYSTIMESTAMP,
                    SYSTIMESTAMP
                )
                """,
                {
                    "node_id": self.node_id,
                    "weight": self.weight,
                    "status": "ACTIVE" if self.isActive else "STANDBY",
                },
            )
            conn.commit()
        finally:
            cursor.close()

    def _read_peer_state(self) -> dict[str, dict]:
        conn = self._get_conn()
        cursor = conn.cursor()
        freshness_sec = self.heartbeat_interval * self.fail_after_missed_heartbeats
        try:
            cursor.execute(
                f"""
                SELECT node_id, weight, status, last_seen
                FROM {self._table}
                WHERE node_id <> :node_id
                  AND last_seen >= SYSTIMESTAMP - NUMTODSINTERVAL(:freshness_sec, 'SECOND')
                """,
                {
                    "node_id": self.node_id,
                    "freshness_sec": freshness_sec,
                },
            )

            peer_state: dict[str, dict] = {}
            for node_id, weight, status, last_seen in cursor.fetchall():
                if isinstance(last_seen, datetime):
                    timestamp = last_seen.timestamp()
                else:
                    timestamp = time.time()
                peer_state[node_id] = {
                    "weight": int(weight),
                    "status": status,
                    "timestamp": timestamp,
                }
            return peer_state
        finally:
            cursor.close()

    def _delete_heartbeat(self) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"DELETE FROM {self._table} WHERE node_id = :node_id",
                {"node_id": self.node_id},
            )
            conn.commit()
        finally:
            cursor.close()


FailoverNode = FailoverNode_db