#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
import re
import threading
import time

from .failovernode import FailoverNode
from .logger import Logger
from .oracle_driver import get_cx_oracle


class FailoverNodeDb(FailoverNode):
    def __init__(self, config_file: str, logger: Logger | None = None):
        super().__init__(config_file=config_file, logger=logger)
        failover_cfg = self.config.get("failover", {})
        db_cfg = failover_cfg.get("db", {})

        self._dsn = db_cfg.get("dsn")
        if not self._dsn:
            raise ValueError("failover.db.dsn is required for DB failover")

        self._table = self._normalize_table_name(db_cfg.get("table", "FAILOVER_NODES"))
        columns_cfg = db_cfg.get("columns", {})
        self._col_node_id = self._normalize_table_name(columns_cfg.get("node_id", "NODE_ID"))
        self._col_weight = self._normalize_table_name(columns_cfg.get("weight", "WEIGHT"))
        self._col_status = self._normalize_table_name(columns_cfg.get("status", "STATUS"))
        self._col_last_seen = self._normalize_table_name(columns_cfg.get("last_seen", "LAST_SEEN"))
        self._col_updated_at = self._normalize_table_name(columns_cfg.get("updated_at", "UPDATED_AT"))
        self._ensure_table_enabled = db_cfg.get("ensure_table", True)

        cmd_cfg = db_cfg.get("command", {})
        cmd_cols = cmd_cfg.get("columns", {})
        self._cmd_table = self._normalize_table_name(cmd_cfg.get("table", "FAILOVER_COMMANDS"))
        self._cmd_col_id = self._normalize_table_name(cmd_cols.get("command_id", "COMMAND_ID"))
        self._cmd_col_node_id = self._normalize_table_name(cmd_cols.get("node_id", "NODE_ID"))
        self._cmd_col_command = self._normalize_table_name(cmd_cols.get("command", "COMMAND"))
        self._cmd_col_created_at = self._normalize_table_name(cmd_cols.get("created_at", "CREATED_AT"))
        self._cmd_col_processed_at = self._normalize_table_name(
            cmd_cols.get("processed_at", "PROCESSED_AT")
        )
        self._cmd_ensure_table = bool(cmd_cfg.get("ensure_table", True))
        self._cmd_poll_interval_sec = max(1, int(cmd_cfg.get("poll_interval_sec", 1)))

        self._conn = None
        self._db_lock = threading.Lock()

    def _build_threads(self) -> list[threading.Thread]:
        if self._ensure_table_enabled:
            self._ensure_table()
        if self._cmd_ensure_table:
            self._ensure_command_table()

        return [
            threading.Thread(target=self._heartbeat_writer, daemon=True, name="db-heartbeat-writer"),
            threading.Thread(target=self._leader_election, daemon=True, name="db-leader-election"),
            threading.Thread(target=self._command_watcher, daemon=True, name="db-command-watcher"),
            threading.Thread(target=self._status_reporter, daemon=True, name="status-reporter"),
        ]

    def _command_watcher(self) -> None:
        while self.running:
            try:
                if self._consume_stop_command():
                    self.logger.info("[%s] Received DB stop command", self.node_id)
                    self.stop()
                    return
            except Exception as e:
                self.logger.exception("[%s] db command watcher error=%s", self.node_id, str(e))
                self._reset_conn()
            time.sleep(self._cmd_poll_interval_sec)

    def _heartbeat_writer(self) -> None:
        while self.running:
            try:
                self._write_heartbeat()
            except Exception as e:
                self.logger.exception("[%s] db heartbeat write error=%s", self.node_id, str(e))
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
            except Exception as e:
                self.logger.exception("[%s] db leader election error=%s", self.node_id, str(e))
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
        except Exception as e:
            self.logger.exception("[%s] db heartbeat delete error=%s", self.node_id, str(e))
        self._close_conn()

    def _normalize_table_name(self, table_name: str) -> str:
        normalized = str(table_name).strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", normalized):
            raise ValueError(f"Invalid Oracle table name for failover.db.table: {table_name}")
        return normalized

    def _get_conn(self):
        cx_Oracle = get_cx_oracle()

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
        cx_Oracle = get_cx_oracle()

        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE {self._table} (
                    {self._col_node_id} VARCHAR2(128) PRIMARY KEY,
                    {self._col_weight} NUMBER NOT NULL,
                    {self._col_status} VARCHAR2(16) NOT NULL,
                    {self._col_last_seen} TIMESTAMP(6) NOT NULL,
                    {self._col_updated_at} TIMESTAMP(6) NOT NULL
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

    def _ensure_command_table(self) -> None:
        cx_Oracle = get_cx_oracle()

        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                CREATE TABLE {self._cmd_table} (
                    {self._cmd_col_id} NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    {self._cmd_col_node_id} VARCHAR2(128) NOT NULL,
                    {self._cmd_col_command} VARCHAR2(32) NOT NULL,
                    {self._cmd_col_created_at} TIMESTAMP(6) DEFAULT SYSTIMESTAMP NOT NULL,
                    {self._cmd_col_processed_at} TIMESTAMP(6)
                )
                """
            )
            conn.commit()
            self.logger.info("[%s] ensured failover command table created: %s", self.node_id, self._cmd_table)
        except cx_Oracle.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if getattr(error_obj, "code", None) != 955:
                raise
        finally:
            cursor.close()

    def _consume_stop_command(self) -> bool:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                SELECT {self._cmd_col_id}
                FROM {self._cmd_table}
                WHERE {self._cmd_col_node_id} = :node_id
                  AND {self._cmd_col_command} = 'STOP'
                  AND {self._cmd_col_processed_at} IS NULL
                ORDER BY {self._cmd_col_created_at}
                FETCH FIRST 1 ROWS ONLY
                """,
                {"node_id": self.node_id},
            )
            row = cursor.fetchone()
            if not row:
                return False

            command_id = row[0]
            cursor.execute(
                f"""
                UPDATE {self._cmd_table}
                SET {self._cmd_col_processed_at} = SYSTIMESTAMP
                WHERE {self._cmd_col_id} = :command_id
                """,
                {"command_id": command_id},
            )
            conn.commit()
            return True
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
                ON (tgt.{self._col_node_id} = src.node_id)
                WHEN MATCHED THEN UPDATE SET
                    tgt.{self._col_weight} = src.weight,
                    tgt.{self._col_status} = src.status,
                    tgt.{self._col_last_seen} = SYSTIMESTAMP,
                    tgt.{self._col_updated_at} = SYSTIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                    {self._col_node_id},
                    {self._col_weight},
                    {self._col_status},
                    {self._col_last_seen},
                    {self._col_updated_at}
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
                                SELECT
                                        {self._col_node_id} AS node_id,
                                        {self._col_weight} AS weight,
                                        {self._col_status} AS status,
                                        {self._col_last_seen} AS last_seen
                FROM {self._table}
                                WHERE {self._col_node_id} <> :node_id
                                    AND {self._col_last_seen} >= SYSTIMESTAMP - NUMTODSINTERVAL(:freshness_sec, 'SECOND')
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
                f"DELETE FROM {self._table} WHERE {self._col_node_id} = :node_id",
                {"node_id": self.node_id},
            )
            conn.commit()
        finally:
            cursor.close()


FailoverNode = FailoverNodeDb


def send_stop_db(
    dsn: str,
    node_id: str,
    table: str = "FAILOVER_COMMANDS",
    node_id_column: str = "NODE_ID",
    command_column: str = "COMMAND",
    created_at_column: str = "CREATED_AT",
) -> dict:
    cx_Oracle = get_cx_oracle()

    t = str(table).strip().upper()
    c_node = str(node_id_column).strip().upper()
    c_cmd = str(command_column).strip().upper()
    c_created = str(created_at_column).strip().upper()

    conn = cx_Oracle.connect(dsn, threaded=True)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            INSERT INTO {t} ({c_node}, {c_cmd}, {c_created})
            VALUES (:node_id, 'STOP', SYSTIMESTAMP)
            """,
            {"node_id": node_id},
        )
        conn.commit()
        return {"status": "ok", "node_id": node_id, "table": t, "command": "STOP"}
    finally:
        cursor.close()
        conn.close()
