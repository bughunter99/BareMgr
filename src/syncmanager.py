#!/usr/bin/env python3
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterable

from .db_registry import build_registry, resolve_dsn
from .logger import Logger
from .oracleconnectionmanager import OracleConnectionManager
from .oracle_driver import get_cx_oracle
from .oracle_utils import validate_oracle_connection
from .processing import ProcessingBase


class SyncManager(ProcessingBase):
    """Sync worker: query sync targets, then run per-table sync in worker threads."""

    def __init__(
        self,
        cfg: dict[str, Any],
        logger: Logger,
        connection_manager: OracleConnectionManager | None = None,
        **_: Any,
    ) -> None:
        sync_cfg = cfg.get("syncmanager", {}) or cfg.get("pipeline", {}).get("syncmanager", {})
        self._tables = [str(t).strip().upper() for t in (sync_cfg.get("tables", []) or []) if str(t).strip()]
        self._dry_run = bool(sync_cfg.get("dry_run", True))

        db_registry = build_registry(cfg)
        self._db_registry = db_registry
        source_alias = str(sync_cfg.get("source_db", "")).strip()
        target_alias = str(sync_cfg.get("target_db", "")).strip()
        self._source_db_alias = source_alias
        self._target_db_alias = target_alias
        source_dsn = str(sync_cfg.get("source_dsn", "")).strip()
        target_dsn = str(sync_cfg.get("target_dsn", "")).strip()

        self._source_dsn = resolve_dsn(db_registry, source_alias, fallback=source_dsn) if source_alias else source_dsn
        self._target_dsn = resolve_dsn(db_registry, target_alias, fallback=target_dsn) if target_alias else target_dsn

        self._conn_manager = connection_manager or OracleConnectionManager(logger)
        self._owns_conn_manager = connection_manager is None

        super().__init__(job_name="sync", section_cfg=sync_cfg, logger=logger)

    def fetch_items(self, _ctx: dict[str, Any]) -> Iterable[dict[str, str]]:
        return [{"table": table} for table in self._tables]

    def process_item(self, item: dict[str, str], _ctx: dict[str, Any]) -> None:
        table = str(item.get("table", "")).strip().upper()
        if not table:
            return

        self._set_status(current_table=table)

        source_count = self._estimate_source_count(table)
        if self._dry_run:
            self._logger.info("[sync] dry-run table=%s source_count=%d", table, source_count)
            return

        if not self._source_dsn or not self._target_dsn:
            raise ValueError("sync requires source_dsn and target_dsn when dry_run=false")

        # Minimal sync example: ensure target table exists and copy all rows.
        self._copy_table_full(table)

    def _estimate_source_count(self, table: str) -> int:
        if not self._source_dsn:
            return 0

        with self._open_cursor(
            db_alias=self._source_db_alias,
            dsn=self._source_dsn,
        ) as cur:
            cur.execute(f"SELECT COUNT(1) FROM {table}")
            row = cur.fetchone()
            return int(row[0]) if row else 0

    @contextmanager
    def _open_cursor(self, *, db_alias: str, dsn: str):
        if db_alias:
            with self._conn_manager.cursor_by_alias(
                db_alias,
                fallback_dsn=dsn,
            ) as cur:
                yield cur
            return

        conn = self._conn_manager.get_connection(dsn, threaded=True)
        cur = conn.cursor()
        try:
            validate_oracle_connection(conn)
            yield cur
        finally:
            cur.close()

    def _copy_table_full(self, table: str) -> None:
        cx_oracle = get_cx_oracle()

        src_conn = self._conn_manager.get_connection(self._source_dsn, threaded=True)
        tgt_conn = self._conn_manager.get_connection(self._target_dsn, threaded=True)
        src_cur = src_conn.cursor()
        tgt_cur = tgt_conn.cursor()

        try:
            validate_oracle_connection(src_conn)
            validate_oracle_connection(tgt_conn)

            src_cur.execute(f"SELECT * FROM {table}")
            columns = [desc[0] for desc in src_cur.description]

            try:
                col_defs = ", ".join(f"{col} VARCHAR2(4000)" for col in columns)
                tgt_cur.execute(f"CREATE TABLE {table} ({col_defs})")
                tgt_conn.commit()
            except cx_oracle.DatabaseError as exc:
                error_obj = exc.args[0] if exc.args else None
                if getattr(error_obj, "code", None) != 955:
                    raise

            tgt_cur.execute(f"DELETE FROM {table}")

            placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
            col_sql = ", ".join(columns)
            insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

            while True:
                rows = src_cur.fetchmany(500)
                if not rows:
                    break
                tgt_cur.executemany(insert_sql, rows)
            tgt_conn.commit()
        finally:
            src_cur.close()
            tgt_cur.close()

    def close(self) -> None:
        if self._owns_conn_manager:
            self._conn_manager.close_all()


class SyncCheckpointStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_checkpoint (
                job_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                last_value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_name, table_name)
            )
            """
        )
        conn.commit()
        conn.close()

    def get(self, job_name: str, table_name: str) -> str | None:
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                cur = conn.execute(
                    """
                    SELECT last_value
                    FROM sync_checkpoint
                    WHERE job_name = ? AND table_name = ?
                    """,
                    (job_name, table_name),
                )
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                conn.close()

    def set(self, job_name: str, table_name: str, value: str) -> None:
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                conn.execute(
                    """
                    INSERT INTO sync_checkpoint (job_name, table_name, last_value, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(job_name, table_name)
                    DO UPDATE SET
                        last_value = excluded.last_value,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (job_name, table_name, value),
                )
                conn.commit()
            finally:
                conn.close()


class SyncOracleToOracle(SyncManager):
    pass


# Backward-compatible symbol for existing tests/call sites.
SyncJobManager = SyncManager
