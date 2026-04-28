#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any

from .logger import Logger
from .oracle_connection_manager import OracleConnectionManager
from .oracle_driver import get_cx_oracle
from .oracle_utils import validate_oracle_connection


def _normalize_identifier(name: str) -> str:
    normalized = str(name).strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", normalized):
        raise ValueError(f"Invalid Oracle identifier: {name}")
    return normalized


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


class SyncBase:
    def __init__(self, cfg: dict[str, Any], logger: Logger, connection_manager=None) -> None:
        self._cfg = cfg
        self._logger = logger

        sync_cfg = cfg.get("sync", {}) or cfg.get("pipeline", {}).get("sync", {})
        self.enabled = bool(sync_cfg.get("enabled", False))
        self.mode = str(sync_cfg.get("mode", "incremental")).strip().lower()
        self.dry_run = bool(sync_cfg.get("dry_run", True))
        self.workers = max(1, int(sync_cfg.get("workers", 1)))
        self.batch_size = max(1, int(sync_cfg.get("batch_size", 500)))
        self.key_column = _normalize_identifier(str(sync_cfg.get("key_column", "ID")))
        self.timestamp_column = _normalize_identifier(
            str(sync_cfg.get("timestamp_column", "UPDATED_AT"))
        )
        self.source_dsn = str(sync_cfg.get("source_dsn", "")).strip()
        self.target_dsn = str(sync_cfg.get("target_dsn", "")).strip()

        table_names = sync_cfg.get("tables", []) or []
        self.tables = [_normalize_identifier(t) for t in table_names]
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "enabled": self.enabled,
            "running": False,
            "workers": self.workers,
            "tables": len(self.tables),
            "mode": self.mode,
            "dry_run": self.dry_run,
            "current_table": "",
            "last_rows": 0,
            "last_error": "",
        }

        checkpoint_path = str(
            sync_cfg.get("checkpoint_db", cfg.get("sqlite", {}).get("path", "data") + "/pipeline/sync_checkpoint.db")
        )
        self._checkpoint = SyncCheckpointStore(checkpoint_path)
        self._connection_manager = connection_manager or OracleConnectionManager(logger)
        self._source_pool_cfg = sync_cfg.get("source_pool", {}) or {}
        self._target_pool_cfg = sync_cfg.get("target_pool", {}) or {}

    def _connect(self, dsn: str) -> Any:
        cx_Oracle = get_cx_oracle()
        return cx_Oracle.connect(dsn, threaded=True)

    def _acquire_source_conn(self) -> tuple[Any, Any | None]:
        if bool(self._source_pool_cfg.get("enabled", False)):
            pool = self._connection_manager.get_session_pool(self._source_pool_cfg)
            return pool.acquire(), pool
        return self._connect(self.source_dsn), None

    def _acquire_target_conn(self) -> tuple[Any, Any | None]:
        if bool(self._target_pool_cfg.get("enabled", False)):
            pool = self._connection_manager.get_session_pool(self._target_pool_cfg)
            return pool.acquire(), pool
        return self._connect(self.target_dsn), None

    def _release_conn(self, conn, pool) -> None:
        if pool is not None:
            pool.release(conn)
        else:
            try:
                conn.close()
            except Exception:
                pass


class SyncOracleToOracle(SyncBase):
    def __init__(self, cfg: dict[str, Any], logger: Logger, connection_manager=None) -> None:
        super().__init__(cfg=cfg, logger=logger, connection_manager=connection_manager)
        sync_cfg = cfg.get("sync", {}) or cfg.get("pipeline", {}).get("sync", {})
        self._ensure_table = bool(sync_cfg.get("ensure_table", False))

    def run(self, ctx: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {"enabled": False, "tables": 0, "rows": 0}
        if not self.tables:
            self._logger.warning("[Sync] enabled but no tables configured")
            return {"enabled": True, "tables": 0, "rows": 0}

        started = time.perf_counter()
        self._set_status(running=True, current_table="", last_rows=0, last_error="")
        self._logger.info(
            "[Sync] started mode=%s dry_run=%s tables=%s workers=%d",
            self.mode,
            self.dry_run,
            self.tables,
            self.workers,
        )

        if self.dry_run:
            try:
                total = self._run_dry_run_counts()
                elapsed = time.perf_counter() - started
                self._set_status(last_rows=total)
                self._logger.info("[Sync] dry-run completed rows=%d elapsed=%.3fs", total, elapsed)
                return {"enabled": True, "tables": len(self.tables), "rows": total, "dry_run": True}
            finally:
                self._set_status(running=False, current_table="")

        if not self.source_dsn or not self.target_dsn:
            raise ValueError("sync source_dsn and target_dsn are required when dry_run=false")

        try:
            total_rows = self._run_sync_tables()

            elapsed = time.perf_counter() - started
            self._set_status(last_rows=total_rows)
            self._logger.info(
                "[Sync] completed tables=%d rows=%d elapsed=%.3fs",
                len(self.tables),
                total_rows,
                elapsed,
            )
            return {"enabled": True, "tables": len(self.tables), "rows": total_rows, "dry_run": False}
        except Exception as exc:
            self._set_status(last_error=str(exc))
            raise
        finally:
            self._set_status(running=False, current_table="")

    def _estimate_source_count(self, table: str) -> int:
        source_pool_enabled = bool(self._source_pool_cfg.get("enabled", False))
        if not self.source_dsn and not source_pool_enabled:
            return 0
        try:
            source_conn, source_pool = self._acquire_source_conn()
            validate_oracle_connection(source_conn)
            cur = source_conn.cursor()
            try:
                cur.execute(f"SELECT COUNT(1) FROM {table}")
                row = cur.fetchone()
                return int(row[0]) if row else 0
            finally:
                cur.close()
                self._release_conn(source_conn, source_pool)
        except Exception:
            self._logger.exception("[Sync] dry-run count failed table=%s", table)
            return 0

    def _run_dry_run_counts(self) -> int:
        if self.workers == 1 or len(self.tables) == 1:
            total = 0
            for table in self.tables:
                count = self._estimate_source_count(table)
                total += count
                self._logger.info("[Sync] dry-run table=%s rows=%d", table, count)
            return total

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            counts = list(ex.map(self._estimate_source_count, self.tables))

        total = 0
        for table, count in zip(self.tables, counts):
            total += count
            self._logger.info("[Sync] dry-run table=%s rows=%d", table, count)
        return total

    def _sync_one_table(self, table: str) -> int:
        self._set_status(current_table=table)
        source_conn, source_pool = self._acquire_source_conn()
        target_conn, target_pool = self._acquire_target_conn()
        try:
            validate_oracle_connection(source_conn)
            validate_oracle_connection(target_conn)
            synced = self._sync_table(table, source_conn, target_conn)
            self._logger.info("[Sync] table=%s synced_rows=%d", table, synced)
            return synced
        finally:
            self._release_conn(source_conn, source_pool)
            self._release_conn(target_conn, target_pool)

    def _run_sync_tables(self) -> int:
        if self.workers == 1 or len(self.tables) == 1:
            total = 0
            for table in self.tables:
                total += self._sync_one_table(table)
            return total

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            return sum(ex.map(self._sync_one_table, self.tables))

    def _sync_table(self, table: str, source_conn, target_conn) -> int:
        if self.mode == "full":
            return self._sync_table_full(table, source_conn, target_conn)
        return self._sync_table_incremental(table, source_conn, target_conn)

    def _sync_table_incremental(self, table: str, source_conn, target_conn) -> int:
        last_value = self._checkpoint.get("oracle_to_oracle", table)
        total = 0

        while True:
            columns, rows = self._fetch_incremental_batch(
                table=table,
                source_conn=source_conn,
                last_value=last_value,
            )
            if not rows:
                break

            if self._ensure_table:
                self._ensure_target_table(target_conn, table, columns)

            self._upsert_rows(target_conn, table, columns, rows)
            total += len(rows)

            new_last = self._extract_checkpoint_value(rows[-1], columns)
            if new_last is not None:
                last_value = new_last
                self._checkpoint.set("oracle_to_oracle", table, new_last)

            if len(rows) < self.batch_size:
                break

        return total

    def _sync_table_full(self, table: str, source_conn, target_conn) -> int:
        source_cur = source_conn.cursor()
        target_cur = target_conn.cursor()
        total = 0
        try:
            source_cur.execute(f"SELECT * FROM {table}")
            columns = [d[0] for d in source_cur.description]
            if self._ensure_table:
                self._ensure_target_table(target_conn, table, columns)

            target_cur.execute(f"DELETE FROM {table}")

            placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
            col_sql = ", ".join(columns)
            insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

            while True:
                rows = source_cur.fetchmany(self.batch_size)
                if not rows:
                    break
                target_cur.executemany(insert_sql, rows)
                total += len(rows)

            target_conn.commit()
            return total
        finally:
            target_cur.close()
            source_cur.close()

    def _fetch_incremental_batch(self, table: str, source_conn, last_value: str | None) -> tuple[list[str], list[tuple[Any, ...]]]:
        cur = source_conn.cursor()
        try:
            if last_value is None:
                sql = (
                    f"SELECT * FROM {table} "
                    f"ORDER BY {self.timestamp_column} "
                    f"FETCH FIRST {self.batch_size} ROWS ONLY"
                )
                cur.execute(sql)
            else:
                sql = (
                    f"SELECT * FROM {table} "
                    f"WHERE {self.timestamp_column} > TO_TIMESTAMP(:last_value, 'YYYY-MM-DD HH24:MI:SS.FF') "
                    f"ORDER BY {self.timestamp_column} "
                    f"FETCH FIRST {self.batch_size} ROWS ONLY"
                )
                cur.execute(sql, {"last_value": last_value})

            columns = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return columns, rows
        finally:
            cur.close()

    def _ensure_target_table(self, target_conn, table: str, columns: list[str]) -> None:
        cx_Oracle = get_cx_oracle()

        cur = target_conn.cursor()
        try:
            col_sql = ", ".join(f"{c} VARCHAR2(4000)" for c in columns)
            cur.execute(f"CREATE TABLE {table} ({col_sql})")
            target_conn.commit()
        except cx_Oracle.DatabaseError as exc:
            error_obj = exc.args[0] if exc.args else None
            if getattr(error_obj, "code", None) != 955:
                raise
        finally:
            cur.close()

    def _upsert_rows(self, target_conn, table: str, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        if self.key_column not in columns:
            raise ValueError(
                f"key_column={self.key_column} not found in source columns for table={table}: {columns}"
            )
        key_idx = columns.index(self.key_column)
        delete_sql = f"DELETE FROM {table} WHERE {self.key_column} = :1"

        placeholders = ", ".join(f":{idx + 1}" for idx in range(len(columns)))
        col_sql = ", ".join(columns)
        insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

        cur = target_conn.cursor()
        try:
            cur.executemany(delete_sql, [(row[key_idx],) for row in rows])
            cur.executemany(insert_sql, rows)
            target_conn.commit()
        finally:
            cur.close()

    def _extract_checkpoint_value(self, row: tuple[Any, ...], columns: list[str]) -> str | None:
        if self.timestamp_column not in columns:
            raise ValueError(
                f"timestamp_column={self.timestamp_column} not found in source columns: {columns}"
            )
        idx = columns.index(self.timestamp_column)
        value = row[idx]
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return str(value)

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            self._status.update(updates)

    def get_status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._status)


class SyncJobManager:
    def __init__(self, cfg: dict[str, Any], logger: Logger, connection_manager=None) -> None:
        self._logger = logger
        self._syncer = SyncOracleToOracle(cfg=cfg, logger=logger, connection_manager=connection_manager)

    def run(self, ctx: dict[str, Any]) -> None:
        result = self._syncer.run(ctx)
        self._logger.info("[Sync] result=%s", result)

    def get_status(self) -> dict[str, Any]:
        return self._syncer.get_status()