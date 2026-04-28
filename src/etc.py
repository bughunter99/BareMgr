#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
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
from .store import Store


class EtcManager:
    def __init__(
        self,
        cfg: dict[str, Any],
        store: Store,
        logger: Logger,
        connection_manager: OracleConnectionManager | None = None,
    ) -> None:
        self._cfg = cfg
        self._store = store
        self._logger = logger

        etc_cfg = cfg.get("pipeline", {}).get("etc", {})
        self.enabled = bool(etc_cfg.get("enabled", False))
        self._workers = max(1, int(etc_cfg.get("workers", 1)))
        self._tasks = list(etc_cfg.get("tasks", []))
        self._last_run_at: dict[str, float] = {}
        self._task_lock = threading.Lock()
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "enabled": self.enabled,
            "workers": self._workers,
            "task_count": len(self._tasks),
            "active_tasks": 0,
            "last_task": "",
            "last_status": "idle",
            "last_error": "",
        }

        sqlite_cfg = etc_cfg.get("sqlite_log", {})
        self._etc_log_path = Path(
            str(sqlite_cfg.get("db_path", self._store.base_dir / "pipeline" / "etc_runs.db"))
        )
        self._etc_log_path.parent.mkdir(parents=True, exist_ok=True)
        self._etc_conn = sqlite3.connect(str(self._etc_log_path), check_same_thread=False)
        self._etc_conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etc_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL,
                detail TEXT,
                affected_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self._etc_conn.commit()
        self._etc_lock = threading.Lock()

        oracle_cfg = etc_cfg.get("oracle", {})
        collector_oracle_dsn = cfg.get("collectors", {}).get("oracle", {}).get("dsn", "")
        self._oracle_dsn = str(oracle_cfg.get("dsn") or collector_oracle_dsn).strip()
        self._connection_manager = connection_manager or OracleConnectionManager(logger)
        self._owns_connection_manager = connection_manager is None

    def run(self, ctx: dict[str, Any]) -> None:
        if not self.enabled:
            return

        now = time.time()
        due_tasks: list[tuple[str, dict[str, Any]]] = []
        with self._task_lock:
            for task in self._tasks:
                if not bool(task.get("enabled", True)):
                    continue
                name = str(task.get("name", task.get("type", "etc-task"))).strip()
                interval_sec = max(1, int(task.get("interval_sec", 60)))
                last_run = self._last_run_at.get(name, 0.0)
                if now - last_run < interval_sec:
                    continue

                self._last_run_at[name] = now
                due_tasks.append((name, task))

        if not due_tasks:
            return

        if self._workers == 1 or len(due_tasks) == 1:
            for name, task in due_tasks:
                self._run_task(name=name, task=task, ctx=ctx)
            return

        with ThreadPoolExecutor(max_workers=self._workers) as ex:
            futures = [
                ex.submit(self._run_task, name=name, task=task, ctx=ctx)
                for name, task in due_tasks
            ]
            for future in futures:
                future.result()

    def _run_task(self, name: str, task: dict[str, Any], ctx: dict[str, Any]) -> None:
        task_type = str(task.get("type", "sqlite_heartbeat")).strip().lower()
        started = time.perf_counter()
        self._task_started(name)
        try:
            if task_type == "sqlite_heartbeat":
                affected = self._task_sqlite_heartbeat(name, task)
                self._log_run(name, task_type, "ok", "heartbeat upserted", affected)
            elif task_type == "sqlite_purge_table":
                affected = self._task_sqlite_purge_table(name, task)
                self._log_run(name, task_type, "ok", "purge completed", affected)
            elif task_type == "oracle_probe":
                affected = self._task_oracle_probe(name, task)
                self._log_run(name, task_type, "ok", "oracle probe completed", affected)
            else:
                self._log_run(name, task_type, "skip", f"unsupported task type: {task_type}", 0)
                self._logger.warning("[Etc] task=%s unsupported type=%s", name, task_type)
                return

            elapsed = time.perf_counter() - started
            self._logger.info("[Etc] task=%s type=%s done affected=%d elapsed=%.3fs", name, task_type, affected, elapsed)
        except Exception as exc:
            self._set_status(last_error=str(exc), last_status="error")
            self._log_run(name, task_type, "error", str(exc), 0)
            self._logger.exception("[Etc] task=%s type=%s failed", name, task_type)
        finally:
            self._task_completed(name)

    def _task_sqlite_heartbeat(self, name: str, task: dict[str, Any]) -> int:
        table = str(task.get("table", "etc_heartbeat"))
        row = {
            "task": name,
            "node_id": self._cfg.get("node_id", "node"),
            "status": "ok",
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        return self._store.upsert_many(table, [row])

    def _task_sqlite_purge_table(self, name: str, task: dict[str, Any]) -> int:
        table = str(task.get("table", "")).strip()
        if not table:
            raise ValueError("sqlite_purge_table requires table")

        retention_minutes = max(1, int(task.get("retention_minutes", 1440)))
        db_path = self._store.base_dir / f"{self._normalize_table_for_file(table)}.db"
        if not db_path.exists():
            self._logger.debug("[Etc] purge skipped table=%s file missing path=%s", table, db_path)
            return 0

        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        try:
            cur = conn.execute(
                f"DELETE FROM \"{table}\" WHERE collected_at < datetime('now', ?)",
                (f"-{retention_minutes} minutes",),
            )
            conn.commit()
            return int(cur.rowcount if cur.rowcount is not None else 0)
        finally:
            conn.close()

    def _task_oracle_probe(self, name: str, task: dict[str, Any]) -> int:
        dsn = str(task.get("dsn") or self._oracle_dsn).strip()
        if not dsn:
            self._logger.warning("[Etc] task=%s oracle_probe skipped (dsn missing)", name)
            return 0

        conn = self._get_oracle_conn(dsn)
        validate_oracle_connection(conn)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM dual")
            _ = cursor.fetchone()
            return 1
        finally:
            cursor.close()

    def _get_oracle_conn(self, dsn: str):
        return self._connection_manager.get_connection(dsn, threaded=True)

    def _task_started(self, name: str) -> None:
        with self._status_lock:
            self._status["active_tasks"] = int(self._status.get("active_tasks", 0)) + 1
            self._status["last_task"] = name
            self._status["last_status"] = "running"
            self._status["last_error"] = ""

    def _task_completed(self, name: str) -> None:
        with self._status_lock:
            active_tasks = int(self._status.get("active_tasks", 0)) - 1
            self._status["active_tasks"] = max(0, active_tasks)
            self._status["last_task"] = name
            if self._status["active_tasks"] == 0 and self._status.get("last_status") != "error":
                self._status["last_status"] = "idle"

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            self._status.update(updates)

    def get_status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._status)

    def _normalize_table_for_file(self, table: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_]", "_", table).strip("_")
        if not normalized:
            raise ValueError(f"invalid table name for purge: {table}")
        return normalized.lower()

    def _log_run(self, task_name: str, task_type: str, status: str, detail: str, affected_count: int) -> None:
        with self._etc_lock:
            self._etc_conn.execute(
                """
                INSERT INTO etc_runs (task_name, task_type, status, detail, affected_count)
                VALUES (?, ?, ?, ?, ?)
                """,
                (task_name, task_type, status, detail, affected_count),
            )
            self._etc_conn.commit()

    def close(self) -> None:
        with self._etc_lock:
            self._etc_conn.close()
        if self._owns_connection_manager:
            self._connection_manager.close_all()