#!/usr/bin/env python3
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import queue
import random
import sqlite3
import threading
import time
from typing import Any

from .logger import Logger
from .oracle_connection_manager import OracleConnectionManager
from .drone_client import DroneClient
from .oracle_sessionpool import OracleResultWriter, OracleSessionPool
from .oracle_utils import makeDictFactory, validate_oracle_connection
from .store import Store


def _process_object_worker(obj: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    score = int(digest[:8], 16) % 1000

    result = dict(obj)
    result["result_status"] = "OK" if score % 7 else "WARN"
    result["result_score"] = score
    result["result_digest"] = digest
    result["processed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return result


class ObjectResultLogger:
    def __init__(self, db_path: str, logger: Logger) -> None:
        self._db_path = db_path
        self._logger = logger
        self._lock = threading.Lock()

        path = Path(self._db_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS object_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obj_id TEXT,
                node_id TEXT,
                status TEXT,
                score INTEGER,
                digest TEXT,
                payload_json TEXT,
                logged_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
        conn.close()

    @property
    def db_path(self) -> str:
        return self._db_path

    def log_many(self, results: list[dict[str, Any]]) -> int:
        if not results:
            return 0
        with self._lock:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            try:
                rows = [
                    (
                        str(item.get("obj_id", "")),
                        str(item.get("node_id", "")),
                        str(item.get("result_status", "")),
                        int(item.get("result_score", 0)),
                        str(item.get("result_digest", "")),
                        json.dumps(item, ensure_ascii=False, default=str),
                    )
                    for item in results
                ]
                conn.executemany(
                    """
                    INSERT INTO object_results (
                        obj_id,
                        node_id,
                        status,
                        score,
                        digest,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                conn.commit()
                return len(rows)
            finally:
                conn.close()


class ProcessingPipeline:
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
        self._connection_manager = connection_manager or OracleConnectionManager(logger)
        self._owns_connection_manager = connection_manager is None

        p_cfg = cfg.get("pipeline", {}).get("processing", {})
        self._node_id = str(cfg.get("node_id", "node"))
        self._batch_min = max(1, int(p_cfg.get("batch_min", 6000)))
        self._batch_max = max(self._batch_min, int(p_cfg.get("batch_max", 8000)))
        self._worker_mode = str(p_cfg.get("worker_mode", "multiprocessing")).strip().lower()
        self._workers = max(1, int(p_cfg.get("workers", 4)))
        self._output_table = str(p_cfg.get("output_table", "pipeline_generated_objects"))
        self._input_limit_per_table = max(1, int(p_cfg.get("input_limit_per_table", 500)))
        self._source_oracle_cfg = p_cfg.get("source_oracle", {}) or {}

        input_tables = p_cfg.get("input_tables", []) or []
        if input_tables:
            self._input_tables = [str(t) for t in input_tables]
        else:
            self._input_tables = self._collect_default_input_tables(cfg)

        self._current_oracle_cfg = p_cfg.get("current_oracle", {}) or {}
        self._worker_oracle_lookup_cfg = p_cfg.get("worker_oracle_lookup", {}) or {}
        self._worker_oracle_lookup_enabled = bool(self._worker_oracle_lookup_cfg.get("enabled", False))
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "running": False,
            "worker_mode": self._worker_mode,
            "workers": self._workers,
            "queue_depth": 0,
            "queued_objects": 0,
            "active_workers": 0,
            "last_result_count": 0,
            "last_run_started_at": "",
            "last_run_completed_at": "",
            "last_error": "",
        }

        self._oracle_pool = OracleSessionPool(p_cfg.get("oracle_pool", {}) or {}, logger)

        object_log_cfg = p_cfg.get("object_log", {}) or {}
        log_db_path = str(
            object_log_cfg.get(
                "db_path",
                self._store.base_dir / "pipeline" / "object_results.db",
            )
        )
        self._object_logger = ObjectResultLogger(log_db_path, logger)

        self._drone = DroneClient(p_cfg.get("drone", {}) or {}, logger)
        self._oracle_writer = OracleResultWriter(
            p_cfg.get("oracle_write", {}) or {},
            logger,
            connection_manager=self._connection_manager,
        )

    def _collect_default_input_tables(self, cfg: dict[str, Any]) -> list[str]:
        tables: list[str] = []
        collectors = cfg.get("collectors", {})

        for collector_name in ("oracle", "splunk"):
            jobs = collectors.get(collector_name, {}).get("jobs", []) or []
            for job in jobs:
                table = str(job.get("table", "")).strip()
                if table and table not in tables:
                    tables.append(table)
        return tables

    def _load_source_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for table in self._input_tables:
            try:
                table_rows = self._store.query(
                    f'SELECT * FROM "{table}" ORDER BY _rowid DESC LIMIT ?',
                    params=(self._input_limit_per_table,),
                    table=table,
                )
                rows.extend(table_rows)
            except Exception:
                self._logger.debug("[Processing] source load skipped table=%s", table)
        return rows

    def _load_current_oracle_rows(self) -> list[dict[str, Any]]:
        cfg = self._current_oracle_cfg
        if not bool(cfg.get("enabled", False)):
            return []

        dsn = str(cfg.get("dsn", "")).strip()
        sql = str(cfg.get("sql", "")).strip()
        limit = max(1, int(cfg.get("limit", 1000)))
        if not sql:
            return []

        if self._oracle_pool.enabled:
            return self._oracle_pool.fetch_many(sql=sql, params={}, limit=limit)

        if not dsn:
            return []

        conn = self._connection_manager.get_connection(dsn, threaded=True)
        cursor = conn.cursor()
        try:
            validate_oracle_connection(conn)
            cursor.execute(sql)
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchmany(limit))
        finally:
            cursor.close()

    def _load_main_job_oracle_rows(self, target_count: int) -> list[dict[str, Any]]:
        cfg = self._source_oracle_cfg
        if not bool(cfg.get("enabled", False)):
            return []

        sql = str(cfg.get("sql", "")).strip()
        if not sql:
            return []

        dsn = str(cfg.get("dsn", "")).strip()
        limit = max(1, int(cfg.get("limit", target_count)))
        fetch_size = min(target_count, limit)

        if self._oracle_pool.enabled:
            rows = self._oracle_pool.fetch_many(sql=sql, params={}, limit=fetch_size)
            return rows[:target_count]

        if not dsn:
            return []

        conn = self._connection_manager.get_connection(dsn, threaded=True)
        cursor = conn.cursor()
        try:
            validate_oracle_connection(conn)
            cursor.execute(sql)
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchmany(fetch_size))[:target_count]
        finally:
            cursor.close()

    def _worker_oracle_lookup(self, obj: dict[str, Any]) -> dict[str, Any] | None:
        if not self._worker_oracle_lookup_enabled:
            return None
        if not self._oracle_pool.enabled:
            return None

        sql = str(self._worker_oracle_lookup_cfg.get("sql", "")).strip()
        if not sql:
            return None

        bind_key = str(self._worker_oracle_lookup_cfg.get("bind_key", "lookup_key")).strip()
        object_field = str(self._worker_oracle_lookup_cfg.get("object_field", "batch_index")).strip()
        limit = max(1, int(self._worker_oracle_lookup_cfg.get("limit", 1)))
        value = obj.get(object_field)
        if value is None:
            value = self._worker_oracle_lookup_cfg.get("default_value", 0)

        rows = self._oracle_pool.fetch_many(sql=sql, params={bind_key: value}, limit=limit)
        if not rows:
            return None
        return rows[0]

    def _build_objects(
        self,
        source_rows: list[dict[str, Any]],
        main_oracle_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        count = len(main_oracle_rows)
        if not source_rows:
            source_rows = [{"source": "empty"}]
        if not main_oracle_rows:
            return []

        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        objects: list[dict[str, Any]] = []
        for idx in range(count):
            src = source_rows[idx % len(source_rows)]
            ora = main_oracle_rows[idx]
            obj_id = f"{self._node_id}-{int(time.time())}-{idx:05d}"
            objects.append(
                {
                    "obj_id": obj_id,
                    "job_id": obj_id,
                    "node_id": self._node_id,
                    "batch_index": idx,
                    "created_at": created_at,
                    "source_snapshot": src,
                    "oracle_snapshot": ora,
                }
            )
        return objects

    def _process_with_worker_queue(self, objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not objects:
            return []

        if self._worker_mode != "thread":
            self._logger.warning(
                "[Processing] queue-based main job uses shared resources; switching worker_mode %s -> thread",
                self._worker_mode,
            )

        work_q: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=max(len(objects), self._workers * 2))
        result_q: queue.Queue[dict[str, Any]] = queue.Queue()

        for obj in objects:
            work_q.put(obj)
        for _ in range(self._workers):
            work_q.put(None)

        self._set_status(queue_depth=work_q.qsize(), queued_objects=len(objects), active_workers=0)

        def _worker_loop() -> None:
            while True:
                item = work_q.get()
                try:
                    if item is None:
                        self._set_status(queue_depth=work_q.qsize())
                        return
                    self._adjust_active_workers(1)
                    self._set_status(queue_depth=work_q.qsize())
                    worker_name = threading.current_thread().name
                    job_id = str(item.get("job_id") or item.get("obj_id") or self._logger.new_jid(prefix="MAIN"))
                    obj_id = str(item.get("obj_id", ""))
                    batch_index = item.get("batch_index")
                    with self._logger.job_context(jid=job_id, prefix="MAIN"):
                        self._logger.info(
                            "[Processing] worker=%s dequeued entry obj_id=%s batch_index=%s queue_depth=%d",
                            worker_name,
                            obj_id,
                            batch_index,
                            work_q.qsize(),
                        )
                        try:
                            result = _process_object_worker(item)
                            lookup_row = self._worker_oracle_lookup(item)
                            if lookup_row is not None:
                                result["oracle_worker_snapshot"] = lookup_row
                            result_q.put(result)
                            self._logger.info(
                                "[Processing] worker=%s completed entry obj_id=%s result_status=%s queue_depth=%d",
                                worker_name,
                                obj_id,
                                result.get("result_status", ""),
                                work_q.qsize(),
                            )
                        except Exception:
                            self._logger.exception(
                                "[Processing] worker=%s failed entry obj_id=%s batch_index=%s",
                                worker_name,
                                obj_id,
                                batch_index,
                            )
                            raise
                finally:
                    if item is not None:
                        self._adjust_active_workers(-1)
                        self._set_status(queue_depth=work_q.qsize())
                    work_q.task_done()

        workers = [
            threading.Thread(target=_worker_loop, daemon=True, name=f"mainjob-worker-{idx}")
            for idx in range(self._workers)
        ]
        for th in workers:
            th.start()

        work_q.join()

        results: list[dict[str, Any]] = []
        while not result_q.empty():
            results.append(result_q.get())
        self._set_status(queue_depth=0, active_workers=0, last_result_count=len(results))
        return results

    def _process_parallel(self, objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not objects:
            return []

        effective_mode = self._worker_mode
        if self._worker_oracle_lookup_enabled and effective_mode != "thread":
            # Oracle session pool cannot be safely shared across multiprocessing workers.
            self._logger.warning(
                "[Processing] worker_oracle_lookup requires shared pool; switching worker_mode %s -> thread",
                effective_mode,
            )
            effective_mode = "thread"

        if effective_mode == "thread":
            def _thread_worker(obj: dict[str, Any]) -> dict[str, Any]:
                result = _process_object_worker(obj)
                lookup_row = self._worker_oracle_lookup(obj)
                if lookup_row is not None:
                    result["oracle_worker_snapshot"] = lookup_row
                return result

            with ThreadPoolExecutor(max_workers=self._workers) as ex:
                return list(ex.map(_thread_worker, objects))

        with ProcessPoolExecutor(max_workers=self._workers) as ex:
            return list(ex.map(_process_object_worker, objects))

    def run(self, ctx: dict[str, Any]) -> None:
        started = time.perf_counter()
        started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self._set_status(
            running=True,
            queue_depth=0,
            queued_objects=0,
            active_workers=0,
            last_result_count=0,
            last_error="",
            last_run_started_at=started_at,
        )
        self._logger.info("[Processing] run started job=%s", ctx.get("job_name", "processing"))
        try:
            source_rows = self._load_source_rows()
            target_count = random.randint(self._batch_min, self._batch_max)
            main_oracle_rows = self._load_main_job_oracle_rows(target_count)
            source_oracle_enabled = bool(self._source_oracle_cfg.get("enabled", False))

            if not main_oracle_rows:
                if source_oracle_enabled:
                    self._logger.warning(
                        "[Processing] source_oracle returned no rows (target_count=%d); skip run",
                        target_count,
                    )
                    return
                main_oracle_rows = [{"oracle": "empty"} for _ in range(target_count)]

            try:
                oracle_rows = self._load_current_oracle_rows()
            except Exception:
                self._logger.exception("[Processing] current oracle read failed")
                oracle_rows = []

            objects = self._build_objects(source_rows, main_oracle_rows)
            self._set_status(queued_objects=len(objects), queue_depth=len(objects))
            if oracle_rows:
                for idx, obj in enumerate(objects):
                    obj["oracle_current_snapshot"] = oracle_rows[idx % len(oracle_rows)]

            self._logger.info(
                "[Processing] queued objects=%d workers=%d",
                len(objects),
                self._workers,
            )
            results = self._process_with_worker_queue(objects)

            self._store.upsert_many(self._output_table, results)
            object_log_count = self._object_logger.log_many(results)
            drone_count = self._drone.send_many(results)
            oracle_count = self._oracle_writer.write_many(results)

            elapsed = time.perf_counter() - started
            self._logger.info(
                "[Processing] run completed objects=%d object_logs=%d drone_sent=%d oracle_written=%d elapsed=%.3fs log_db=%s",
                len(results),
                object_log_count,
                drone_count,
                oracle_count,
                elapsed,
                self._object_logger.db_path,
            )
        except Exception as exc:
            self._set_status(last_error=str(exc))
            raise
        finally:
            completed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            self._set_status(running=False, queue_depth=0, active_workers=0, last_run_completed_at=completed_at)

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            self._status.update(updates)

    def _adjust_active_workers(self, delta: int) -> None:
        with self._status_lock:
            current = int(self._status.get("active_workers", 0))
            self._status["active_workers"] = max(0, current + delta)

    def get_status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._status)

    def close(self) -> None:
        self._oracle_writer.close()
        self._oracle_pool.close()
        if self._owns_connection_manager:
            self._connection_manager.close_all()