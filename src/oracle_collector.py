#!/usr/bin/env python3
"""
oracle_collector.py — Oracle DB 주기 수집기.

· 수집 잡은 oracle_jobs.py의 ORACLE_JOBS에 코드로 정의한다.
· 각 잡은 db alias를 지정해 서로 다른 DB에서 조회할 수 있다.
· 각 잡은 post_process 함수로 쿼리 직후 결과를 후처리할 수 있다.
· use_last_ts=True 인 잡은 :last_ts 바인드 변수로 증분 수집한다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import inspect
import time
from typing import Any
from typing import TYPE_CHECKING

from .collector import BaseCollector
from .db_registry import resolve_dsn, resolve_pool_cfg
from .oracle_connection_manager import OracleConnectionManager
from .oracle_jobs import ORACLE_JOBS, OracleJob, PostProcessContext
from .oracle_utils import makeDictFactory, validate_oracle_connection

if TYPE_CHECKING:
    from .store import Store
    from .logger import Logger


class OracleCollector(BaseCollector):
    def __init__(
        self,
        cfg: dict,
        store: "Store",
        logger: "Logger",
        on_collect=None,
        connection_manager: OracleConnectionManager | None = None,
        db_registry: dict | None = None,
    ) -> None:
        super().__init__(
            name="oracle",
            interval_sec=cfg.get("interval_sec", 60),
            store=store,
            logger=logger,
            on_collect=on_collect,
        )
        self._dsn: str = cfg["dsn"] if "dsn" in cfg else ""
        self._jobs: list[OracleJob] = list(ORACLE_JOBS)
        self._test_cfg: dict = cfg.get("test", {})
        self._test_mode: bool = cfg.get("test_mode", False) or self._test_cfg.get("enabled", False)
        self._test_rows: int = int(self._test_cfg.get("rows", 5000))
        self._test_emit_once: bool = self._test_cfg.get("emit_once", True)
        self._test_emitted: bool = False
        self._test_batch_no: int = 0
        # 잡별 마지막 수집 시각 (UTC ISO8601)
        self._last_ts: dict[str, str] = {
            job.name: "1970-01-01 00:00:00" for job in self._jobs
        }
        self._connection_manager = connection_manager or OracleConnectionManager(logger)
        self._owns_connection_manager = connection_manager is None
        # db: alias 우선, 없으면 dsn: fallback
        self._db_registry: dict[str, dict] = db_registry or {}
        self._default_db_alias = str(cfg.get("db", "")).strip()
        if self._default_db_alias:
            self._dsn = resolve_dsn(self._db_registry, self._default_db_alias, fallback=self._dsn)
        # pool: alias 우선, 없으면 oracle_pool 섹션 fallback
        pool_from_alias = resolve_pool_cfg(self._db_registry, self._default_db_alias)
        legacy_pool = cfg.get("oracle_pool", {}) or {}
        self._default_pool_cfg = pool_from_alias or legacy_pool
        self._default_pool_enabled = bool(self._default_pool_cfg.get("enabled", False))

    # ── 연결 관리 ────────────────────────────────────────────────────
    def _get_conn(self, dsn: str):
        return self._connection_manager.get_connection(dsn, threaded=True)

    def _close_conn(self, dsn: str) -> None:
        self._connection_manager.invalidate(dsn, threaded=True)

    def _acquire_conn(self, *, dsn: str, pool_cfg: dict[str, Any], pool_enabled: bool):
        if pool_enabled:
            pool = self._connection_manager.get_session_pool(pool_cfg)
            return pool.acquire(), pool
        return self._get_conn(dsn), None

    def _resolve_job_connection(self, job: OracleJob) -> tuple[str, dict[str, Any], bool, str]:
        job_db_alias = str(job.db).strip() or self._default_db_alias
        dsn = self._dsn
        if job_db_alias:
            dsn = resolve_dsn(self._db_registry, job_db_alias, fallback=dsn)

        pool_cfg = self._default_pool_cfg
        pool_enabled = self._default_pool_enabled
        if job_db_alias:
            alias_pool_cfg = resolve_pool_cfg(self._db_registry, job_db_alias)
            if alias_pool_cfg:
                pool_cfg = alias_pool_cfg
                pool_enabled = bool(pool_cfg.get("enabled", False))

        if not str(dsn).strip():
            raise ValueError(
                f"Oracle DSN is required for job={job.name}. Check job.db alias or collectors.oracle.dsn"
            )

        return dsn, pool_cfg, pool_enabled, job_db_alias

    def _apply_post_process(
        self,
        *,
        job: OracleJob,
        rows: list[dict],
        job_db_alias: str,
        jid: str,
        collected_at: str,
    ) -> list[dict]:
        context: PostProcessContext = {
            "job_name": job.name,
            "table": job.table,
            "db_alias": job_db_alias,
            "jid": jid,
            "collected_at": collected_at,
        }

        def _run_one(fn, in_rows: list[dict]) -> list[dict]:
            try:
                arg_count = len(inspect.signature(fn).parameters)
            except (TypeError, ValueError):
                arg_count = 1

            if arg_count >= 2:
                out_rows = fn(in_rows, context)
            else:
                out_rows = fn(in_rows)

            if out_rows is None:
                raise ValueError(f"post_process must return list[dict], got None for job={job.name}")
            return out_rows

        processed = _run_one(job.post_process, rows)
        for fn in job.post_processes:
            processed = _run_one(fn, processed)
        return processed

    # ── 수집 ────────────────────────────────────────────────────────
    def collect(self) -> list[tuple[str, list[dict], str]]:
        if self._test_mode:
            return self._collect_test_rows()

        results: list[tuple[str, list[dict], str]] = []

        for job in self._jobs:
            jid = self.logger.new_jid(prefix="ORA")
            with self.logger.job_context(jid=jid, prefix="ORA"):
                dsn = ""
                conn = None
                _pool = None
                cursor = None
                failed = False
                try:
                    dsn, pool_cfg, pool_enabled, job_db_alias = self._resolve_job_connection(job)
                    conn, _pool = self._acquire_conn(
                        dsn=dsn,
                        pool_cfg=pool_cfg,
                        pool_enabled=pool_enabled,
                    )
                    validate_oracle_connection(conn)
                    cursor = conn.cursor()
                    started_at = time.perf_counter()
                    if job.use_last_ts:
                        cursor.execute(job.sql, {"last_ts": self._last_ts[job.name]})
                    else:
                        cursor.execute(job.sql)
                    cursor.rowfactory = makeDictFactory(cursor)
                    rows = list(cursor.fetchall())
                    collected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    rows = self._apply_post_process(
                        job=job,
                        rows=rows,
                        job_db_alias=job_db_alias,
                        jid=jid,
                        collected_at=collected_at,
                    )
                    elapsed = time.perf_counter() - started_at
                    if rows:
                        results.append((job.table, rows, jid))
                        self._last_ts[job.name] = collected_at
                    self.logger.info(
                        "[OracleCollector] job=%s db=%s table=%s rows=%d elapsed=%.3fs",
                        job.name,
                        job_db_alias or "<default>",
                        job.table,
                        len(rows),
                        elapsed,
                    )
                except Exception:
                    failed = True
                    self.logger.exception("[OracleCollector] job=%s query error", job.name)
                    raise
                finally:
                    if cursor is not None:
                        try:
                            cursor.close()
                        except Exception:
                            pass
                    if conn is not None and _pool is not None:
                        try:
                            _pool.release(conn)
                        except Exception:
                            pass
                    elif failed and conn is not None and dsn:
                        try:
                            self._close_conn(dsn)
                        except Exception:
                            pass

        return results

    def _collect_test_rows(self) -> list[tuple[str, list[dict], str]]:
        """Oracle 없이 테스트할 때 각 잡별 더미 데이터를 생성한다."""
        if self._test_emit_once and self._test_emitted:
            self.logger.debug("[OracleCollector:test] already emitted, skip")
            return []

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        batch_no = self._test_batch_no
        self._test_batch_no += 1

        results: list[tuple[str, list[dict], str]] = []
        for job in self._jobs:
            rows_count = job.test_rows
            jid = self.logger.new_jid(prefix="ORA")
            with self.logger.job_context(jid=jid, prefix="ORA"):
                started_at = time.perf_counter()
                rows = [
                    {
                        "id": f"{job.name}-batch{batch_no}-{i}",
                        "job": job.name,
                        "batch_no": str(batch_no),
                        "payload": f"dummy-payload-{i}",
                        "qty": (i * 7) % 1000,
                        "updated_at": now,
                        "source": "oracle_test_mode",
                    }
                    for i in range(rows_count)
                ]
                results.append((job.table, rows, jid))
                elapsed = time.perf_counter() - started_at
                self.logger.info(
                    "[OracleCollector:test] job=%s table=%s rows=%d batch=%d elapsed=%.3fs",
                    job.name,
                    job.table,
                    len(rows),
                    batch_no,
                    elapsed,
                )

        self._test_emitted = True
        return results

    def stop(self) -> None:
        super().stop()
        if self._owns_connection_manager:
            self._connection_manager.close_all()
