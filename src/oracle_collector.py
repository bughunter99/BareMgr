#!/usr/bin/env python3
"""
oracle_collector.py — Oracle DB 주기 수집기.

· 수집 잡은 oracle_jobs.py의 ORACLE_JOBS에 코드로 정의한다.
· use_last_ts=True 인 잡은 :last_ts 바인드 변수로 증분 수집한다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import TYPE_CHECKING

from .collector import BaseCollector
from .db_registry import resolve_dsn, resolve_pool_cfg
from .oracle_connection_manager import OracleConnectionManager
from .oracle_jobs import ORACLE_JOBS, OracleJob
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
        _registry = db_registry or {}
        db_alias = str(cfg.get("db", "")).strip()
        if db_alias:
            self._dsn = resolve_dsn(_registry, db_alias, fallback=self._dsn)
        # pool: alias 우선, 없으면 oracle_pool 섹션 fallback
        pool_from_alias = resolve_pool_cfg(_registry, db_alias)
        legacy_pool = cfg.get("oracle_pool", {}) or {}
        self._pool_cfg = pool_from_alias or legacy_pool
        self._pool_enabled = bool(self._pool_cfg.get("enabled", False))

    # ── 연결 관리 ────────────────────────────────────────────────────
    def _get_conn(self):
        return self._connection_manager.get_connection(self._dsn, threaded=True)

    def _close_conn(self) -> None:
        self._connection_manager.invalidate(self._dsn, threaded=True)

    def _acquire_conn(self):
        if self._pool_enabled:
            pool = self._connection_manager.get_session_pool(self._pool_cfg)
            return pool.acquire(), pool
        return self._get_conn(), None

    # ── 수집 ────────────────────────────────────────────────────────
    def collect(self) -> list[tuple[str, list[dict], str]]:
        if self._test_mode:
            return self._collect_test_rows()

        results: list[tuple[str, list[dict], str]] = []
        conn, _pool = self._acquire_conn()
        validate_oracle_connection(conn)
        cursor = conn.cursor()

        for job in self._jobs:
            jid = self.logger.new_jid(prefix="ORA")
            with self.logger.job_context(jid=jid, prefix="ORA"):
                try:
                    started_at = time.perf_counter()
                    if job.use_last_ts:
                        cursor.execute(job.sql, {"last_ts": self._last_ts[job.name]})
                    else:
                        cursor.execute(job.sql)
                    cursor.rowfactory = makeDictFactory(cursor)
                    rows = list(cursor.fetchall())
                    elapsed = time.perf_counter() - started_at
                    if rows:
                        results.append((job.table, rows, jid))
                        self._last_ts[job.name] = datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    self.logger.info(
                        "[OracleCollector] job=%s table=%s rows=%d elapsed=%.3fs",
                        job.name,
                        job.table,
                        len(rows),
                        elapsed,
                    )
                except Exception:
                    self.logger.exception("[OracleCollector] job=%s query error", job.name)
                    cursor.close()
                    if _pool is not None:
                        _pool.release(conn)
                    else:
                        self._close_conn()
                    raise

        cursor.close()
        if _pool is not None:
            _pool.release(conn)
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
