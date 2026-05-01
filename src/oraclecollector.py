#!/usr/bin/env python3
"""
oraclecollector.py — Oracle DB 주기 수집기.

· 수집 잡은 oraclejobs.py의 ORACLE_JOBS에 코드로 정의한다.
· 각 잡은 db alias를 지정해 서로 다른 DB에서 조회할 수 있다.
· 각 잡은 query(config, cursor) 함수에서 SQL 실행/바인딩/결과 가공을 처리한다.
· 한 잡에서 예외가 나도 로그만 남기고 다음 잡을 계속 처리한다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import TYPE_CHECKING

from .basecollector import BaseCollector
from .oracleconnectionmanager import OracleConnectionManager
from .oraclejobs import ORACLE_JOBS, OracleJob

if TYPE_CHECKING:
    from .appconfig import AppConfig
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
        app_config: "AppConfig | None" = None,
    ) -> None:
        super().__init__(
            name="oracle",
            interval_sec=cfg.get("interval_sec", 60),
            store=store,
            logger=logger,
            on_collect=on_collect,
        )
        self._cfg: dict = cfg
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
        self._app_config = app_config
        if connection_manager is None and app_config is not None:
            self._connection_manager = OracleConnectionManager(
                logger,
                db_registry=app_config.db_registry,
            )
        else:
            self._connection_manager = connection_manager or OracleConnectionManager(logger)
        self._owns_connection_manager = connection_manager is None
        # alias 기반 연결만 허용한다.

    def _resolve_job_db_alias(self, job: OracleJob) -> str:
        return str(job.db).strip()

    # ── 수집 ────────────────────────────────────────────────────────
    def collect(self) -> list[tuple[str, list[dict], str]]:
        if self._test_mode:
            return self._collect_test_rows()

        results: list[tuple[str, list[dict], str]] = []

        for job in self._jobs:
            jid = self.logger.new_jid()
            with self.logger.job_context(jid=jid):
                job_db_alias = ""
                try:
                    job_db_alias = self._resolve_job_db_alias(job)
                    if not job_db_alias:
                        raise ValueError(
                            f"Oracle job.db alias is required for job={job.name}."
                        )
                    with self._connection_manager.cursor_by_alias(
                        job_db_alias,
                    ) as cursor:
                        started_at = time.perf_counter()
                        runtime_cfg = dict(self._cfg)
                        runtime_cfg["_last_ts"] = self._last_ts[job.name]
                        rows = job.query(runtime_cfg, cursor)
                    collected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
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
                except Exception as e:
                    self.logger.exception(
                        "[OracleCollector] job=%s db=%s table=%s query error=%s; continue",
                        job.name,
                        job_db_alias or "<default>",
                        job.table,
                        str(e),
                    )
                    continue

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
            jid = self.logger.new_jid()
            with self.logger.job_context(jid=jid):
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
