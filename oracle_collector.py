#!/usr/bin/env python3
"""
oracle_collector.py — Oracle DB 주기 수집기.

· config["collectors"]["oracle"] 기준으로 동작한다.
· jobs 마다 SQL의 :last_ts 바인드 변수에 마지막 수집 timestamp를 넘긴다.
· cx_Oracle 패키지가 없으면 ImportError를 발생시킨다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import TYPE_CHECKING

from collector import BaseCollector

if TYPE_CHECKING:
    from store import Store
    from logger import Logger


class OracleCollector(BaseCollector):
    """
    config 예시:
        "oracle": {
            "enabled": true,
            "dsn": "user/password@host:port/service",
            "interval_sec": 60,
            "jobs": [
                {
                    "name": "inventory",
                    "sql": "SELECT id, name, qty, updated_at FROM inventory WHERE updated_at > :last_ts",
                    "table": "inventory"
                }
            ]
        }
    """

    def __init__(
        self,
        cfg: dict,
        store: "Store",
        logger: "Logger",
        on_collect=None,
    ) -> None:
        super().__init__(
            name="oracle",
            interval_sec=cfg.get("interval_sec", 60),
            store=store,
            logger=logger,
            on_collect=on_collect,
        )
        self._dsn: str = cfg["dsn"]
        self._jobs: list[dict] = cfg.get("jobs", [])
        self._test_cfg: dict = cfg.get("test", {})
        self._test_mode: bool = cfg.get("test_mode", False) or self._test_cfg.get("enabled", False)
        self._test_rows: int = int(self._test_cfg.get("rows", 5000))
        self._test_emit_once: bool = self._test_cfg.get("emit_once", True)
        self._test_emitted: bool = False
        self._test_batch_no: int = 0
        # 잡별 마지막 수집 시각 (UTC ISO8601)
        self._last_ts: dict[str, str] = {
            job["name"]: "1970-01-01 00:00:00" for job in self._jobs
        }
        self._conn = None

    # ── 연결 관리 ────────────────────────────────────────────────────
    def _get_conn(self):
        import cx_Oracle  # type: ignore[import]

        if self._conn is None:
            self._conn = cx_Oracle.connect(self._dsn)
        return self._conn

    def _close_conn(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── 수집 ────────────────────────────────────────────────────────
    def collect(self) -> list[tuple[str, list[dict]]]:
        if self._test_mode:
            return self._collect_test_rows()

        results: list[tuple[str, list[dict]]] = []
        conn = self._get_conn()
        cursor = conn.cursor()

        for job in self._jobs:
            name: str = job["name"]
            sql: str = job["sql"]
            table: str = job["table"]
            last_ts: str = self._last_ts[name]

            try:
                started_at = time.perf_counter()
                cursor.execute(sql, {"last_ts": last_ts})
                cols = [d[0].lower() for d in cursor.description]
                rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
                elapsed = time.perf_counter() - started_at
                if rows:
                    results.append((table, rows))
                    # 수집 시각 갱신
                    self._last_ts[name] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    self.logger.info(
                        "[OracleCollector] ACTIVE selected job=%s table=%s rows=%d elapsed=%.3fs",
                        name,
                        table,
                        len(rows),
                        elapsed,
                    )
                else:
                    self.logger.info(
                        "[OracleCollector] ACTIVE selected job=%s table=%s rows=0 elapsed=%.3fs",
                        name,
                        table,
                        elapsed,
                    )
            except Exception:
                self.logger.exception("[OracleCollector] job=%s query error", name)
                self._close_conn()  # 재연결 유도
                raise

        cursor.close()
        return results

    def _collect_test_rows(self) -> list[tuple[str, list[dict]]]:
        """Oracle 없이 테스트할 때 임의 데이터(기본 5000건)를 생성한다."""
        if self._test_emit_once and self._test_emitted:
            self.logger.debug("[OracleCollector:test] already emitted, skip")
            return []

        jobs = self._jobs or [{"name": "oracle_test", "table": "oracle_test_data"}]
        rows_per_job = max(1, self._test_rows)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        batch_no = self._test_batch_no
        self._test_batch_no += 1

        results: list[tuple[str, list[dict]]] = []
        for idx, job in enumerate(jobs):
            table = job.get("table", job.get("name", f"oracle_test_{idx}"))
            name = job.get("name", f"job_{idx}")
            rows_per_job = max(1, int(job.get("test_rows", self._test_rows)))
            started_at = time.perf_counter()
            rows = []
            for i in range(rows_per_job):
                rows.append(
                    {
                        "id": f"{name}-batch{batch_no}-{i}",
                        "job": name,
                        "batch_no": str(batch_no),
                        "payload": f"dummy-payload-{i}",
                        "qty": (i * 7) % 1000,
                        "updated_at": now,
                        "source": "oracle_test_mode",
                    }
                )
            results.append((table, rows))
            elapsed = time.perf_counter() - started_at
            self.logger.info(
                "[OracleCollector:test] ACTIVE selected job=%s table=%s rows=%d batch=%d elapsed=%.3fs",
                name,
                table,
                len(rows),
                batch_no,
                elapsed,
            )

        self._test_emitted = True
        return results

    def stop(self) -> None:
        super().stop()
        self._close_conn()
