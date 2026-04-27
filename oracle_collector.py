#!/usr/bin/env python3
"""
oracle_collector.py — Oracle DB 주기 수집기.

· config["collectors"]["oracle"] 기준으로 동작한다.
· jobs 마다 SQL의 :last_ts 바인드 변수에 마지막 수집 timestamp를 넘긴다.
· cx_Oracle 패키지가 없으면 ImportError를 발생시킨다.
"""

from __future__ import annotations

from datetime import datetime, timezone
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
        results: list[tuple[str, list[dict]]] = []
        conn = self._get_conn()
        cursor = conn.cursor()

        for job in self._jobs:
            name: str = job["name"]
            sql: str = job["sql"]
            table: str = job["table"]
            last_ts: str = self._last_ts[name]

            try:
                cursor.execute(sql, {"last_ts": last_ts})
                cols = [d[0].lower() for d in cursor.description]
                rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
                if rows:
                    results.append((table, rows))
                    # 수집 시각 갱신
                    self._last_ts[name] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    self.logger.info(
                        "[OracleCollector] job=%s fetched %d rows", name, len(rows)
                    )
                else:
                    self.logger.debug("[OracleCollector] job=%s no new rows", name)
            except Exception:
                self.logger.exception("[OracleCollector] job=%s query error", name)
                self._close_conn()  # 재연결 유도
                raise

        cursor.close()
        return results

    def stop(self) -> None:
        super().stop()
        self._close_conn()
