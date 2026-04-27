#!/usr/bin/env python3
"""
splunk_collector.py — Splunk 주기 수집기.

· config["collectors"]["splunk"] 기준으로 동작한다.
· splunklib(splunk-sdk) 패키지를 사용한다.
· jobs 마다 SPL 쿼리를 실행해 결과를 SQLite에 저장한다.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from collector import BaseCollector

if TYPE_CHECKING:
    from store import Store
    from logger import Logger


class SplunkCollector(BaseCollector):
    """
    config 예시:
        "splunk": {
            "enabled": true,
            "host": "splunk.internal",
            "port": 8089,
            "username": "admin",
            "password": "changeme",
            "scheme": "https",
            "interval_sec": 120,
            "jobs": [
                {
                    "name": "error_events",
                    "query": "search index=main level=ERROR earliest=-2m latest=now | table _time, host, message",
                    "table": "splunk_errors"
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
            name="splunk",
            interval_sec=cfg.get("interval_sec", 120),
            store=store,
            logger=logger,
            on_collect=on_collect,
        )
        self._host: str = cfg["host"]
        self._port: int = cfg.get("port", 8089)
        self._username: str = cfg["username"]
        self._password: str = cfg["password"]
        self._scheme: str = cfg.get("scheme", "https")
        self._jobs: list[dict] = cfg.get("jobs", [])
        self._service = None

    # ── 연결 관리 ────────────────────────────────────────────────────
    def _get_service(self):
        import splunklib.client as client  # type: ignore[import]

        if self._service is None:
            self._service = client.connect(
                host=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                scheme=self._scheme,
            )
        return self._service

    def _close_service(self) -> None:
        self._service = None

    # ── 수집 ────────────────────────────────────────────────────────
    def _run_search(self, service, query: str, job_name: str) -> list[dict]:
        import splunklib.results as results  # type: ignore[import]

        kwargs = {
            "exec_mode": "blocking",
            "output_mode": "json",
        }
        job = service.jobs.create(f"search {query}", **kwargs)

        # blocking이지만 안전하게 완료 대기
        max_wait = 60
        waited = 0
        while not job.is_done():
            time.sleep(1)
            waited += 1
            if waited >= max_wait:
                self.logger.warning(
                    "[SplunkCollector] job=%s search timeout, cancelling", job_name
                )
                job.cancel()
                return []

        rows: list[dict] = []
        for result in results.JSONResultsReader(job.results(output_mode="json")):
            if isinstance(result, dict):
                rows.append(result)
        job.cancel()
        return rows

    def collect(self) -> list[tuple[str, list[dict]]]:
        service = self._get_service()
        collected: list[tuple[str, list[dict]]] = []

        for job in self._jobs:
            name: str = job["name"]
            query: str = job["query"]
            table: str = job["table"]

            try:
                rows = self._run_search(service, query, name)
                if rows:
                    collected.append((table, rows))
                    self.logger.info(
                        "[SplunkCollector] job=%s fetched %d rows", name, len(rows)
                    )
                else:
                    self.logger.debug("[SplunkCollector] job=%s no results", name)
            except Exception:
                self.logger.exception("[SplunkCollector] job=%s error", name)
                self._close_service()  # 재연결 유도
                raise

        return collected

    def stop(self) -> None:
        super().stop()
        self._close_service()
