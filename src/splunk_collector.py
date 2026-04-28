#!/usr/bin/env python3
"""
splunk_collector.py — Splunk 주기 수집기.

· splunklib(splunk-sdk) 패키지를 사용한다.
· 수집 잡은 splunk_jobs.py의 SPLUNK_JOBS에 코드로 정의한다.
"""

from __future__ import annotations

from datetime import datetime, timezone
import inspect
import time
from typing import TYPE_CHECKING

from .collector import BaseCollector
from .splunk_jobs import SPLUNK_JOBS, PostProcessContext, SplunkJob
from .splunk_search import SplunkSearch, configure_splunk_search

if TYPE_CHECKING:
    from .store import Store
    from .logger import Logger


class SplunkCollector(BaseCollector):
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
        self._cfg: dict = cfg
        self._host: str = str(cfg.get("host", "")).strip()
        self._port: int = cfg.get("port", 8089)
        self._username: str = str(cfg.get("username", "")).strip()
        self._password: str = str(cfg.get("password", "")).strip()
        self._scheme: str = cfg.get("scheme", "https")
        self._base_url: str = str(cfg.get("base_url", "")).strip()
        self._credential_key: str = str(cfg.get("credential_key", "")).strip()
        self._splunk_token: str = str(cfg.get("splunk_token", "")).strip()
        self._timeout_sec: float = float(cfg.get("timeout_sec", 60.0))
        self._jobs: list[SplunkJob] = list(SPLUNK_JOBS)
        self._test_cfg: dict = cfg.get("test", {})
        self._test_mode: bool = cfg.get("test_mode", False) or self._test_cfg.get("enabled", False)
        self._test_rows: int = int(self._test_cfg.get("rows", 10000))
        self._test_emit_once: bool = self._test_cfg.get("emit_once", False)
        self._test_emitted: bool = False
        self._test_batch_no: int = 0
        self._service = None
        self._use_splunk_search = bool(self._base_url and self._splunk_token)

        if self._use_splunk_search:
            configure_splunk_search(
                base_url=self._base_url,
                credential_key=self._credential_key,
                splunk_token=self._splunk_token,
                timeout_sec=self._timeout_sec,
            )

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

    def _apply_post_process(
        self,
        *,
        job: SplunkJob,
        rows: list[dict],
        jid: str,
        collected_at: str,
    ) -> list[dict]:
        context: PostProcessContext = {
            "job_name": job.name,
            "table": job.table,
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
    def _run_search(self, service, query: str, job_name: str) -> list[dict]:
        if self._use_splunk_search:
            return SplunkSearch(query)

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

    def collect(self) -> list[tuple[str, list[dict], str]]:
        if self._test_mode:
            return self._collect_test_rows()

        service = None if self._use_splunk_search else self._get_service()
        collected: list[tuple[str, list[dict], str]] = []

        for job in self._jobs:
            jid = self.logger.new_jid(prefix="SPL")

            with self.logger.job_context(jid=jid, prefix="SPL"):
                try:
                    started_at = time.perf_counter()
                    query = job.makeQuery(self._cfg)
                    rows = self._run_search(service, query, job.name)
                    collected_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    rows = self._apply_post_process(
                        job=job,
                        rows=rows,
                        jid=jid,
                        collected_at=collected_at,
                    )
                    elapsed = time.perf_counter() - started_at
                    if rows:
                        collected.append((job.table, rows, jid))
                    self.logger.info(
                        "[SplunkCollector] job=%s table=%s rows=%d elapsed=%.3fs",
                        job.name,
                        job.table,
                        len(rows),
                        elapsed,
                    )
                except Exception:
                    self.logger.exception("[SplunkCollector] job=%s error", job.name)
                    self._close_service()
                    raise

        return collected

    def _collect_test_rows(self) -> list[tuple[str, list[dict], str]]:
        if self._test_emit_once and self._test_emitted:
            self.logger.debug("[SplunkCollector:test] already emitted, skip")
            return []

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        batch_no = self._test_batch_no
        self._test_batch_no += 1

        results: list[tuple[str, list[dict], str]] = []
        for job in self._jobs:
            rows_count = job.test_rows
            jid = self.logger.new_jid(prefix="SPL")
            with self.logger.job_context(jid=jid, prefix="SPL"):
                started_at = time.perf_counter()
                rows = [
                    {
                        "event_id": f"{job.name}-batch{batch_no}-{i}",
                        "job": job.name,
                        "batch_no": str(batch_no),
                        "host": f"host-{i % 16}",
                        "message": f"synthetic splunk event {i}",
                        "severity": ["INFO", "WARN", "ERROR"][i % 3],
                        "updated_at": now,
                        "source": "splunk_test_mode",
                    }
                    for i in range(rows_count)
                ]
                results.append((job.table, rows, jid))
                elapsed = time.perf_counter() - started_at
                self.logger.info(
                    "[SplunkCollector:test] job=%s table=%s rows=%d batch=%d elapsed=%.3fs",
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
        self._close_service()
