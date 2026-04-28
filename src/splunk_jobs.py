#!/usr/bin/env python3
"""
splunk_jobs.py — SplunkCollector가 실행할 수집 잡 코드 정의.

각 SplunkJob 필드:
    name      : 잡 식별자 (로그·체크포인트에 사용)
    table     : SQLite에 저장할 테이블명
    test_rows : 테스트 모드에서 생성할 더미 행 수

각 잡은 query(config, service, run_search) 함수를 구현해
SPL 조립 + 검색 실행 + 결과 가공(list[dict]) 반환을 직접 처리한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SplunkJob:
    name: str
    table: str
    test_rows: int = 10000

    def query(self, cfg: dict[str, Any], service, run_search) -> list[dict]:
        raise NotImplementedError


class SplunkJob1(SplunkJob):
    _QUERY = "search index=main | head 1 | eval label='job1' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job1",
            table="splunk_job1",
            test_rows=10000,
        )

    def query(self, cfg: dict[str, Any], service, run_search) -> list[dict]:
        suffix = str((cfg.get("query_filters", {}) or {}).get("job1", "")).strip()
        spl = f"{self._QUERY} {suffix}".strip()
        rows = run_search(service, spl, self.name)
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["job"] = self.name
            out.append(item)
        return out


class SplunkJob2(SplunkJob):
    _QUERY = "search index=main | head 2 | eval label='job2' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job2",
            table="splunk_job2",
            test_rows=8000,
        )

    def query(self, cfg: dict[str, Any], service, run_search) -> list[dict]:
        source_filter = str((cfg.get("query_filters", {}) or {}).get("job2", "")).strip()
        spl = f"{self._QUERY} {source_filter}".strip()
        rows = run_search(service, spl, self.name)
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["severity"] = str(item.get("severity", "INFO")).upper()
            out.append(item)
        return out


class SplunkJob3(SplunkJob):
    _QUERY = "search index=main | head 3 | eval label='job3' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job3",
            table="splunk_job3",
            test_rows=6000,
        )

    def query(self, cfg: dict[str, Any], service, run_search) -> list[dict]:
        keyword = str((cfg.get("query_params", {}) or {}).get("job3_keyword", "")).strip()
        spl = self._QUERY
        if keyword:
            spl = f"{spl} | search {keyword}"
        rows = run_search(service, spl, self.name)
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["pipeline_key"] = f"{self.name}:{self.table}"
            out.append(item)
        return out


class SplunkJob4(SplunkJob):
    _QUERY = "search index=main | head 4 | eval label='job4' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job4",
            table="splunk_job4",
            test_rows=4000,
        )

    def query(self, cfg: dict[str, Any], service, run_search) -> list[dict]:
        host = str((cfg.get("query_params", {}) or {}).get("job4_host", "")).strip()
        spl = self._QUERY
        if host:
            spl = f"{spl} | search host={host}"
        rows = run_search(service, spl, self.name)
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["host"] = str(item.get("host", "")).lower()
            out.append(item)
        return out


class SplunkJob5(SplunkJob):
    _QUERY = "search index=main | head 5 | eval label='job5' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job5",
            table="splunk_job5",
            test_rows=2000,
        )

    def query(self, cfg: dict[str, Any], service, run_search) -> list[dict]:
        limit = int((cfg.get("query_params", {}) or {}).get("job5_head", 5))
        spl = f"search index=main | head {max(1, limit)} | eval label='job5' | table _time, host, label"
        rows = run_search(service, spl, self.name)
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["processed_by"] = self.name
            out.append(item)
        return out


# ── 수집 잡 목록 ─────────────────────────────────────────────────────
# 각 클래스에 SPL 실행과 결과 가공 로직을 묶어 정의한다.

SPLUNK_JOBS: list[SplunkJob] = [
    SplunkJob1(),
    SplunkJob2(),
    SplunkJob3(),
    SplunkJob4(),
    SplunkJob5(),
]
