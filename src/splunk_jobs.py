#!/usr/bin/env python3
"""
splunk_jobs.py — SplunkCollector가 실행할 수집 잡 코드 정의.

각 SplunkJob 필드:
    name          : 잡 식별자 (로그·체크포인트에 사용)
    table         : SQLite에 저장할 테이블명
    query         : 기본 SPL(Search Processing Language) 쿼리
    test_rows     : 테스트 모드에서 생성할 더미 행 수
    post_process  : 쿼리 결과 행(list[dict])을 후처리하는 함수
    post_processes: 여러 후처리 함수 체인

각 잡은 makeQuery(config)를 오버라이드해서 설정 기반 동적 SPL을 생성할 수 있다.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from typing import Callable
from typing import TypedDict


class PostProcessContext(TypedDict):
        job_name: str
        table: str
        jid: str
        collected_at: str


PostProcessRowsOnlyFn = Callable[[list[dict]], list[dict]]
PostProcessWithContextFn = Callable[[list[dict], PostProcessContext], list[dict]]
PostProcessFn = PostProcessRowsOnlyFn | PostProcessWithContextFn


def identity_post_process(rows: list[dict]) -> list[dict]:
        return rows


@dataclass
class SplunkJob:
    name: str
    table: str
    query: str
    test_rows: int = 10000
    post_process: PostProcessFn = identity_post_process
    post_processes: list[PostProcessFn] = field(default_factory=list)

    def makeQuery(self, cfg: dict[str, Any]) -> str:
        return self.query


class SplunkJob1(SplunkJob):
    QUERY = "search index=main | head 1 | eval label='job1' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job1",
            table="splunk_job1",
            query=self.QUERY,
            test_rows=10000,
            post_process=self.post_process,
        )

    def makeQuery(self, cfg: dict[str, Any]) -> str:
        suffix = str((cfg.get("query_filters", {}) or {}).get("job1", "")).strip()
        return f"{self.query} {suffix}".strip()

    @staticmethod
    def post_process(rows: list[dict], context: PostProcessContext) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["job"] = context["job_name"]
            item["jid"] = context["jid"]
            out.append(item)
        return out


class SplunkJob2(SplunkJob):
    QUERY = "search index=main | head 2 | eval label='job2' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job2",
            table="splunk_job2",
            query=self.QUERY,
            test_rows=8000,
            post_process=self.post_process,
        )

    def makeQuery(self, cfg: dict[str, Any]) -> str:
        source_filter = str((cfg.get("query_filters", {}) or {}).get("job2", "")).strip()
        return f"{self.query} {source_filter}".strip()

    @staticmethod
    def post_process(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["severity"] = str(item.get("severity", "INFO")).upper()
            out.append(item)
        return out


class SplunkJob3(SplunkJob):
    QUERY = "search index=main | head 3 | eval label='job3' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job3",
            table="splunk_job3",
            query=self.QUERY,
            test_rows=6000,
            post_process=self.post_process,
        )

    def makeQuery(self, cfg: dict[str, Any]) -> str:
        keyword = str((cfg.get("query_params", {}) or {}).get("job3_keyword", "")).strip()
        if not keyword:
            return self.query
        return f"{self.query} | search {keyword}"

    @staticmethod
    def post_process(rows: list[dict], context: PostProcessContext) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["pipeline_key"] = f"{context['job_name']}:{context['table']}"
            out.append(item)
        return out


class SplunkJob4(SplunkJob):
    QUERY = "search index=main | head 4 | eval label='job4' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job4",
            table="splunk_job4",
            query=self.QUERY,
            test_rows=4000,
            post_process=self.post_process,
        )

    def makeQuery(self, cfg: dict[str, Any]) -> str:
        host = str((cfg.get("query_params", {}) or {}).get("job4_host", "")).strip()
        if not host:
            return self.query
        return f"{self.query} | search host={host}"

    @staticmethod
    def post_process(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["host"] = str(item.get("host", "")).lower()
            out.append(item)
        return out


class SplunkJob5(SplunkJob):
    QUERY = "search index=main | head 5 | eval label='job5' | table _time, host, label"

    def __init__(self) -> None:
        super().__init__(
            name="job5",
            table="splunk_job5",
            query=self.QUERY,
            test_rows=2000,
            post_process=self.post_process,
        )

    def makeQuery(self, cfg: dict[str, Any]) -> str:
        limit = int((cfg.get("query_params", {}) or {}).get("job5_head", 5))
        return f"search index=main | head {max(1, limit)} | eval label='job5' | table _time, host, label"

    @staticmethod
    def post_process(rows: list[dict], context: PostProcessContext) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["processed_at"] = context["collected_at"]
            out.append(item)
        return out


# ── 수집 잡 목록 ─────────────────────────────────────────────────────
# 각 클래스에 SPL과 전용 후처리 로직을 묶어 정의한다.

SPLUNK_JOBS: list[SplunkJob] = [
    SplunkJob1(),
    SplunkJob2(),
    SplunkJob3(),
    SplunkJob4(),
    SplunkJob5(),
]
