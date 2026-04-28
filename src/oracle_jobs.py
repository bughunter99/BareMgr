#!/usr/bin/env python3
"""
oracle_jobs.py — OracleCollector가 실행할 수집 잡 코드 정의.

각 OracleJob 필드:
    name        : 잡 식별자 (로그·체크포인트에 사용)
    table       : SQLite에 저장할 테이블명
    sql         : 실행할 SELECT SQL
    db          : 실행할 DB alias (예: DB_MAIN, DB_TARGET). 비우면 collector 기본 DB 사용
    use_last_ts : True이면 :last_ts 바인드 변수를 쿼리에 전달한다
    test_rows   : 테스트 모드에서 생성할 더미 행 수
    post_process: 쿼리 결과 행(list[dict])을 후처리하는 함수
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable
from typing import TypedDict


class PostProcessContext(TypedDict):
    job_name: str
    table: str
    db_alias: str
    jid: str
    collected_at: str


PostProcessRowsOnlyFn = Callable[[list[dict]], list[dict]]
PostProcessWithContextFn = Callable[[list[dict], PostProcessContext], list[dict]]
PostProcessFn = PostProcessRowsOnlyFn | PostProcessWithContextFn


def identity_post_process(rows: list[dict]) -> list[dict]:
    return rows


@dataclass
class OracleJob:
    name: str
    table: str
    sql: str
    db: str = ""
    use_last_ts: bool = True
    test_rows: int = 5000
    post_process: PostProcessFn = identity_post_process
    post_processes: list[PostProcessFn] = field(default_factory=list)


class OracleJob1(OracleJob):
    SQL = """
        SELECT
            1 AS seq,
            'job1' AS label,
            TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS collected_at
        FROM dual
    """

    def __init__(self) -> None:
        super().__init__(
            name="job1",
            table="job1_results",
            sql=self.SQL,
            db="DB_MAIN",
            use_last_ts=False,
            test_rows=5000,
            post_process=self.post_process,
        )

    @staticmethod
    def post_process(rows: list[dict], context: PostProcessContext) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["job"] = context["job_name"]
            item["db"] = context["db_alias"]
            out.append(item)
        return out


class OracleJob2(OracleJob):
    SQL = """
        SELECT
            2 AS seq,
            'job2' AS label,
            'A|B|C' AS raw_tags
        FROM dual
    """

    def __init__(self) -> None:
        super().__init__(
            name="job2",
            table="job2_results",
            sql=self.SQL,
            db="DB_TARGET",
            use_last_ts=False,
            test_rows=3000,
            post_process=self.post_process,
        )

    @staticmethod
    def post_process(rows: list[dict], context: PostProcessContext) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            tags = str(item.get("raw_tags", "")).split("|")
            item["tags"] = [t for t in tags if t]
            item["jid"] = context["jid"]
            out.append(item)
        return out


class OracleJob3(OracleJob):
    SQL = """
        SELECT
            3 AS seq,
            'job3' AS label,
            '123' AS amount,
            'KRW' AS currency
        FROM dual
    """

    def __init__(self) -> None:
        super().__init__(
            name="job3",
            table="job3_results",
            sql=self.SQL,
            db="DB_MAIN",
            use_last_ts=False,
            test_rows=4000,
            post_process=self.post_process,
        )

    @staticmethod
    def post_process(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            try:
                item["amount"] = int(str(item.get("amount", "0")))
            except ValueError:
                item["amount"] = 0
            out.append(item)
        return out


class OracleJob4(OracleJob):
    SQL = """
        SELECT
            4 AS seq,
            'job4' AS label,
            '  mixed_case_text  ' AS raw_text
        FROM dual
    """

    def __init__(self) -> None:
        super().__init__(
            name="job4",
            table="job4_results",
            sql=self.SQL,
            db="DB_TARGET",
            use_last_ts=False,
            test_rows=2000,
            post_process=self.post_process,
        )

    @staticmethod
    def post_process(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["text"] = str(item.get("raw_text", "")).strip().lower()
            out.append(item)
        return out


class OracleJob5(OracleJob):
    SQL = """
        SELECT
            5 AS seq,
            'job5' AS label,
            TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF3') AS event_ts
        FROM dual
    """

    def __init__(self) -> None:
        super().__init__(
            name="job5",
            table="job5_results",
            sql=self.SQL,
            db="DB_MAIN",
            use_last_ts=False,
            test_rows=1000,
            post_process=self.post_process,
        )

    @staticmethod
    def post_process(rows: list[dict], context: PostProcessContext) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["processed_at"] = context["collected_at"]
            item["pipeline_key"] = f"{context['job_name']}:{context['table']}"
            out.append(item)
        return out


# ── 수집 잡 목록 ─────────────────────────────────────────────────────
# 각 클래스에 SQL과 전용 후처리 로직을 묶어 정의한다.

ORACLE_JOBS: list[OracleJob] = [
    OracleJob1(),
    OracleJob2(),
    OracleJob3(),
    OracleJob4(),
    OracleJob5(),
]
