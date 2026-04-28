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


# ── 수집 잡 목록 ─────────────────────────────────────────────────────
# sql을 실제 쿼리로 교체하면 된다.
# db에는 실행할 DB alias를 넣을 수 있다. (예: DB_MAIN, DB_TARGET)
# use_last_ts=True 인 잡은 WHERE 조건에 :last_ts 바인드 변수를 사용한다.
# use_last_ts=False 인 잡은 항상 전체를 조회한다.
# post_process는 아래 두 형태를 모두 지원한다.
#   - post_process(rows)
#   - post_process(rows, context)
# post_processes에 여러 함수를 넣으면 순차 적용된다.

ORACLE_JOBS: list[OracleJob] = [
    OracleJob(
        name="job1",
        table="job1_results",
        sql="SELECT 1 AS seq, 'job1' AS label FROM dual",
        db="DB_MAIN",
        use_last_ts=False,
        test_rows=5000,
    ),
    OracleJob(
        name="job2",
        table="job2_results",
        sql="SELECT 2 AS seq, 'job2' AS label FROM dual",
        db="DB_TARGET",
        use_last_ts=False,
        test_rows=3000,
    ),
    OracleJob(
        name="job3",
        table="job3_results",
        sql="SELECT 3 AS seq, 'job3' AS label FROM dual",
        db="DB_MAIN",
        use_last_ts=False,
        test_rows=4000,
    ),
    OracleJob(
        name="job4",
        table="job4_results",
        sql="SELECT 4 AS seq, 'job4' AS label FROM dual",
        db="DB_TARGET",
        use_last_ts=False,
        test_rows=2000,
    ),
    OracleJob(
        name="job5",
        table="job5_results",
        sql="SELECT 5 AS seq, 'job5' AS label FROM dual",
        db="DB_MAIN",
        use_last_ts=False,
        test_rows=1000,
    ),
]
