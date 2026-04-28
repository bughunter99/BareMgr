#!/usr/bin/env python3
"""
splunk_jobs.py — SplunkCollector가 실행할 수집 잡 코드 정의.

각 SplunkJob 필드:
  name       : 잡 식별자 (로그·체크포인트에 사용)
  table      : SQLite에 저장할 테이블명
  query      : 실행할 SPL(Search Processing Language) 쿼리
  test_rows  : 테스트 모드에서 생성할 더미 행 수
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SplunkJob:
    name: str
    table: str
    query: str
    test_rows: int = 10000


# ── 수집 잡 목록 ─────────────────────────────────────────────────────
# query를 실제 SPL 쿼리로 교체하면 된다.
# table은 SQLite에 저장될 테이블명이다.

SPLUNK_JOBS: list[SplunkJob] = [
    SplunkJob(
        name="job1",
        table="splunk_job1",
        query="search index=main | head 1 | eval label='job1' | table _time, host, label",
        test_rows=10000,
    ),
    SplunkJob(
        name="job2",
        table="splunk_job2",
        query="search index=main | head 2 | eval label='job2' | table _time, host, label",
        test_rows=8000,
    ),
    SplunkJob(
        name="job3",
        table="splunk_job3",
        query="search index=main | head 3 | eval label='job3' | table _time, host, label",
        test_rows=6000,
    ),
    SplunkJob(
        name="job4",
        table="splunk_job4",
        query="search index=main | head 4 | eval label='job4' | table _time, host, label",
        test_rows=4000,
    ),
    SplunkJob(
        name="job5",
        table="splunk_job5",
        query="search index=main | head 5 | eval label='job5' | table _time, host, label",
        test_rows=2000,
    ),
]
