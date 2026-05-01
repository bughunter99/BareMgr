#!/usr/bin/env python3
"""
oraclejobs.py — OracleCollector가 실행할 수집 잡 코드 정의.

각 OracleJob 필드:
    name      : 잡 식별자 (로그·체크포인트에 사용)
    table     : SQLite에 저장할 테이블명
    db        : 실행할 DB alias (예: DB_MAIN, DB_TARGET). 필수
    test_rows : 테스트 모드에서 생성할 더미 행 수

각 잡은 query(config, cursor) 함수를 구현해 직접 SQL 실행 + 파라미터 바인딩 +
결과(list[dict]) 반환을 담당한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .oracle_utils import makeDictFactory


@dataclass
class OracleJob:
    name: str
    table: str
    db: str = ""
    test_rows: int = 5000

    def query(self, cfg: dict[str, Any], cursor) -> list[dict]:
        raise NotImplementedError


class OracleJob1(OracleJob):
    _SQL = """
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
            db="DB_MAIN",
            test_rows=5000,
        )

    def query(self, cfg: dict[str, Any], cursor) -> list[dict]:
        sql = self._SQL
        cond = str((cfg.get("query_filters", {}) or {}).get("job1", "")).strip()
        if cond:
            sql = f"SELECT * FROM ({self._SQL}) WHERE {cond}"
        cursor.execute(sql)
        cursor.rowfactory = makeDictFactory(cursor)
        return list(cursor.fetchall())


class OracleJob2(OracleJob):
    _SQL = """
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
            db="DB_TARGET",
            test_rows=3000,
        )

    def query(self, cfg: dict[str, Any], cursor) -> list[dict]:
        sql = self._SQL
        cond = str((cfg.get("query_filters", {}) or {}).get("job2", "")).strip()
        if cond:
            sql = f"SELECT * FROM ({self._SQL}) WHERE {cond}"
        cursor.execute(sql)
        cursor.rowfactory = makeDictFactory(cursor)
        rows = list(cursor.fetchall())
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            tags = str(item.get("raw_tags", "")).split("|")
            item["tags"] = [t for t in tags if t]
            out.append(item)
        return out


class OracleJob3(OracleJob):
    _SQL = """
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
            db="DB_MAIN",
            test_rows=4000,
        )

    def query(self, cfg: dict[str, Any], cursor) -> list[dict]:
        sql = self._SQL
        cond = str((cfg.get("query_filters", {}) or {}).get("job3", "")).strip()
        if cond:
            sql = f"SELECT * FROM ({self._SQL}) WHERE {cond}"
        cursor.execute(sql)
        cursor.rowfactory = makeDictFactory(cursor)
        rows = list(cursor.fetchall())
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
    _SQL = """
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
            db="DB_TARGET",
            test_rows=2000,
        )

    def query(self, cfg: dict[str, Any], cursor) -> list[dict]:
        sql = self._SQL
        cond = str((cfg.get("query_filters", {}) or {}).get("job4", "")).strip()
        if cond:
            sql = f"SELECT * FROM ({self._SQL}) WHERE {cond}"
        cursor.execute(sql)
        cursor.rowfactory = makeDictFactory(cursor)
        rows = list(cursor.fetchall())
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["text"] = str(item.get("raw_text", "")).strip().lower()
            out.append(item)
        return out


class OracleJob5(OracleJob):
    _SQL = """
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
            db="DB_MAIN",
            test_rows=1000,
        )

    def query(self, cfg: dict[str, Any], cursor) -> list[dict]:
        sql = self._SQL
        cond = str((cfg.get("query_filters", {}) or {}).get("job5", "")).strip()
        if cond:
            sql = f"SELECT * FROM ({self._SQL}) WHERE {cond}"
        cursor.execute(sql)
        cursor.rowfactory = makeDictFactory(cursor)
        rows = list(cursor.fetchall())
        out: list[dict] = []
        for row in rows:
            item = dict(row)
            item["pipeline_key"] = f"{self.name}:{self.table}"
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
