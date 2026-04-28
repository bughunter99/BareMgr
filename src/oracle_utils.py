#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oracle_driver import get_cx_oracle


ORACLE_CONNECTION_CHECK_SQL = "SELECT sysdate FROM dual"


def makeDictFactory(cursor: Any):
    column_names = [desc[0].lower() for desc in cursor.description]

    def _factory(*args: Any) -> dict[str, Any]:
        return dict(zip(column_names, args))

    return _factory


def validate_oracle_connection(
    conn: Any,
    *,
    sql: str = ORACLE_CONNECTION_CHECK_SQL,
) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        cursor.fetchone()
    finally:
        cursor.close()


def get_oracle_error_type():
    cx_Oracle = get_cx_oracle()
    return cx_Oracle.DatabaseError