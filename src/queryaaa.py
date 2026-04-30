#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oraclequery import OracleQuery


class QueryAAA(OracleQuery):
    """AAA domain queries."""

    def fetch_profile(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'AAA_PROFILE' AS source_name,
                   TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') AS fetched_at
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})

    def fetch_detail(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'AAA_DETAIL' AS source_name,
                   NULL AS extra_info
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})
