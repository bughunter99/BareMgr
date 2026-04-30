#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oraclequery import OracleQuery


class QueryDDD(OracleQuery):
    """DDD domain queries."""

    def fetch_rules(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'DDD_RULES' AS source_name,
                   'PASS' AS rule_result
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})

    def fetch_violations(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'DDD_VIOLATIONS' AS source_name,
                   0 AS violation_count
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})
