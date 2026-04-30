#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oraclequery import OracleQuery


class QueryCCC(OracleQuery):
    """CCC domain queries."""

    def fetch_metrics(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'CCC_METRICS' AS source_name,
                   1 AS metric_count
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})

    def fetch_aggregates(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'CCC_AGGS' AS source_name,
                   0 AS total
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})
