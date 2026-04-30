#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oraclequery import OracleQuery


class QueryBBB(OracleQuery):
    """BBB domain queries."""

    def fetch_status(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'BBB_STATUS' AS source_name,
                   'OK' AS status_code
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})

    def fetch_flags(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'BBB_FLAGS' AS source_name,
                   0 AS flag_count
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})
