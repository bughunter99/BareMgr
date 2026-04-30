#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oraclequery import OracleQuery


class QueryEEE(OracleQuery):
    """EEE domain queries."""

    def fetch_events(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'EEE_EVENTS' AS source_name,
                   'N' AS has_alert
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})

    def fetch_history(self, cursor: Any, obj_id: str) -> list[dict[str, Any]]:
        sql = """
            SELECT :obj_id AS obj_id,
                   'EEE_HISTORY' AS source_name,
                   0 AS event_count
            FROM dual
        """
        return self._fetch(cursor, sql, {"obj_id": obj_id})
