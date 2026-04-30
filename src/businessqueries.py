#!/usr/bin/env python3
from __future__ import annotations

from typing import Any

from .oracle_utils import makeDictFactory


class OracleQuery:
    """Base class for domain-scoped Oracle query classes.

    Each subclass groups all SQL queries belonging to one business domain.
    Query methods receive an open cursor and return list[dict] via makeDictFactory.
    Instances are stateless except for config; the caller owns the connection lifecycle.
    """

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self._cfg = cfg or {}

    def _fetch(self, cursor: Any, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        cursor.execute(sql, params or {})
        cursor.rowfactory = makeDictFactory(cursor)
        return list(cursor.fetchall())


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
