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
