#!/usr/bin/env python3
from __future__ import annotations

import threading
from typing import Any

from .oracle_driver import get_cx_oracle
from .oracle_utils import makeDictFactory, validate_oracle_connection


class OracleConnectionManager:
    def __init__(self, logger=None) -> None:
        self._logger = logger
        self._lock = threading.Lock()
        self._connections: dict[tuple[str, bool], Any] = {}
        self._session_pools: dict[tuple[str, str, str, int, int, int, bool, str], Any] = {}

    def get_connection(self, dsn: str, *, threaded: bool = True):
        normalized_dsn = str(dsn).strip()
        if not normalized_dsn:
            raise ValueError("Oracle DSN is required")

        key = (normalized_dsn, bool(threaded))
        with self._lock:
            conn = self._connections.get(key)
            if conn is not None:
                return conn

            cx_Oracle = get_cx_oracle()
            conn = cx_Oracle.connect(normalized_dsn, threaded=threaded)
            validate_oracle_connection(conn)
            self._connections[key] = conn
            if self._logger is not None:
                self._logger.info("[OracleConnectionManager] opened dsn=%s threaded=%s", normalized_dsn, threaded)
            return conn

    def invalidate(self, dsn: str, *, threaded: bool = True) -> None:
        normalized_dsn = str(dsn).strip()
        key = (normalized_dsn, bool(threaded))
        with self._lock:
            conn = self._connections.pop(key, None)
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass

    def _normalize_pool_cfg(self, cfg: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "enabled": bool(cfg.get("enabled", False)),
            "user": str(cfg.get("user", "")).strip(),
            "password": str(cfg.get("password", "")).strip(),
            "dsn": str(cfg.get("dsn", "")).strip(),
            "min": max(1, int(cfg.get("min", 1))),
            "max": max(1, int(cfg.get("max", 10))),
            "increment": max(1, int(cfg.get("increment", 1))),
            "threaded": bool(cfg.get("threaded", True)),
            "getmode": str(cfg.get("getmode", "wait")).strip().lower(),
        }
        normalized["max"] = max(normalized["min"], normalized["max"])
        return normalized

    def _pool_key(self, normalized_cfg: dict[str, Any]) -> tuple[str, str, str, int, int, int, bool, str]:
        return (
            normalized_cfg["user"],
            normalized_cfg["password"],
            normalized_cfg["dsn"],
            int(normalized_cfg["min"]),
            int(normalized_cfg["max"]),
            int(normalized_cfg["increment"]),
            bool(normalized_cfg["threaded"]),
            str(normalized_cfg["getmode"]),
        )

    def get_session_pool(self, cfg: dict[str, Any]):
        normalized = self._normalize_pool_cfg(cfg)
        if not normalized["enabled"]:
            return None

        if not normalized["user"] or not normalized["password"] or not normalized["dsn"]:
            raise ValueError("oracle_pool enabled but user/password/dsn not fully configured")

        cx_Oracle = get_cx_oracle()
        key = self._pool_key(normalized)

        with self._lock:
            existing = self._session_pools.get(key)
            if existing is not None:
                return existing

            getmode = cx_Oracle.SPOOL_ATTRVAL_WAIT
            if normalized["getmode"] == "nowait":
                getmode = cx_Oracle.SPOOL_ATTRVAL_NOWAIT

            pool = cx_Oracle.SessionPool(
                user=normalized["user"],
                password=normalized["password"],
                dsn=normalized["dsn"],
                min=int(normalized["min"]),
                max=int(normalized["max"]),
                increment=int(normalized["increment"]),
                threaded=bool(normalized["threaded"]),
                getmode=getmode,
            )
            self._session_pools[key] = pool
            if self._logger is not None:
                self._logger.info(
                    "[OracleConnectionManager] session pool created dsn=%s min=%d max=%d increment=%d",
                    normalized["dsn"],
                    normalized["min"],
                    normalized["max"],
                    normalized["increment"],
                )
            return pool

    def fetch_many_from_pool(
        self,
        cfg: dict[str, Any],
        *,
        sql: str,
        params: dict[str, Any] | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not sql:
            return []

        pool = self.get_session_pool(cfg)
        if pool is None:
            return []

        conn = pool.acquire()
        cursor = conn.cursor()
        try:
            validate_oracle_connection(conn)
            cursor.execute(sql, params or {})
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchmany(max(1, int(limit))))
        finally:
            cursor.close()
            pool.release(conn)

    def close_all(self) -> None:
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
            pools = list(self._session_pools.values())
            self._session_pools.clear()

        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass

        for pool in pools:
            try:
                pool.close()
            except Exception:
                pass