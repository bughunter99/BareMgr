#!/usr/bin/env python3
from __future__ import annotations

import threading
from typing import Any

from .oracle_driver import get_cx_oracle
from .oracle_utils import validate_oracle_connection


class OracleConnectionManager:
    def __init__(self, logger=None) -> None:
        self._logger = logger
        self._lock = threading.Lock()
        self._connections: dict[tuple[str, bool], Any] = {}

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

    def close_all(self) -> None:
        with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()

        for conn in connections:
            try:
                conn.close()
            except Exception:
                pass