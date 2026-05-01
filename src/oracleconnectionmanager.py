#!/usr/bin/env python3
from __future__ import annotations

from contextlib import contextmanager
import time
import threading
from typing import Any

from .db_registry import resolve_dsn, resolve_pool_cfg
from .oracle_driver import get_cx_oracle
from .oracle_utils import makeDictFactory, validate_oracle_connection


class OracleConnectionManager:
    def __init__(
        self,
        logger=None,
        *,
        cursor_acquire_timeout_sec: float = 30.0,
        db_registry: dict[str, dict] | None = None,
    ) -> None:
        self._logger = logger
        self._lock = threading.Lock()
        self._connections: dict[tuple[str, bool], Any] = {}
        self._session_pools: dict[tuple[str, str, str, int, int, int, bool, str], Any] = {}
        self._db_registry: dict[str, dict] = dict(db_registry or {})
        self._cursor_acquire_timeout_sec = (
            float(cursor_acquire_timeout_sec) if float(cursor_acquire_timeout_sec) > 0 else 30.0
        )

    def set_db_registry(self, db_registry: dict[str, dict] | None) -> None:
        self._db_registry = dict(db_registry or {})

    def _coerce_timeout(self, value: Any, default: float) -> float:
        try:
            timeout_value = float(value)
        except Exception:
            return default
        return timeout_value if timeout_value > 0 else default

    def _resolve_cursor_acquire_timeout(self, alias: str, timeout: float | None) -> float:
        if timeout is not None:
            return self._coerce_timeout(timeout, self._cursor_acquire_timeout_sec)

        entry = self._db_registry.get(alias, {}) or {}
        if isinstance(entry, dict):
            per_alias_timeout = entry.get("cursor_acquire_timeout_sec", None)
            if per_alias_timeout is not None:
                return self._coerce_timeout(per_alias_timeout, self._cursor_acquire_timeout_sec)

            pool_cfg = entry.get("pool", {}) or {}
            if isinstance(pool_cfg, dict):
                pool_timeout = pool_cfg.get("cursor_acquire_timeout_sec", None)
                if pool_timeout is not None:
                    return self._coerce_timeout(pool_timeout, self._cursor_acquire_timeout_sec)

        return self._cursor_acquire_timeout_sec

    def get_connection(self, dsn: str, *, threaded: bool = True) -> Any:
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

    def get_session_pool(self, cfg: dict[str, Any]) -> Any | None:
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

    @contextmanager
    def cursor_by_alias(
        self,
        db_alias: str,
        timeout: float | None = None,
    ):
        """db alias 기준으로 cursor를 열고 자동 정리한다.

        - alias의 pool.enabled=true면 session pool에서 acquire (timeout 적용)
        - 아니면 alias로 resolve_dsn 후 shared connection에서 cursor 반환
        """
        alias = str(db_alias or "").strip()
        if not alias:
            raise ValueError("db alias is required")

        wait = self._resolve_cursor_acquire_timeout(alias, timeout)

        pool_cfg = resolve_pool_cfg(self._db_registry, alias)
        pool_enabled = bool(pool_cfg.get("enabled", False))

        pool = None
        conn = None
        cursor = None
        try:
            if pool_enabled:
                pool = self.get_session_pool(pool_cfg)
                if pool is None:
                    raise ValueError(
                        f"oracle session pool is required alias={alias}"
                    )
                deadline = time.monotonic() + wait
                while conn is None:
                    try:
                        conn = pool.acquire()
                    except Exception as e:
                        if time.monotonic() >= deadline:
                            raise TimeoutError(
                                f"oracle cursor acquire timed out alias={alias} wait={wait}s"
                            ) from e
                        time.sleep(0.05)
            else:
                dsn = resolve_dsn(self._db_registry, alias)
                if not str(dsn).strip():
                    raise ValueError(
                        f"oracle dsn is required alias={alias}"
                    )
                conn = self.get_connection(dsn, threaded=True)

            validate_oracle_connection(conn)
            cursor = conn.cursor()
            yield cursor
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if pool is not None and conn is not None:
                try:
                    pool.release(conn)
                except Exception:
                    pass

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