#!/usr/bin/env python3
"""
store.py — Thread-safe SQLite 저장소.

· sqlite.path는 디렉토리 경로로 사용한다.
· 테이블마다 별도 SQLite 파일(<table>.db)을 생성한다.
· sqlite_connections 설정이 있으면 테이블별로 특정 SQLite 파일/DDL을 적용한다.
· 컬럼은 dict 키로부터 동적으로 추론한다.
· upsert_many()로 배치 삽입/갱신.
"""

import re
import shutil
import sqlite3
import threading
from contextlib import ExitStack
from pathlib import Path
from typing import Any


class _WriteLockPool:
    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def get(self, key: str) -> threading.Lock:
        with self._meta_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock


class Store:
    def __init__(
        self,
        base_dir: str,
        *,
        logger=None,
        replication_cfg: dict[str, Any] | None = None,
        sqlite_connections: list[dict[str, Any]] | None = None,
        object_sqlite_types: list[dict[str, Any]] | None = None,
        object_sqlite_done_dir: str | None = None,
    ) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._object_done_dir = (
            Path(object_sqlite_done_dir) if object_sqlite_done_dir
            else self._base_dir / "objects" / "done"
        )
        self._meta_lock = threading.Lock()
        self._write_conns: dict[str, sqlite3.Connection] = {}
        self._write_locks = _WriteLockPool()
        self._logger = logger
        self._replication_cfg = replication_cfg or {}
        self._sqlite_connections = sqlite_connections or []
        self._object_sqlite_types = object_sqlite_types or []
        self._table_routes: dict[str, dict[str, Any]] = self._build_table_routes()
        self._object_type_ddls: dict[str, str] = self._build_object_type_ddls()
        self._ddl_applied: set[tuple[str, str]] = set()
        self._closed = False

    def _normalize_scope(self, scope: str) -> str:
        normalized = str(scope or "system").strip().lower()
        if normalized not in ("system", "object"):
            raise ValueError(f"invalid scope: {scope}")
        return normalized

    def _normalize_object_name(self, object_name: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_-]", "_", str(object_name)).strip("_")
        if not normalized:
            raise ValueError(f"invalid object_name: {object_name}")
        return normalized

    def _normalize_object_type(self, object_type: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_-]", "_", str(object_type)).strip("_")
        if not normalized:
            raise ValueError(f"invalid object_type: {object_type}")
        return normalized.upper()

    def _normalize_table(self, table: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_]", "_", table).strip("_")
        if not normalized:
            raise ValueError(f"invalid table name: {table}")
        return normalized.lower()

    def _resolve_route_db_path(self, path_value: str) -> str:
        p = Path(path_value)
        if not p.is_absolute():
            p = Path.cwd() / p
        return str(p)

    def _build_table_routes(self) -> dict[str, dict[str, Any]]:
        routes: dict[str, dict[str, Any]] = {}
        # sqlite_connections 기반 테이블 라우팅
        for entry in self._sqlite_connections:
            if not isinstance(entry, dict):
                continue
            table = str(entry.get("table", "")).strip()
            if not table:
                continue
            path_value = str(entry.get("path", "")).strip()
            if not path_value:
                continue
            resolved_db_path = self._resolve_route_db_path(path_value)
            routes[self._normalize_table(table)] = {
                "db_path": resolved_db_path,
                "ddl_file": str(entry.get("ddl_file", "")).strip(),
            }

        return routes

    def _build_object_type_ddls(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for entry in self._object_sqlite_types:
            if not isinstance(entry, dict):
                continue
            object_type = str(entry.get("type", "")).strip()
            ddl_file = str(entry.get("ddl_file", "")).strip()
            if not object_type or not ddl_file:
                continue
            key = self._normalize_object_type(object_type)
            mapping[key] = ddl_file
        return mapping

    def _resolve_db_path(self, table: str, *, scope: str = "system", object_name: str | None = None) -> Path:
        normalized_scope = self._normalize_scope(scope)
        if normalized_scope == "object":
            if not object_name:
                raise ValueError("object_name is required when scope='object'")
            obj = self._normalize_object_name(object_name)
            path_obj = self._base_dir / "objects" / f"{obj}.db"
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            return path_obj

        key = self._normalize_table(table)
        route = self._table_routes.get(key, {})
        route_path = str(route.get("db_path", "")).strip()

        if route_path:
            path_obj = Path(route_path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            return path_obj

        return self._base_dir / f"{key}.db"

    def _configure_conn(self, conn: sqlite3.Connection, *, read_only: bool) -> sqlite3.Connection:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        if read_only:
            conn.execute("PRAGMA query_only=ON")
        return conn

    def _get_db_key_for_table(self, table: str, *, scope: str = "system", object_name: str | None = None) -> str:
        db_path = self._resolve_db_path(table, scope=scope, object_name=object_name)
        return str(db_path.resolve())

    def _get_db_key_for_object(self, object_name: str) -> str:
        return self._get_db_key_for_table("_", scope="object", object_name=object_name)

    def _get_write_conn_for_key(self, key: str) -> sqlite3.Connection:
        with self._meta_lock:
            if self._closed:
                raise RuntimeError("store is closed")
            conn = self._write_conns.get(key)
            if conn is not None:
                return conn

            conn = self._configure_conn(sqlite3.connect(key, check_same_thread=False), read_only=False)
            self._write_conns[key] = conn
            return conn

    def _get_write_conn_for_table(
        self,
        table: str,
        *,
        scope: str = "system",
        object_name: str | None = None,
    ) -> tuple[sqlite3.Connection, str]:
        key = self._get_db_key_for_table(table, scope=scope, object_name=object_name)
        return self._get_write_conn_for_key(key), key

    def _open_read_conn_for_key(self, key: str) -> sqlite3.Connection:
        with self._meta_lock:
            if self._closed:
                raise RuntimeError("store is closed")
        return self._configure_conn(sqlite3.connect(key, check_same_thread=False), read_only=True)

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    # ── 내부 ────────────────────────────────────────────────────────
    def _table_exists(self, cursor: sqlite3.Cursor, table: str) -> bool:
        chk = cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        return chk is not None

    def _resolve_ddl_path(self, ddl_file: str) -> Path:
        p = Path(ddl_file)
        if p.is_absolute():
            return p
        return (Path.cwd() / p).resolve()

    def initialize_registered_ddls(self) -> None:
        """시작 시 등록된 sqlite_connections의 DDL을 점검/생성한다."""
        if not self._table_routes:
            return

        for table, route in self._table_routes.items():
            ddl_file = str(route.get("ddl_file", "")).strip()
            if not ddl_file:
                continue

            conn = None
            conn_key = ""
            try:
                conn, conn_key = self._get_write_conn_for_table(table)
                with self._write_locks.get(conn_key):
                    cur = conn.cursor()
                    try:
                        exists_before = self._table_exists(cur, table)
                        if exists_before:
                            if self._logger is not None:
                                self._logger.info(
                                    "[Store] ddl check table=%s already exists", table
                                )
                            self._ddl_applied.add((conn_key, table))
                            continue

                        self._apply_route_ddl_if_needed(cur, table, conn_key=conn_key)
                        conn.commit()
                        exists_after = self._table_exists(cur, table)
                        if self._logger is not None:
                            if exists_after:
                                self._logger.info(
                                    "[Store] ddl applied table=%s ddl=%s created",
                                    table,
                                    ddl_file,
                                )
                            else:
                                self._logger.warning(
                                    "[Store] ddl applied but table missing table=%s ddl=%s",
                                    table,
                                    ddl_file,
                                )
                    finally:
                        cur.close()
            except Exception:
                if self._logger is not None:
                    self._logger.exception(
                        "[Store] ddl init failed table=%s ddl=%s",
                        table,
                        ddl_file,
                    )

    def initialize_object_sqlite(self, object_name: str, object_type: str) -> Path:
        """객체별 SQLite 파일을 생성하고 object_type에 매핑된 DDL을 적용한다."""
        normalized_type = self._normalize_object_type(object_type)
        ddl_file = self._object_type_ddls.get(normalized_type, "").strip()
        if not ddl_file:
            raise ValueError(f"object type is not registered: {normalized_type}")

        conn_key = self._get_db_key_for_object(object_name)
        conn = self._get_write_conn_for_key(conn_key)

        apply_key = (conn_key, f"__object_type__:{normalized_type}")
        with self._write_locks.get(conn_key):
            if apply_key in self._ddl_applied:
                return Path(conn_key)

            ddl_path = self._resolve_ddl_path(ddl_file)
            if not ddl_path.exists():
                raise FileNotFoundError(
                    f"Object DDL file not found type={normalized_type}: {ddl_path}"
                )

            cur = conn.cursor()
            try:
                script = ddl_path.read_text(encoding="utf-8")
                cur.executescript(script)
                conn.commit()
                self._ddl_applied.add(apply_key)
            finally:
                cur.close()

        return Path(conn_key)

    def finalize_object_sqlite(self, object_name: str, *, dest_dir: str | None = None) -> Path:
        """객체 처리 완료 후 SQLite 파일을 완료 디렉토리로 이동한다.

        WAL 체크포인트 → 연결 종료 → 파일 이동 순서로 안전하게 처리한다.
        이동된 파일 경로를 반환한다.
        """
        conn_key = self._get_db_key_for_object(object_name)
        db_path = Path(conn_key)

        if not db_path.exists():
            raise FileNotFoundError(
                f"Object SQLite not found object_name={object_name}: {db_path}"
            )

        done_dir = Path(dest_dir) if dest_dir else self._object_done_dir
        done_dir.mkdir(parents=True, exist_ok=True)
        dest_path = done_dir / db_path.name

        lock = self._write_locks.get(conn_key)
        with lock:
            # WAL 체크포인트: 모든 변경 내용을 메인 파일에 반영
            with self._meta_lock:
                conn = self._write_conns.pop(conn_key, None)
            if conn is not None:
                try:
                    conn.execute("PRAGMA wal_checkpoint(FULL)")
                    conn.commit()
                finally:
                    conn.close()

            # .db, .db-wal, .db-shm 파일 이동
            for suffix in ("", "-wal", "-shm"):
                src = db_path.parent / (db_path.name + suffix)
                if src.exists():
                    shutil.move(str(src), str(done_dir / src.name))

        # 이 객체 관련 ddl_applied 항목 제거
        with self._meta_lock:
            self._ddl_applied = {
                k for k in self._ddl_applied if k[0] != conn_key
            }

        if self._logger is not None:
            self._logger.info(
                "[Store] finalized object_name=%s -> %s", object_name, dest_path
            )

        return dest_path

    def _apply_route_ddl_if_needed(
        self,
        cursor: sqlite3.Cursor,
        table: str,
        *,
        conn_key: str,
    ) -> None:
        route = self._table_routes.get(table, {})
        ddl_file = str(route.get("ddl_file", "")).strip()
        if not ddl_file:
            return

        apply_key = (conn_key, table)
        if apply_key in self._ddl_applied:
            return

        ddl_path = self._resolve_ddl_path(ddl_file)
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found for table={table}: {ddl_path}")

        script = ddl_path.read_text(encoding="utf-8")
        cursor.executescript(script)
        self._ddl_applied.add(apply_key)

    def _ensure_table(self, cursor: sqlite3.Cursor, table: str, row: dict, *, conn_key: str) -> None:
        self._apply_route_ddl_if_needed(cursor, table, conn_key=conn_key)
        if self._table_exists(cursor, table):
            return
        cols = ", ".join(
            f'"{k}" TEXT' for k in row
            if k not in ("_table", "_ts")
        )
        cursor.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" '
            f'(_rowid INTEGER PRIMARY KEY AUTOINCREMENT, '
            f'collected_at TEXT DEFAULT CURRENT_TIMESTAMP, '
            f'{cols})'
        )

    def _upsert_rows(
        self,
        cursor: sqlite3.Cursor,
        table: str,
        rows: list[dict],
        *,
        conn_key: str,
    ) -> None:
        if not rows:
            return
        self._ensure_table(cursor, table, rows[0], conn_key=conn_key)
        keys = [k for k in rows[0] if k not in ("_table", "_ts")]
        placeholders = ", ".join("?" * len(keys))
        col_names = ", ".join(f'"{k}"' for k in keys)
        sql = (
            f'INSERT OR REPLACE INTO "{table}" ({col_names}) '
            f"VALUES ({placeholders})"
        )
        cursor.executemany(
            sql, [[str(row.get(k, "")) for k in keys] for row in rows]
        )

    # ── 공개 API ─────────────────────────────────────────────────────
    def upsert_many(
        self,
        table: str,
        rows: list[dict[str, Any]],
        *,
        scope: str = "system",
        object_name: str | None = None,
    ) -> int:
        """rows를 table에 일괄 삽입/갱신. 삽입된 행 수 반환."""
        if not rows:
            return 0
        normalized_scope = self._normalize_scope(scope)
        conn, conn_key = self._get_write_conn_for_table(
            table,
            scope=normalized_scope,
            object_name=object_name,
        )
        with self._write_locks.get(conn_key):
            cur = conn.cursor()
            try:
                self._upsert_rows(cur, table, rows, conn_key=conn_key)
                conn.commit()
                return cur.rowcount
            finally:
                cur.close()

    def replicate_message(self, data: dict[str, Any]) -> None:
        """역직렬화된 복제 메시지를 저장."""
        table: str = data["table"]
        rows: list[dict] = data["rows"]
        self.upsert_many(table, rows)

    def query(
        self,
        sql: str,
        params: tuple = (),
        table: str | None = None,
        *,
        scope: str = "system",
        object_name: str | None = None,
    ) -> list[dict]:
        normalized_scope = self._normalize_scope(scope)
        if normalized_scope == "object":
            if not object_name:
                raise ValueError("object_name is required when scope='object'")
            resolved_table = table or "_"
            conn_key = self._get_db_key_for_table(
                resolved_table,
                scope="object",
                object_name=object_name,
            )
        elif table is None:
            with self._meta_lock:
                if len(self._write_conns) != 1:
                    raise ValueError("table is required when multiple table DB files exist")
                conn_key = next(iter(self._write_conns))
        else:
            conn_key = self._get_db_key_for_table(table, scope="system")

        conn = self._open_read_conn_for_key(conn_key)
        try:
            cur = conn.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    def close(self) -> None:
        with self._meta_lock:
            if self._closed:
                return
            self._closed = True
            conn_items = sorted(self._write_conns.items())
            self._write_conns = {}

        with ExitStack() as stack:
            for conn_key, _conn in conn_items:
                stack.enter_context(self._write_locks.get(conn_key))
            for _conn_key, conn in conn_items:
                conn.close()
