#!/usr/bin/env python3
"""
store.py — Thread-safe SQLite 저장소.

· sqlite.path는 디렉토리 경로로 사용한다.
· 테이블마다 별도 SQLite 파일(<table>.db)을 생성한다.
· sqlite_connections 설정이 있으면 테이블별로 특정 SQLite 파일/DDL을 적용한다.
· 컬럼은 dict 키로부터 동적으로 추론한다.
· upsert_many()로 배치 삽입/갱신.
· replicate_from()으로 원격에서 받은 직렬화 데이터 그대로 저장.
"""

import json
import re
import sqlite3
import threading
from pathlib import Path
from typing import Any


class Store:
    def __init__(
        self,
        base_dir: str,
        *,
        logger=None,
        replication_cfg: dict[str, Any] | None = None,
        sqlite_connections: list[dict[str, Any]] | None = None,
    ) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conns: dict[str, sqlite3.Connection] = {}
        self._logger = logger
        self._replication_cfg = replication_cfg or {}
        self._sqlite_connections = sqlite_connections or []
        self._sqlite_targets: dict[str, str] = self._build_sqlite_targets()
        self._table_routes: dict[str, dict[str, Any]] = self._build_table_routes()
        self._ddl_applied: set[tuple[str, str]] = set()

    def _normalize_table(self, table: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_]", "_", table).strip("_")
        if not normalized:
            raise ValueError(f"invalid table name: {table}")
        return normalized.lower()

    def _build_sqlite_targets(self) -> dict[str, str]:
        targets: dict[str, str] = {}

        # New top-level section (preferred): sqlite_connections
        for entry in self._sqlite_connections:
            if not isinstance(entry, dict):
                continue
            # table route와 별도로 sqlite 이름을 쓸 수 있게 optional 지원
            name = str(entry.get("name", "")).strip()
            path = str(entry.get("path", "")).strip()
            if name and path:
                targets[name] = path

        # Legacy fallback under replication
        for name, target in (self._replication_cfg.get("sqlite_targets", {}) or {}).items():
            if isinstance(target, dict):
                path = str(target.get("path", "")).strip()
                if path:
                    targets.setdefault(str(name).strip(), path)

        return targets

    def _build_table_routes(self) -> dict[str, dict[str, Any]]:
        routes: dict[str, dict[str, Any]] = {}

        # New top-level section (preferred): sqlite_connections
        for entry in self._sqlite_connections:
            if not isinstance(entry, dict):
                continue
            table = str(entry.get("table", "")).strip()
            if not table:
                continue
            sqlite_target = str(entry.get("sqlite", "")).strip()
            path_value = str(entry.get("path", "")).strip()
            routes[self._normalize_table(table)] = {
                "name": str(entry.get("name", "")).strip(),
                "sqlite": sqlite_target,
                "path": path_value,
                "ddl_file": str(entry.get("ddl_file", "")).strip(),
            }

        # Legacy fallback under replication
        for table, route in (self._replication_cfg.get("table_routes", {}) or {}).items():
            if not isinstance(route, dict):
                continue
            key = self._normalize_table(str(table))
            routes.setdefault(
                key,
                {
                    "name": "",
                    "sqlite": str(route.get("sqlite", "")).strip(),
                    "ddl_file": str(route.get("ddl_file", "")).strip(),
                },
            )

        return routes

    def _resolve_db_path(self, table: str) -> Path:
        key = self._normalize_table(table)
        route = self._table_routes.get(key, {})
        route_path = str(route.get("path", "")).strip()
        sqlite_target = str(route.get("sqlite", "")).strip()

        if route_path:
            path_obj = Path(route_path)
            if not path_obj.is_absolute():
                path_obj = Path.cwd() / path_obj
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            return path_obj

        if sqlite_target:
            target_path = self._sqlite_targets.get(sqlite_target, sqlite_target)
            path_obj = Path(target_path)
            if not path_obj.is_absolute():
                path_obj = Path.cwd() / path_obj
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            return path_obj

        return self._base_dir / f"{key}.db"

    def _get_conn_for_table(self, table: str) -> tuple[sqlite3.Connection, str]:
        db_path = self._resolve_db_path(table)
        key = str(db_path.resolve())
        conn = self._conns.get(key)
        if conn is not None:
            return conn, key

        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        self._conns[key] = conn
        return conn, key

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

        with self._lock:
            for table, route in self._table_routes.items():
                ddl_file = str(route.get("ddl_file", "")).strip()
                if not ddl_file:
                    continue

                conn = None
                conn_key = ""
                try:
                    conn, conn_key = self._get_conn_for_table(table)
                    cur = conn.cursor()
                    exists_before = self._table_exists(cur, table)
                    if exists_before:
                        if self._logger is not None:
                            self._logger.info(
                                "[Store] ddl check table=%s already exists", table
                            )
                        self._ddl_applied.add((conn_key, table))
                        cur.close()
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
                    cur.close()
                except Exception:
                    if self._logger is not None:
                        self._logger.exception(
                            "[Store] ddl init failed table=%s ddl=%s",
                            table,
                            ddl_file,
                        )

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
    def upsert_many(self, table: str, rows: list[dict[str, Any]]) -> int:
        """rows를 table에 일괄 삽입/갱신. 삽입된 행 수 반환."""
        if not rows:
            return 0
        with self._lock:
            conn, conn_key = self._get_conn_for_table(table)
            cur = conn.cursor()
            self._upsert_rows(cur, table, rows, conn_key=conn_key)
            conn.commit()
            return cur.rowcount

    def replicate_from(self, payload: bytes) -> None:
        """replicator가 받은 직렬화 페이로드를 그대로 저장."""
        data: dict = json.loads(payload)
        self.replicate_message(data)

    def replicate_message(self, data: dict[str, Any]) -> None:
        """역직렬화된 복제 메시지를 저장."""
        table: str = data["table"]
        rows: list[dict] = data["rows"]
        self.upsert_many(table, rows)

    def serialize(self, table: str, rows: list[dict], metadata: dict[str, Any] | None = None) -> bytes:
        """복제 전송용 직렬화."""
        payload = {"table": table, "rows": rows}
        if metadata:
            payload.update(metadata)
        return json.dumps(payload, ensure_ascii=False, default=str).encode()

    def query(self, sql: str, params: tuple = (), table: str | None = None) -> list[dict]:
        with self._lock:
            if table is None:
                if len(self._conns) == 1:
                    conn = next(iter(self._conns.values()))
                else:
                    raise ValueError("table is required when multiple table DB files exist")
            else:
                conn, _ = self._get_conn_for_table(table)
            cur = conn.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            for conn in self._conns.values():
                conn.close()
            self._conns.clear()
