#!/usr/bin/env python3
"""
store.py — Thread-safe SQLite 저장소.

· sqlite.path는 디렉토리 경로로 사용한다.
· 테이블마다 별도 SQLite 파일(<table>.db)을 생성한다.
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
    def __init__(self, base_dir: str) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conns: dict[str, sqlite3.Connection] = {}

    def _normalize_table(self, table: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9_]", "_", table).strip("_")
        if not normalized:
            raise ValueError(f"invalid table name: {table}")
        return normalized.lower()

    def _get_conn(self, table: str) -> sqlite3.Connection:
        key = self._normalize_table(table)
        conn = self._conns.get(key)
        if conn is not None:
            return conn

        db_path = self._base_dir / f"{key}.db"
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        self._conns[key] = conn
        return conn

    # ── 내부 ────────────────────────────────────────────────────────
    def _ensure_table(self, cursor: sqlite3.Cursor, table: str, row: dict) -> None:
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
    ) -> None:
        if not rows:
            return
        self._ensure_table(cursor, table, rows[0])
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
            conn = self._get_conn(table)
            cur = conn.cursor()
            self._upsert_rows(cur, table, rows)
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
                conn = self._get_conn(table)
            cur = conn.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            for conn in self._conns.values():
                conn.close()
            self._conns.clear()
