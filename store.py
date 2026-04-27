#!/usr/bin/env python3
"""
store.py — Thread-safe SQLite 저장소.

· 테이블은 최초 upsert 시 자동 생성된다.
· 컬럼은 dict 키로부터 동적으로 추론한다.
· upsert_many()로 배치 삽입/갱신.
· replicate_from()으로 원격에서 받은 직렬화 데이터 그대로 저장.
"""

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class Store:
    def __init__(self, db_path: str) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

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
            cur = self._conn.cursor()
            self._upsert_rows(cur, table, rows)
            self._conn.commit()
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

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
