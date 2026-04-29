import sqlite3
from pathlib import Path
import sys
from types import SimpleNamespace

from src.logger import Logger
from src.oracle_connection_manager import OracleConnectionManager
from src.oracle_collector import OracleCollector
from src.business import ProcessingPipeline
from src.store import Store


def test_processing_pipeline_generates_logs_and_output(tmp_path: Path) -> None:
    data_dir = tmp_path / "app1"
    store = Store(str(data_dir))
    log = Logger(name="test.processing", log_base=str(tmp_path / "logs" / "processing"), console=False)

    cfg = {
        "node_id": "node-test",
        "collectors": {
            "oracle": {
                "jobs": [{"table": "inventory"}],
            }
        },
        "pipeline": {
            "processing": {
                "worker_mode": "thread",
                "workers": 2,
                "batch_min": 6,
                "batch_max": 8,
                "input_tables": ["inventory"],
                "output_table": "pipeline_generated_objects",
                "object_log": {
                    "db_path": str(data_dir / "pipeline" / "object_results.db"),
                },
                "drone": {
                    "enabled": True,
                    "dry_run": True,
                },
                "oracle_write": {
                    "enabled": True,
                    "dry_run": True,
                    "table": "PIPELINE_RESULTS",
                },
            }
        },
    }

    store.upsert_many("inventory", [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}])

    pipeline = ProcessingPipeline(cfg=cfg, store=store, logger=log)
    try:
        pipeline.run({"job_name": "processing"})

        out_rows = store.query(
            'SELECT COUNT(*) AS cnt FROM "pipeline_generated_objects"',
            table="pipeline_generated_objects",
        )
        assert 6 <= int(out_rows[0]["cnt"]) <= 8

        object_log_db = data_dir / "pipeline" / "object_results.db"
        assert object_log_db.exists()

        conn = sqlite3.connect(str(object_log_db))
        try:
            cur = conn.execute("SELECT COUNT(*) FROM object_results")
            cnt = int(cur.fetchone()[0])
            assert 6 <= cnt <= 8
        finally:
            conn.close()
    finally:
        pipeline.close()
        store.close()
        log.stop()


def test_processing_logs_job_id_when_worker_dequeues_entry(tmp_path: Path) -> None:
    data_dir = tmp_path / "app1"
    store = Store(str(data_dir))
    log = Logger(name="test.processing.dequeue", log_base=str(tmp_path / "logs" / "processing-dequeue"), console=False)
    captured_logs: list[str] = []

    def capture_info(msg: str, *args, **kwargs) -> None:
        captured_logs.append(msg % args if args else msg)

    def capture_exception(msg: str, *args, **kwargs) -> None:
        captured_logs.append(msg % args if args else msg)

    log.info = capture_info  # type: ignore[method-assign]
    log.exception = capture_exception  # type: ignore[method-assign]

    cfg = {
        "node_id": "node-test",
        "collectors": {
            "oracle": {
                "jobs": [{"table": "inventory"}],
            }
        },
        "pipeline": {
            "processing": {
                "worker_mode": "thread",
                "workers": 1,
                "batch_min": 1,
                "batch_max": 1,
                "input_tables": ["inventory"],
                "output_table": "pipeline_generated_objects",
                "drone": {"enabled": False},
                "oracle_write": {"enabled": False},
            }
        },
    }

    store.upsert_many("inventory", [{"id": "1", "name": "a"}])

    pipeline = ProcessingPipeline(cfg=cfg, store=store, logger=log)
    try:
        pipeline.run({"job_name": "processing"})
        assert any("dequeued entry obj_id=node-test-" in line for line in captured_logs)
        assert any("completed entry obj_id=node-test-" in line for line in captured_logs)
    finally:
        pipeline.close()
        store.close()
        log.stop()


def test_processing_and_collector_share_connection_manager_for_same_dsn(tmp_path: Path, monkeypatch) -> None:
    connect_calls: list[tuple[str, bool]] = []

    class FakeCursor:
        def __init__(self) -> None:
            self.description = [("ID",), ("UPDATED_AT",)]
            self._last_sql = ""

        def execute(self, sql, params=None):
            self._last_sql = str(sql)

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            if "WHERE updated_at > :last_ts" in self._last_sql:
                return [("1", "2026-04-28 00:00:00")]
            return [("1", "2026-04-28 00:00:00")]

        def fetchmany(self, limit):
            return [("1", "2026-04-28 00:00:00")]

        def close(self):
            pass

        def executemany(self, sql, rows):
            return None

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def close(self):
            pass

    def fake_connect(dsn, threaded=True):
        connect_calls.append((dsn, threaded))
        return FakeConnection()

    monkeypatch.setitem(
        sys.modules,
        "oracledb",
        SimpleNamespace(
            connect=fake_connect,
            DatabaseError=Exception,
        ),
    )
    sys.modules.pop("cx_Oracle", None)

    data_dir = tmp_path / "app1"
    store = Store(str(data_dir))
    log = Logger(name="test.processing.shared", log_base=str(tmp_path / "logs" / "processing-shared"), console=False)
    manager = OracleConnectionManager(log)

    cfg = {
        "node_id": "node-test",
        "collectors": {
            "oracle": {
                "dsn": "shared-dsn",
                "jobs": [
                    {
                        "name": "inventory",
                        "sql": "SELECT id, updated_at FROM inventory WHERE updated_at > :last_ts",
                        "table": "inventory",
                    }
                ],
            }
        },
        "pipeline": {
            "processing": {
                "worker_mode": "thread",
                "workers": 1,
                "batch_min": 1,
                "batch_max": 1,
                "input_tables": ["inventory"],
                "output_table": "pipeline_generated_objects",
                "source_oracle": {
                    "enabled": True,
                    "dsn": "shared-dsn",
                    "sql": "SELECT id, updated_at FROM inventory ORDER BY updated_at DESC",
                    "limit": 1,
                },
                "current_oracle": {
                    "enabled": True,
                    "dsn": "shared-dsn",
                    "sql": "SELECT id, updated_at FROM inventory ORDER BY updated_at DESC",
                    "limit": 1,
                },
                "drone": {"enabled": False},
                "oracle_write": {"enabled": False},
            }
        },
    }

    store.upsert_many("inventory", [{"id": "1", "name": "a"}])

    collector = OracleCollector(
        cfg=cfg["collectors"]["oracle"],
        store=store,
        logger=log,
        connection_manager=manager,
    )
    pipeline = ProcessingPipeline(cfg=cfg, store=store, logger=log, connection_manager=manager)
    try:
        collector.collect()
        pipeline.run({"job_name": "processing"})
        assert connect_calls == [("shared-dsn", True)]
    finally:
        collector.stop()
        pipeline.close()
        manager.close_all()
        store.close()
        log.stop()
