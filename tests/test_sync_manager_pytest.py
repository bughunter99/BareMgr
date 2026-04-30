from pathlib import Path
from types import SimpleNamespace
import sys

from src.logger import Logger
from src.syncmanager import SyncJobManager, SyncOracleToOracle


def test_sync_manager_dry_run_no_oracle_connection(tmp_path: Path) -> None:
    cfg = {
        "sqlite": {"path": str(tmp_path / "app1")},
        "pipeline": {
            "sync": {
                "enabled": True,
                "dry_run": True,
                "mode": "incremental",
                "source_dsn": "",
                "target_dsn": "",
                "tables": ["INVENTORY"],
                "key_column": "ID",
                "timestamp_column": "UPDATED_AT",
                "batch_size": 100,
                "checkpoint_db": str(tmp_path / "sync_checkpoint.db"),
            }
        },
    }

    log = Logger(name="test.sync", log_base=str(tmp_path / "logs" / "sync"), console=False)
    mgr = SyncJobManager(cfg=cfg, logger=log)
    try:
        mgr.run({"job_name": "sync"})
    finally:
        log.stop()


def test_sync_workers_come_from_config(tmp_path: Path, monkeypatch) -> None:
    cfg = {
        "sqlite": {"path": str(tmp_path / "app1")},
        "pipeline": {
            "sync": {
                "enabled": True,
                "dry_run": True,
                "workers": 3,
                "tables": ["INVENTORY", "ORDERS"],
                "checkpoint_db": str(tmp_path / "sync_checkpoint.db"),
            }
        },
    }

    log = Logger(name="test.sync.workers", log_base=str(tmp_path / "logs" / "sync-workers"), console=False)
    created_workers: list[int] = []

    class FakeExecutor:
        def __init__(self, max_workers: int):
            created_workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, fn, items):
            return [fn(item) for item in items]

    monkeypatch.setattr("src.syncmanager.ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(SyncOracleToOracle, "_estimate_source_count", lambda self, table: 1)

    mgr = SyncJobManager(cfg=cfg, logger=log)
    try:
        mgr.run({"job_name": "sync"})
        assert created_workers == [3]
    finally:
        log.stop()


def test_sync_validates_connection_before_table_sync(tmp_path: Path, monkeypatch) -> None:
    executed_sql: list[str] = []

    class FakeCursor:
        def execute(self, sql, params=None):
            executed_sql.append(str(sql))

        def fetchone(self):
            return (1,)

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection())
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    cfg = {
        "sqlite": {"path": str(tmp_path / "app1")},
        "pipeline": {
            "sync": {
                "enabled": True,
                "dry_run": False,
                "workers": 1,
                "source_dsn": "source-dsn",
                "target_dsn": "target-dsn",
                "tables": ["INVENTORY"],
                "checkpoint_db": str(tmp_path / "sync_checkpoint.db"),
            }
        },
    }

    log = Logger(name="test.sync.validate", log_base=str(tmp_path / "logs" / "sync-validate"), console=False)
    syncer = SyncOracleToOracle(cfg=cfg, logger=log)
    monkeypatch.setattr(SyncOracleToOracle, "_sync_table", lambda self, table, source_conn, target_conn: 0)

    try:
        result = syncer.run({"job_name": "sync"})
        assert result["rows"] == 0
        assert executed_sql[:2] == ["SELECT sysdate FROM dual", "SELECT sysdate FROM dual"]
    finally:
        log.stop()
