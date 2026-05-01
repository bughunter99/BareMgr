from pathlib import Path

from src.logger import Logger
from src.syncmanager import SyncJobManager, SyncOracleToOracle
from src.appconfig import AppConfig


def test_sync_manager_dry_run_no_oracle_connection(tmp_path: Path) -> None:
    cfg = {
        "sqlite": {"path": str(tmp_path / "app1")},
        "syncmanager": {
            "enabled": True,
            "dry_run": True,
            "mode": "incremental",
            "tables": ["INVENTORY"],
            "key_column": "ID",
            "timestamp_column": "UPDATED_AT",
            "batch_size": 100,
            "checkpoint_db": str(tmp_path / "sync_checkpoint.db"),
        },
    }

    log = Logger(name="test.sync", log_base=str(tmp_path / "logs" / "sync"), console=False)
    mgr = SyncJobManager(app_config=AppConfig(cfg), logger=log)
    try:
        mgr.run({"job_name": "sync"})
    finally:
        log.stop()


def test_sync_workers_come_from_config(tmp_path: Path) -> None:
    cfg = {
        "sqlite": {"path": str(tmp_path / "app1")},
        "syncmanager": {
            "enabled": True,
            "dry_run": True,
            "workers": 3,
            "tables": ["INVENTORY", "ORDERS"],
            "checkpoint_db": str(tmp_path / "sync_checkpoint.db"),
        },
    }

    log = Logger(name="test.sync.workers", log_base=str(tmp_path / "logs" / "sync-workers"), console=False)

    mgr = SyncJobManager(app_config=AppConfig(cfg), logger=log)
    try:
        # workers 설정이 section_cfg에서 올바르게 읽히는지 확인
        assert mgr._cfg.get("workers") == 3
        assert mgr._tables == ["INVENTORY", "ORDERS"]
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

    cfg = {
        "sqlite": {"path": str(tmp_path / "app1")},
        "oracle_connections": [
            {"alias": "DB_MAIN", "tns": "source-dsn", "user": "u", "password": "p"},
            {"alias": "DB_TARGET", "tns": "target-dsn", "user": "u", "password": "p"},
        ],
        "syncmanager": {
            "enabled": True,
            "dry_run": False,
            "workers": 1,
            "source_db": "DB_MAIN",
            "target_db": "DB_TARGET",
            "tables": ["INVENTORY"],
            "checkpoint_db": str(tmp_path / "sync_checkpoint.db"),
        },
    }

    log = Logger(name="test.sync.validate", log_base=str(tmp_path / "logs" / "sync-validate"), console=False)
    syncer = SyncOracleToOracle(app_config=AppConfig(cfg), logger=log)
    monkeypatch.setattr(syncer._conn_manager, "get_connection", lambda dsn, **_: FakeConnection())
    monkeypatch.setattr(SyncOracleToOracle, "_copy_table_full", lambda self, table: 0)

    try:
        syncer.run({"job_name": "sync"})
        # _estimate_source_count 호출 시 validate_oracle_connection → SELECT sysdate FROM dual 검증
        assert "SELECT sysdate FROM dual" in executed_sql
    finally:
        log.stop()
