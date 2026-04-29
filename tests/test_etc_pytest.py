from pathlib import Path
import sqlite3
from types import SimpleNamespace
import sys

from src.etcmanager import EtcManager
from src.logger import Logger
from src.store import Store


def test_etc_manager_runs_heartbeat_and_logs(tmp_path: Path) -> None:
    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.etc", log_base=str(tmp_path / "logs" / "etc"), console=False)

    cfg = {
        "node_id": "node-test",
        "sqlite": {"path": str(tmp_path / "app1")},
        "pipeline": {
            "etc": {
                "enabled": True,
                "sqlite_log": {"db_path": str(tmp_path / "app1" / "pipeline" / "etc_runs.db")},
                "tasks": [
                    {
                        "name": "etc-heartbeat",
                        "type": "sqlite_heartbeat",
                        "enabled": True,
                        "interval_sec": 1,
                        "table": "etc_heartbeat",
                    }
                ],
            }
        },
    }

    mgr = EtcManager(cfg=cfg, store=store, logger=log)
    try:
        mgr.run({"job_name": "etc"})

        rows = store.query('SELECT COUNT(*) AS cnt FROM "etc_heartbeat"', table="etc_heartbeat")
        assert int(rows[0]["cnt"]) >= 1

        etc_log_db = tmp_path / "app1" / "pipeline" / "etc_runs.db"
        assert etc_log_db.exists()
        conn = sqlite3.connect(str(etc_log_db))
        try:
            cur = conn.execute("SELECT COUNT(*) FROM etc_runs")
            cnt = int(cur.fetchone()[0])
            assert cnt >= 1
        finally:
            conn.close()
    finally:
        mgr.close()
        store.close()
        log.stop()


def test_etc_workers_come_from_config(tmp_path: Path, monkeypatch) -> None:
    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.etc.workers", log_base=str(tmp_path / "logs" / "etc-workers"), console=False)

    cfg = {
        "node_id": "node-test",
        "pipeline": {
            "etc": {
                "enabled": True,
                "workers": 2,
                "tasks": [
                    {"name": "a", "type": "sqlite_heartbeat", "enabled": True, "interval_sec": 1},
                    {"name": "b", "type": "sqlite_heartbeat", "enabled": True, "interval_sec": 1},
                ],
            }
        },
    }

    created_workers: list[int] = []

    class FakeExecutor:
        def __init__(self, max_workers: int):
            created_workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            fn(*args, **kwargs)

            class DoneFuture:
                def result(self):
                    return None

            return DoneFuture()

    monkeypatch.setattr("src.etcmanager.ThreadPoolExecutor", FakeExecutor)

    mgr = EtcManager(cfg=cfg, store=store, logger=log)
    try:
        mgr.run({"job_name": "etc"})
        assert created_workers == [2]
    finally:
        mgr.close()
        store.close()
        log.stop()


def test_etc_oracle_probe_validates_connection_first(tmp_path: Path, monkeypatch) -> None:
    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.etc.oracle", log_base=str(tmp_path / "logs" / "etc-oracle"), console=False)
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
        "node_id": "node-test",
        "pipeline": {
            "etc": {
                "enabled": True,
                "oracle": {"dsn": "dsn"},
                "tasks": [
                    {"name": "oracle-probe", "type": "oracle_probe", "enabled": True, "interval_sec": 1}
                ],
            }
        },
    }

    mgr = EtcManager(cfg=cfg, store=store, logger=log)
    try:
        mgr.run({"job_name": "etc"})
        assert executed_sql[:2] == ["SELECT sysdate FROM dual", "SELECT 1 FROM dual"]
    finally:
        mgr.close()
        store.close()
        log.stop()
