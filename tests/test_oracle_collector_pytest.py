from types import SimpleNamespace
import sys

from src.logger import Logger
from src.oracle_collector import OracleCollector
from src.store import Store


def test_oracle_collector_validates_connection_before_query(tmp_path, monkeypatch) -> None:
    executed_sql: list[str] = []

    class FakeCursor:
        description = [("ID",), ("UPDATED_AT",)]

        def execute(self, sql, params=None):
            executed_sql.append(str(sql))

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "2026-04-28 00:00:00")]

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn: FakeConnection())
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.oracle.collector", log_base=str(tmp_path / "logs" / "oracle-collector"), console=False)
    collector = OracleCollector(
        cfg={
            "dsn": "dsn",
            "jobs": [
                {
                    "name": "inventory",
                    "sql": "SELECT id, updated_at FROM inventory WHERE updated_at > :last_ts",
                    "table": "inventory",
                }
            ],
        },
        store=store,
        logger=log,
    )

    try:
        results = collector.collect()
        assert len(results) == 1
        assert executed_sql[:2] == [
            "SELECT sysdate FROM dual",
            "SELECT id, updated_at FROM inventory WHERE updated_at > :last_ts",
        ]
    finally:
        collector.stop()
        store.close()
        log.stop()