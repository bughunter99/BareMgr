import sys
from types import SimpleNamespace

from src.oracle_connection_manager import OracleConnectionManager


def test_oracle_connection_manager_reuses_same_dsn(monkeypatch) -> None:
    connect_calls: list[tuple[str, bool]] = []

    class FakeCursor:
        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return (1,)

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    def fake_connect(dsn, threaded=True):
        connect_calls.append((dsn, threaded))
        return FakeConnection()

    monkeypatch.setitem(sys.modules, "oracledb", SimpleNamespace(connect=fake_connect))
    sys.modules.pop("cx_Oracle", None)

    manager = OracleConnectionManager()
    conn1 = manager.get_connection("shared-dsn", threaded=True)
    conn2 = manager.get_connection("shared-dsn", threaded=True)

    assert conn1 is conn2
    assert connect_calls == [("shared-dsn", True)]


def test_oracle_connection_manager_invalidates_and_reopens(monkeypatch) -> None:
    connect_calls: list[str] = []

    class FakeCursor:
        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return (1,)

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    def fake_connect(dsn, threaded=True):
        connect_calls.append(dsn)
        return FakeConnection()

    monkeypatch.setitem(sys.modules, "oracledb", SimpleNamespace(connect=fake_connect))
    sys.modules.pop("cx_Oracle", None)

    manager = OracleConnectionManager()
    manager.get_connection("shared-dsn")
    manager.invalidate("shared-dsn")
    manager.get_connection("shared-dsn")

    assert connect_calls == ["shared-dsn", "shared-dsn"]