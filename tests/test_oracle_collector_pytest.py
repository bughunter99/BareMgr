from types import SimpleNamespace
import sys

from src.logger import Logger
from src.oracleconnectionmanager import OracleConnectionManager
import src.oraclecollector as oracle_collector_module
from src.oraclecollector import OracleCollector
from src.oraclejobs import OracleJob
from src.oracle_utils import makeDictFactory
from src.store import Store
from src.appconfig import AppConfig


def _install_fake_cx_oracle(monkeypatch, make_connection) -> None:
    class FakeSessionPool:
        def __init__(
            self,
            *,
            user,
            password,
            dsn,
            min,
            max,
            increment,
            threaded,
            getmode,
        ) -> None:
            self._dsn = dsn

        def acquire(self):
            return make_connection(self._dsn)

        def release(self, _conn) -> None:
            return None

        def close(self) -> None:
            return None

    fake_module = SimpleNamespace(
        connect=lambda dsn, threaded=True: make_connection(dsn),
        SessionPool=FakeSessionPool,
        SPOOL_ATTRVAL_WAIT=1,
        SPOOL_ATTRVAL_NOWAIT=2,
    )
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)
    monkeypatch.setattr("src.oracleconnectionmanager.get_cx_oracle", lambda: fake_module)


def _make_manager(log: Logger, registry: dict[str, dict]) -> OracleConnectionManager:
    return OracleConnectionManager(log, db_registry=registry)


def test_oracle_collector_runs_job_query_method(tmp_path, monkeypatch) -> None:
    executed_sql: list[str] = []

    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def execute(self, sql, params=None):
            executed_sql.append(str(sql))

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "ok")]

        def close(self):
            pass

    class FakeConnection:
        def __init__(self, _dsn: str) -> None:
            pass

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    _install_fake_cx_oracle(monkeypatch, lambda dsn: FakeConnection(dsn))

    class JobQueryOnly(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_q", table="t_q", db="DB_MAIN", test_rows=1)

        def query(self, cfg: dict, cursor) -> list[dict]:
            cursor.execute("SELECT 7 AS seq, 'ok' AS label FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.oracle.collector.query", log_base=str(tmp_path / "logs" / "oracle-collector-query"), console=False)
    manager = _make_manager(
        log,
        {
            "DB_MAIN": {
                "user": "u1",
                "password": "p1",
                "tns": "main:1521/ORCL",
                "pool": {"enabled": True, "min": 1, "max": 2, "increment": 1, "threaded": True, "getmode": "wait"},
            }
        },
    )
    collector = OracleCollector(
        app_config=AppConfig({"collectors": {"oracle": {"db": "DB_MAIN"}}}),
        store=store,
        logger=log,
        connection_manager=manager,
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [JobQueryOnly()])
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 1
        assert executed_sql[:2] == [
            "SELECT sysdate FROM dual",
            "SELECT 7 AS seq, 'ok' AS label FROM dual",
        ]
    finally:
        collector.stop()
        manager.close_all()
        store.close()
        log.stop()


def test_oracle_collector_can_resolve_different_db_per_job(tmp_path, monkeypatch) -> None:
    executed: list[tuple[str, str]] = []

    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def __init__(self, dsn: str) -> None:
            self._dsn = dsn

        def execute(self, sql, params=None):
            executed.append((self._dsn, str(sql)))

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "ok")]

        def close(self):
            pass

    class FakeConnection:
        def __init__(self, dsn: str) -> None:
            self._dsn = dsn

        def cursor(self):
            return FakeCursor(self._dsn)

        def close(self):
            pass

    _install_fake_cx_oracle(monkeypatch, lambda dsn: FakeConnection(dsn))

    class JobMain(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_main", table="t_main", db="DB_MAIN")

        def query(self, cfg: dict, cursor) -> list[dict]:
            cursor.execute("SELECT 1 FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    class JobTarget(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_target", table="t_target", db="DB_TARGET")

        def query(self, cfg: dict, cursor) -> list[dict]:
            cursor.execute("SELECT 2 FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app2"))
    log = Logger(name="test.oracle.collector.alias", log_base=str(tmp_path / "logs" / "oracle-collector-alias"), console=False)
    manager = _make_manager(
        log,
        {
            "DB_MAIN": {
                "user": "u1",
                "password": "p1",
                "tns": "main:1521/ORCL",
                "pool": {"enabled": True, "min": 1, "max": 2, "increment": 1, "threaded": True, "getmode": "wait"},
            },
            "DB_TARGET": {
                "user": "u2",
                "password": "p2",
                "tns": "target:1521/ORCL",
                "pool": {"enabled": True, "min": 1, "max": 2, "increment": 1, "threaded": True, "getmode": "wait"},
            },
        },
    )
    collector = OracleCollector(
        app_config=AppConfig({"collectors": {"oracle": {"db": "DB_MAIN"}}}),
        store=store,
        logger=log,
        connection_manager=manager,
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [JobMain(), JobTarget()])
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 2
        dsns = [dsn for dsn, sql in executed if "SELECT" in sql]
        assert "main:1521/ORCL" in dsns
        assert "target:1521/ORCL" in dsns
    finally:
        collector.stop()
        manager.close_all()
        store.close()
        log.stop()


def test_oracle_collector_passes_config_into_job_query(tmp_path, monkeypatch) -> None:
    executed_sql: list[str] = []

    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def execute(self, sql, params=None):
            executed_sql.append(str(sql))

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "cfg")]

        def close(self):
            pass

    class FakeConnection:
        def __init__(self, _dsn: str) -> None:
            pass

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    _install_fake_cx_oracle(monkeypatch, lambda dsn: FakeConnection(dsn))

    class ConfigAwareJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_cfg", table="t_cfg", db="DB_MAIN")

        def query(self, cfg: dict, cursor) -> list[dict]:
            threshold = int((cfg.get("query_params", {}) or {}).get("threshold", 0))
            cursor.execute(f"SELECT {threshold} AS seq, 'cfg' AS label FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app3"))
    log = Logger(name="test.oracle.collector.cfg", log_base=str(tmp_path / "logs" / "oracle-collector-cfg"), console=False)
    manager = _make_manager(
        log,
        {
            "DB_MAIN": {
                "user": "u1",
                "password": "p1",
                "tns": "main:1521/ORCL",
                "pool": {"enabled": True, "min": 1, "max": 2, "increment": 1, "threaded": True, "getmode": "wait"},
            }
        },
    )
    collector = OracleCollector(
        app_config=AppConfig({"collectors": {"oracle": {"db": "DB_MAIN", "query_params": {"threshold": 77}}}}),
        store=store,
        logger=log,
        connection_manager=manager,
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [ConfigAwareJob()])
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 1
        assert "SELECT 77 AS seq, 'cfg' AS label FROM dual" in executed_sql
    finally:
        collector.stop()
        manager.close_all()
        store.close()
        log.stop()


def test_oracle_collector_continues_after_job_error(tmp_path, monkeypatch) -> None:
    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "ok")]

        def close(self):
            pass

    class FakeConnection:
        def __init__(self, _dsn: str) -> None:
            pass

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    _install_fake_cx_oracle(monkeypatch, lambda dsn: FakeConnection(dsn))

    class FailingJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_fail", table="t_fail", db="DB_MAIN")

        def query(self, cfg: dict, cursor) -> list[dict]:
            raise RuntimeError("boom")

    class SuccessJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_ok", table="t_ok", db="DB_MAIN")

        def query(self, cfg: dict, cursor) -> list[dict]:
            cursor.execute("SELECT 1 AS seq, 'ok' AS label FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app4"))
    log = Logger(name="test.oracle.collector.continue", log_base=str(tmp_path / "logs" / "oracle-collector-continue"), console=False)
    manager = _make_manager(
        log,
        {
            "DB_MAIN": {
                "user": "u1",
                "password": "p1",
                "tns": "main:1521/ORCL",
                "pool": {"enabled": True, "min": 1, "max": 2, "increment": 1, "threaded": True, "getmode": "wait"},
            }
        },
    )
    collector = OracleCollector(
        app_config=AppConfig({"collectors": {"oracle": {"db": "DB_MAIN"}}}),
        store=store,
        logger=log,
        connection_manager=manager,
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [FailingJob(), SuccessJob()])
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    logged: list[str] = []
    original_exception = collector.logger.exception

    def wrapped_exception(msg, *args, **kwargs):
        logged.append(msg % args if args else str(msg))
        return original_exception(msg, *args, **kwargs)

    collector.logger.exception = wrapped_exception  # type: ignore[method-assign]

    try:
        results = collector.collect()
        assert len(results) == 1
        assert results[0][0] == "t_ok"
        assert any("job=job_fail" in m for m in logged)
    finally:
        collector.stop()
        manager.close_all()
        store.close()
        log.stop()


def test_oracle_collector_cursor_acquire_timeout(tmp_path, monkeypatch) -> None:
    class TimeoutSessionPool:
        def __init__(
            self,
            *,
            user,
            password,
            dsn,
            min,
            max,
            increment,
            threaded,
            getmode,
        ) -> None:
            pass

        def acquire(self):
            raise RuntimeError("pool is busy")

        def release(self, _conn) -> None:
            return None

        def close(self) -> None:
            return None

    fake_module = SimpleNamespace(
        connect=lambda dsn, threaded=True: None,
        SessionPool=TimeoutSessionPool,
        SPOOL_ATTRVAL_WAIT=1,
        SPOOL_ATTRVAL_NOWAIT=2,
    )
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)
    monkeypatch.setattr("src.oracleconnectionmanager.get_cx_oracle", lambda: fake_module)

    class TimeoutJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_timeout", table="t_timeout", db="DB_MAIN")

        def query(self, cfg: dict, cursor) -> list[dict]:
            return [{"id": "1"}]

    store = Store(str(tmp_path / "app5"))
    log = Logger(name="test.oracle.collector.timeout", log_base=str(tmp_path / "logs" / "oracle-collector-timeout"), console=False)
    manager = _make_manager(
        log,
        {
            "DB_MAIN": {
                "user": "u1",
                "password": "p1",
                "tns": "main:1521/ORCL",
                "cursor_acquire_timeout_sec": 0.1,
                "pool": {"enabled": True, "min": 1, "max": 1, "increment": 1, "threaded": True, "getmode": "wait"},
            }
        },
    )
    collector = OracleCollector(
        app_config=AppConfig({"collectors": {"oracle": {"db": "DB_MAIN"}}}),
        store=store,
        logger=log,
        connection_manager=manager,
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [TimeoutJob()])
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    logged: list[str] = []
    original_exception = collector.logger.exception

    def wrapped_exception(msg, *args, **kwargs):
        logged.append(msg % args if args else str(msg))
        return original_exception(msg, *args, **kwargs)

    collector.logger.exception = wrapped_exception  # type: ignore[method-assign]

    try:
        results = collector.collect()
        assert results == []
        assert any("timed out" in m.lower() for m in logged)
    finally:
        collector.stop()
        manager.close_all()
        store.close()
        log.stop()