from types import SimpleNamespace
import sys

from src.logger import Logger
import src.oracle_collector as oracle_collector_module
from src.oracle_collector import OracleCollector
from src.oracle_jobs import OracleJob
from src.oracle_utils import makeDictFactory
from src.store import Store


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
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection())
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    class JobQueryOnly(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_q", table="t_q", db="", test_rows=1)

        def query(self, cfg: dict, cursor) -> list[dict]:
            cursor.execute("SELECT 7 AS seq, 'ok' AS label FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.oracle.collector.query", log_base=str(tmp_path / "logs" / "oracle-collector-query"), console=False)
    collector = OracleCollector(cfg={"dsn": "dsn"}, store=store, logger=log)
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

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection(dsn))
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

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
    collector = OracleCollector(
        cfg={"db": "DB_MAIN"},
        store=store,
        logger=log,
        db_registry={
            "DB_MAIN": {"user": "u1", "password": "p1", "tns": "main:1521/ORCL"},
            "DB_TARGET": {"user": "u2", "password": "p2", "tns": "target:1521/ORCL"},
        },
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [JobMain(), JobTarget()])
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 2
        dsns = [dsn for dsn, sql in executed if "SELECT" in sql]
        assert dsns[0] == "u1/p1@main:1521/ORCL"
        assert dsns[1] == "u2/p2@target:1521/ORCL"
    finally:
        collector.stop()
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
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection())
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    class ConfigAwareJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_cfg", table="t_cfg")

        def query(self, cfg: dict, cursor) -> list[dict]:
            threshold = int((cfg.get("query_params", {}) or {}).get("threshold", 0))
            cursor.execute(f"SELECT {threshold} AS seq, 'cfg' AS label FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app3"))
    log = Logger(name="test.oracle.collector.cfg", log_base=str(tmp_path / "logs" / "oracle-collector-cfg"), console=False)
    collector = OracleCollector(
        cfg={"dsn": "dsn", "query_params": {"threshold": 77}},
        store=store,
        logger=log,
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
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection())
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    class FailingJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_fail", table="t_fail")

        def query(self, cfg: dict, cursor) -> list[dict]:
            raise RuntimeError("boom")

    class SuccessJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(name="job_ok", table="t_ok")

        def query(self, cfg: dict, cursor) -> list[dict]:
            cursor.execute("SELECT 1 AS seq, 'ok' AS label FROM dual")
            cursor.rowfactory = makeDictFactory(cursor)
            return list(cursor.fetchall())

    store = Store(str(tmp_path / "app4"))
    log = Logger(name="test.oracle.collector.continue", log_base=str(tmp_path / "logs" / "oracle-collector-continue"), console=False)
    collector = OracleCollector(cfg={"dsn": "dsn"}, store=store, logger=log)
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
        store.close()
        log.stop()