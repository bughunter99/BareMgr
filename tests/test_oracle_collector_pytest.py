from types import SimpleNamespace
import sys

from src.logger import Logger
import src.oracle_collector as oracle_collector_module
from src.oracle_collector import OracleCollector
from src.oracle_jobs import OracleJob
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
        },
        store=store,
        logger=log,
    )
    monkeypatch.setattr(
        oracle_collector_module,
        "ORACLE_JOBS",
        [
            OracleJob(
                name="inventory",
                table="inventory",
                sql="SELECT id, updated_at FROM inventory WHERE updated_at > :last_ts",
                use_last_ts=True,
            )
        ],
    )
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

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
    monkeypatch.setattr(
        oracle_collector_module,
        "ORACLE_JOBS",
        [
            OracleJob(name="job_main", table="t_main", sql="SELECT 1 FROM dual", db="DB_MAIN", use_last_ts=False),
            OracleJob(name="job_target", table="t_target", sql="SELECT 2 FROM dual", db="DB_TARGET", use_last_ts=False),
        ],
    )
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 2
        dsns = [dsn for dsn, _sql in executed if "SELECT" in _sql]
        assert dsns[0] == "u1/p1@main:1521/ORCL"
        assert dsns[1] == "u2/p2@target:1521/ORCL"
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_oracle_collector_applies_post_process_per_job(tmp_path, monkeypatch) -> None:
    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "raw")]

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection(dsn))
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    def post_process(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            new_row = dict(row)
            new_row["label"] = "processed"
            out.append(new_row)
        return out

    store = Store(str(tmp_path / "app3"))
    log = Logger(name="test.oracle.collector.post", log_base=str(tmp_path / "logs" / "oracle-collector-post"), console=False)

    collector = OracleCollector(
        cfg={"dsn": "dsn"},
        store=store,
        logger=log,
    )
    monkeypatch.setattr(
        oracle_collector_module,
        "ORACLE_JOBS",
        [
            OracleJob(
                name="job_post",
                table="t_post",
                sql="SELECT 1 AS seq, 'raw' AS label FROM dual",
                use_last_ts=False,
                post_process=post_process,
            )
        ],
    )
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 1
        table, rows, _jid = results[0]
        assert table == "t_post"
        assert rows[0]["label"] == "processed"
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_oracle_collector_applies_post_process_with_context(tmp_path, monkeypatch) -> None:
    received_context: dict = {}

    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "raw")]

        def close(self):
            pass

    class FakeConnection:
        def __init__(self, dsn: str) -> None:
            self._dsn = dsn

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection(dsn))
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    def post_process(rows: list[dict], context: dict) -> list[dict]:
        received_context.update(context)
        out: list[dict] = []
        for row in rows:
            new_row = dict(row)
            new_row["label"] = f"processed:{context['job_name']}:{context['db_alias']}"
            out.append(new_row)
        return out

    store = Store(str(tmp_path / "app4"))
    log = Logger(name="test.oracle.collector.post.ctx", log_base=str(tmp_path / "logs" / "oracle-collector-post-ctx"), console=False)

    collector = OracleCollector(
        cfg={"db": "DB_MAIN"},
        store=store,
        logger=log,
        db_registry={
            "DB_MAIN": {"user": "u1", "password": "p1", "tns": "main:1521/ORCL"},
        },
    )
    monkeypatch.setattr(
        oracle_collector_module,
        "ORACLE_JOBS",
        [
            OracleJob(
                name="job_post_ctx",
                table="t_post_ctx",
                sql="SELECT 1 AS seq, 'raw' AS label FROM dual",
                db="DB_MAIN",
                use_last_ts=False,
                post_process=post_process,
            )
        ],
    )
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 1
        table, rows, jid = results[0]
        assert table == "t_post_ctx"
        assert rows[0]["label"].startswith("processed:job_post_ctx:DB_MAIN")
        assert received_context["job_name"] == "job_post_ctx"
        assert received_context["table"] == "t_post_ctx"
        assert received_context["db_alias"] == "DB_MAIN"
        assert received_context["jid"] == jid
        assert received_context["collected_at"]
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_oracle_collector_applies_multiple_post_processes_in_order(tmp_path, monkeypatch) -> None:
    class FakeCursor:
        description = [("SEQ",), ("LABEL",)]

        def execute(self, sql, params=None):
            return None

        def fetchone(self):
            return ("2026-04-28",)

        def fetchall(self):
            return [("1", "raw")]

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    fake_module = SimpleNamespace(connect=lambda dsn, threaded=True: FakeConnection(dsn))
    monkeypatch.setitem(sys.modules, "cx_Oracle", fake_module)

    def step1(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            new_row = dict(row)
            new_row["label"] = "step1"
            out.append(new_row)
        return out

    def step2(rows: list[dict], context: dict) -> list[dict]:
        out: list[dict] = []
        for row in rows:
            new_row = dict(row)
            new_row["label"] = f"{new_row['label']}:{context['job_name']}"
            out.append(new_row)
        return out

    store = Store(str(tmp_path / "app5"))
    log = Logger(name="test.oracle.collector.post.chain", log_base=str(tmp_path / "logs" / "oracle-collector-post-chain"), console=False)

    collector = OracleCollector(
        cfg={"dsn": "dsn"},
        store=store,
        logger=log,
    )
    monkeypatch.setattr(
        oracle_collector_module,
        "ORACLE_JOBS",
        [
            OracleJob(
                name="job_chain",
                table="t_chain",
                sql="SELECT 1 AS seq, 'raw' AS label FROM dual",
                use_last_ts=False,
                post_processes=[step1, step2],
            )
        ],
    )
    collector._jobs = list(oracle_collector_module.ORACLE_JOBS)
    collector._last_ts = {job.name: "1970-01-01 00:00:00" for job in collector._jobs}

    try:
        results = collector.collect()
        assert len(results) == 1
        _table, rows, _jid = results[0]
        assert rows[0]["label"] == "step1:job_chain"
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_oracle_collector_uses_make_query_with_config(tmp_path, monkeypatch) -> None:
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

    class ConfigDrivenJob(OracleJob):
        def __init__(self) -> None:
            super().__init__(
                name="job_cfg",
                table="t_cfg",
                sql="SELECT 1 AS seq, 'x' AS label FROM dual",
                use_last_ts=False,
            )

        def makeQuery(self, cfg: dict) -> str:
            threshold = int((cfg.get("query_params", {}) or {}).get("threshold", 0))
            return f"SELECT {threshold} AS seq, 'cfg' AS label FROM dual"

    store = Store(str(tmp_path / "app6"))
    log = Logger(name="test.oracle.collector.makequery", log_base=str(tmp_path / "logs" / "oracle-collector-makequery"), console=False)

    collector = OracleCollector(
        cfg={"dsn": "dsn", "query_params": {"threshold": 77}},
        store=store,
        logger=log,
    )
    monkeypatch.setattr(oracle_collector_module, "ORACLE_JOBS", [ConfigDrivenJob()])
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