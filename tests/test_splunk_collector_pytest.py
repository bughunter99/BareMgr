from src.logger import Logger
import src.splunk_collector as splunk_collector_module
from src.splunk_collector import SplunkCollector
from src.splunk_jobs import SplunkJob
from src.store import Store


def test_splunk_collector_runs_job_query_method(tmp_path, monkeypatch) -> None:
    captured_queries: list[str] = []

    class QueryOnlySplunkJob(SplunkJob):
        def __init__(self) -> None:
            super().__init__(
                name="job_query",
                table="splunk_query",
                test_rows=10,
            )

        def query(self, cfg: dict, service, run_search) -> list[dict]:
            level = str((cfg.get("query_params", {}) or {}).get("level", "INFO"))
            rows = run_search(service, f"search index=main level={level} | head 1", self.name)
            out: list[dict] = []
            for row in rows:
                item = dict(row)
                item["job"] = self.name
                out.append(item)
            return out

    def fake_run_search(self, service, query: str, job_name: str) -> list[dict]:
        captured_queries.append(query)
        return [{"host": "h1", "message": "m1"}]

    store = Store(str(tmp_path / "app1"))
    log = Logger(name="test.splunk.collector", log_base=str(tmp_path / "logs" / "splunk-collector"), console=False)
    collector = SplunkCollector(
        cfg={
            "base_url": "http://local",
            "splunk_token": "t",
            "query_params": {"level": "ERROR"},
        },
        store=store,
        logger=log,
    )

    monkeypatch.setattr(splunk_collector_module, "SPLUNK_JOBS", [QueryOnlySplunkJob()])
    collector._jobs = list(splunk_collector_module.SPLUNK_JOBS)
    monkeypatch.setattr(SplunkCollector, "_run_search", fake_run_search)

    try:
        results = collector.collect()
        assert len(results) == 1
        assert captured_queries == ["search index=main level=ERROR | head 1"]
        table, rows, jid = results[0]
        assert table == "splunk_query"
        assert rows[0]["job"] == "job_query"
        assert jid
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_splunk_collector_job_query_handles_row_processing(tmp_path, monkeypatch) -> None:
    class ProcessingInsideQueryJob(SplunkJob):
        def __init__(self) -> None:
            super().__init__(
                name="job_process",
                table="splunk_process",
            )

        def query(self, cfg: dict, service, run_search) -> list[dict]:
            rows = run_search(service, "search index=main | head 1", self.name)
            out: list[dict] = []
            for row in rows:
                item = dict(row)
                item["message"] = f"processed:{item['message']}"
                out.append(item)
            return out

    def fake_run_search(self, service, query: str, job_name: str) -> list[dict]:
        return [{"host": "h1", "message": "raw"}]

    store = Store(str(tmp_path / "app2"))
    log = Logger(name="test.splunk.collector.chain", log_base=str(tmp_path / "logs" / "splunk-collector-chain"), console=False)
    collector = SplunkCollector(
        cfg={"base_url": "http://local", "splunk_token": "t"},
        store=store,
        logger=log,
    )

    monkeypatch.setattr(splunk_collector_module, "SPLUNK_JOBS", [ProcessingInsideQueryJob()])
    collector._jobs = list(splunk_collector_module.SPLUNK_JOBS)
    monkeypatch.setattr(SplunkCollector, "_run_search", fake_run_search)

    try:
        results = collector.collect()
        assert len(results) == 1
        _table, rows, _jid = results[0]
        assert rows[0]["message"] == "processed:raw"
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_splunk_collector_continues_after_job_error(tmp_path, monkeypatch) -> None:
    class FailingSplunkJob(SplunkJob):
        def __init__(self) -> None:
            super().__init__(name="job_fail", table="splunk_fail")

        def query(self, cfg: dict, service, run_search) -> list[dict]:
            raise RuntimeError("boom")

    class SuccessSplunkJob(SplunkJob):
        def __init__(self) -> None:
            super().__init__(name="job_ok", table="splunk_ok")

        def query(self, cfg: dict, service, run_search) -> list[dict]:
            return [{"host": "h1", "message": "ok"}]

    store = Store(str(tmp_path / "app3"))
    log = Logger(name="test.splunk.collector.continue", log_base=str(tmp_path / "logs" / "splunk-collector-continue"), console=False)
    collector = SplunkCollector(
        cfg={"base_url": "http://local", "splunk_token": "t"},
        store=store,
        logger=log,
    )

    monkeypatch.setattr(splunk_collector_module, "SPLUNK_JOBS", [FailingSplunkJob(), SuccessSplunkJob()])
    collector._jobs = list(splunk_collector_module.SPLUNK_JOBS)

    logged: list[str] = []
    original_exception = collector.logger.exception

    def wrapped_exception(msg, *args, **kwargs):
        logged.append(msg % args if args else str(msg))
        return original_exception(msg, *args, **kwargs)

    collector.logger.exception = wrapped_exception  # type: ignore[method-assign]

    try:
        results = collector.collect()
        assert len(results) == 1
        assert results[0][0] == "splunk_ok"
        assert any("job=job_fail" in m for m in logged)
    finally:
        collector.stop()
        store.close()
        log.stop()
