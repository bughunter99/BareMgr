from src.logger import Logger
import src.splunk_collector as splunk_collector_module
from src.splunk_collector import SplunkCollector
from src.splunk_jobs import SplunkJob
from src.store import Store


def test_splunk_collector_uses_make_query_and_post_process_context(tmp_path, monkeypatch) -> None:
    captured_queries: list[str] = []

    class ConfigDrivenSplunkJob(SplunkJob):
        def __init__(self) -> None:
            super().__init__(
                name="job_cfg",
                table="splunk_cfg",
                query="search index=main",
                test_rows=10,
                post_process=self.post_process,
            )

        def makeQuery(self, cfg: dict) -> str:
            level = str((cfg.get("query_params", {}) or {}).get("level", "INFO"))
            return f"search index=main level={level} | head 1"

        @staticmethod
        def post_process(rows: list[dict], context: dict) -> list[dict]:
            out: list[dict] = []
            for row in rows:
                item = dict(row)
                item["job"] = context["job_name"]
                item["jid"] = context["jid"]
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

    monkeypatch.setattr(splunk_collector_module, "SPLUNK_JOBS", [ConfigDrivenSplunkJob()])
    collector._jobs = list(splunk_collector_module.SPLUNK_JOBS)
    monkeypatch.setattr(SplunkCollector, "_run_search", fake_run_search)

    try:
        results = collector.collect()
        assert len(results) == 1
        assert captured_queries == ["search index=main level=ERROR | head 1"]
        table, rows, jid = results[0]
        assert table == "splunk_cfg"
        assert rows[0]["job"] == "job_cfg"
        assert rows[0]["jid"] == jid
    finally:
        collector.stop()
        store.close()
        log.stop()


def test_splunk_collector_applies_multiple_post_processes_in_order(tmp_path, monkeypatch) -> None:
    class ChainedSplunkJob(SplunkJob):
        def __init__(self) -> None:
            super().__init__(
                name="job_chain",
                table="splunk_chain",
                query="search index=main | head 1",
                post_process=self.step1,
                post_processes=[self.step2],
            )

        @staticmethod
        def step1(rows: list[dict]) -> list[dict]:
            out: list[dict] = []
            for row in rows:
                item = dict(row)
                item["message"] = "step1"
                out.append(item)
            return out

        @staticmethod
        def step2(rows: list[dict], context: dict) -> list[dict]:
            out: list[dict] = []
            for row in rows:
                item = dict(row)
                item["message"] = f"{item['message']}:{context['job_name']}"
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

    monkeypatch.setattr(splunk_collector_module, "SPLUNK_JOBS", [ChainedSplunkJob()])
    collector._jobs = list(splunk_collector_module.SPLUNK_JOBS)
    monkeypatch.setattr(SplunkCollector, "_run_search", fake_run_search)

    try:
        results = collector.collect()
        assert len(results) == 1
        _table, rows, _jid = results[0]
        assert rows[0]["message"] == "step1:job_chain"
    finally:
        collector.stop()
        store.close()
        log.stop()
