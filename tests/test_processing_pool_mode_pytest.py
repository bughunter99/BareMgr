from pathlib import Path

from src.logger import Logger
from src.businessmanager import ProcessingPipeline
from src.store import Store


def test_processing_switches_to_thread_when_worker_lookup_enabled(tmp_path: Path) -> None:
    store = Store(str(tmp_path / "app1"))
    logger = Logger(name="test.processing.pool", log_base=str(tmp_path / "logs" / "pool"), console=False)

    cfg = {
        "node_id": "node-test",
        "collectors": {
            "oracle": {"jobs": [{"table": "inventory"}]},
        },
        "pipeline": {
            "processing": {
                "worker_mode": "multiprocessing",
                "workers": 2,
                "batch_min": 3,
                "batch_max": 3,
                "output_table": "pipeline_generated_objects",
                "input_tables": ["inventory"],
                "oracle_pool": {
                    "enabled": False,
                },
                "worker_oracle_lookup": {
                    "enabled": True,
                    "sql": "SELECT 1 FROM dual WHERE 1=:lookup_key",
                    "bind_key": "lookup_key",
                    "object_field": "batch_index",
                },
                "drone": {"enabled": False},
                "oracle_write": {"enabled": False},
            }
        },
    }

    store.upsert_many("inventory", [{"id": "1"}])

    pipeline = ProcessingPipeline(cfg=cfg, store=store, logger=logger)
    try:
        pipeline.run({"job_name": "processing"})
        rows = store.query(
            'SELECT COUNT(*) AS cnt FROM "pipeline_generated_objects"',
            table="pipeline_generated_objects",
        )
        assert int(rows[0]["cnt"]) == 3
    finally:
        pipeline.close()
        store.close()
        logger.stop()
