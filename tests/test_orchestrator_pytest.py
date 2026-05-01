import time
from pathlib import Path

from src.basecollector import BaseCollector
from src.logger import Logger
from src.apporchestrator import AppOrchestrator
from src.store import Store
from src.appconfig import AppConfig


def test_orchestrator_processing_runs_only_when_active(tmp_path: Path) -> None:
    log = Logger(name="test.orchestrator", log_base=str(tmp_path / "logs" / "orch"), console=False)
    called: list[dict] = []

    def on_processing(ctx: dict) -> None:
        called.append(ctx)

    def on_sync(_ctx: dict) -> None:
        pass

    cfg = {
        "pipeline": {
            "business": {
                "enabled": True,
                "interval_sec": 1,
                "run_on_active_only": True,
            },
        },
        "syncmanager": {
            "enabled": False,
        },
    }

    orch = AppOrchestrator(logger=log, processing_callback=on_processing, sync_callback=on_sync, app_config=AppConfig(cfg))
    try:
        orch.start()
        time.sleep(1.3)
        assert len(called) == 0

        orch.set_active(True)
        time.sleep(1.3)
        assert len(called) >= 1
    finally:
        orch.stop()
        log.stop()


def test_orchestrator_runs_immediately_on_active_transition(tmp_path: Path) -> None:
    log = Logger(name="test.orchestrator.immediate", log_base=str(tmp_path / "logs" / "orch2"), console=False)
    called: list[dict] = []

    def on_processing(ctx: dict) -> None:
        called.append(ctx)

    def on_sync(_ctx: dict) -> None:
        pass

    cfg = {
        "pipeline": {
            "business": {
                "enabled": True,
                "interval_sec": 30,
                "run_on_active_only": True,
            },
        },
        "syncmanager": {
            "enabled": False,
        },
    }

    orch = AppOrchestrator(logger=log, processing_callback=on_processing, sync_callback=on_sync, app_config=AppConfig(cfg))
    try:
        orch.start()
        time.sleep(0.2)
        assert len(called) == 0

        orch.set_active(True)
        time.sleep(0.3)
        assert len(called) >= 1
    finally:
        orch.stop()
        log.stop()


def test_orchestrator_collector_runs_immediately_on_active_transition(tmp_path: Path) -> None:
    log = Logger(name="test.orchestrator.collector.immediate", log_base=str(tmp_path / "logs" / "orch3"), console=False)
    called_at: list[float] = []
    store = Store(str(tmp_path / "app_collector"))

    class DummyCollector(BaseCollector):
        def collect(self) -> list[tuple[str, list[dict]]]:
            called_at.append(time.monotonic())
            return []

    collector = DummyCollector(
        name="dummy",
        interval_sec=30,
        store=store,
        logger=log,
    )

    def on_processing(_ctx: dict) -> None:
        pass

    def on_sync(_ctx: dict) -> None:
        pass

    cfg = {
        "pipeline": {
            "business": {
                "enabled": False,
            },
        },
        "syncmanager": {
            "enabled": False,
        },
    }

    orch = AppOrchestrator(
        logger=log,
        processing_callback=on_processing,
        sync_callback=on_sync,
        collectors=[collector],
        app_config=AppConfig(cfg),
    )
    try:
        orch.start()
        time.sleep(0.2)
        assert len(called_at) == 0

        orch.set_active(True)
        time.sleep(0.5)
        assert len(called_at) >= 1
    finally:
        orch.stop()
        store.close()
        log.stop()
