import time
from pathlib import Path

from src.logger import Logger
from src.apporchestrator import AppOrchestrator


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

    orch = AppOrchestrator(cfg=cfg, logger=log, processing_callback=on_processing, sync_callback=on_sync)
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

    orch = AppOrchestrator(cfg=cfg, logger=log, processing_callback=on_processing, sync_callback=on_sync)
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
