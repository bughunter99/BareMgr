from pathlib import Path

from src.syncmanager import SyncCheckpointStore


def test_sync_checkpoint_store_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "sync_checkpoint.db"
    store = SyncCheckpointStore(str(db_path))

    assert store.get("oracle_to_oracle", "INVENTORY") is None

    store.set("oracle_to_oracle", "INVENTORY", "2026-04-27 00:00:00.000000")
    assert store.get("oracle_to_oracle", "INVENTORY") == "2026-04-27 00:00:00.000000"

    store.set("oracle_to_oracle", "INVENTORY", "2026-04-27 00:01:00.000000")
    assert store.get("oracle_to_oracle", "INVENTORY") == "2026-04-27 00:01:00.000000"
