import threading
import time
from pathlib import Path

from src.store import Store


def test_store_creates_one_sqlite_per_table(tmp_path: Path) -> None:
    base_dir = tmp_path / "app1"
    store = Store(str(base_dir))
    try:
        inserted_inv = store.upsert_many(
            "inventory",
            [{"id": "1", "name": "n1"}, {"id": "2", "name": "n2"}],
        )
        inserted_ord = store.upsert_many(
            "orders",
            [{"order_id": "o1", "status": "NEW"}],
        )

        assert inserted_inv == 2
        assert inserted_ord == 1

        assert (base_dir / "inventory.db").exists()
        assert (base_dir / "orders.db").exists()

        inv_rows = store.query('SELECT COUNT(*) AS cnt FROM "inventory"', table="inventory")
        ord_rows = store.query('SELECT COUNT(*) AS cnt FROM "orders"', table="orders")

        assert inv_rows[0]["cnt"] == 2
        assert ord_rows[0]["cnt"] == 1
    finally:
        store.close()


def test_query_requires_table_when_multiple_dbs(tmp_path: Path) -> None:
    store = Store(str(tmp_path / "app2"))
    try:
        store.upsert_many("t1", [{"a": "1"}])
        store.upsert_many("t2", [{"a": "2"}])

        try:
            store.query('SELECT COUNT(*) AS cnt FROM "t1"')
            raised = False
        except ValueError:
            raised = True

        assert raised is True
    finally:
        store.close()


def test_query_can_run_while_write_is_in_progress(tmp_path: Path) -> None:
    store = Store(str(tmp_path / "app3"))
    original_upsert_rows = store._upsert_rows
    write_started = threading.Event()
    allow_commit = threading.Event()

    try:
        store.upsert_many("inventory", [{"id": "1", "name": "n1"}])

        def delayed_upsert(cursor, table, rows, *, conn_key):
            original_upsert_rows(cursor, table, rows, conn_key=conn_key)
            write_started.set()
            assert allow_commit.wait(timeout=2.0)

        store._upsert_rows = delayed_upsert

        writer = threading.Thread(
            target=store.upsert_many,
            args=("inventory", [{"id": "2", "name": "n2"}]),
        )
        writer.start()

        assert write_started.wait(timeout=1.0)

        result_holder: dict[str, int] = {}

        def run_query() -> None:
            rows = store.query('SELECT COUNT(*) AS cnt FROM "inventory"', table="inventory")
            result_holder["cnt"] = int(rows[0]["cnt"])

        reader = threading.Thread(target=run_query)
        started_at = time.perf_counter()
        reader.start()
        reader.join(timeout=1.0)
        elapsed = time.perf_counter() - started_at

        assert reader.is_alive() is False
        assert elapsed < 1.0
        assert result_holder["cnt"] == 1

        allow_commit.set()
        writer.join(timeout=1.0)
        assert writer.is_alive() is False

        final_rows = store.query('SELECT COUNT(*) AS cnt FROM "inventory"', table="inventory")
        assert int(final_rows[0]["cnt"]) == 2
    finally:
        allow_commit.set()
        store._upsert_rows = original_upsert_rows
        store.close()
