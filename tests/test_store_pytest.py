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
