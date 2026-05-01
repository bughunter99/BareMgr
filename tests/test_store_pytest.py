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


def test_scope_object_routes_to_object_sqlite(tmp_path: Path) -> None:
    store = Store(str(tmp_path / "app4"))
    try:
        store.upsert_many("inventory", [{"id": "s1"}], scope="system")
        store.upsert_many("inventory", [{"id": "o1"}], scope="object", object_name="job-001")

        sys_rows = store.query(
            'SELECT COUNT(*) AS cnt FROM "inventory"',
            table="inventory",
            scope="system",
        )
        obj_rows = store.query(
            'SELECT COUNT(*) AS cnt FROM "inventory"',
            table="inventory",
            scope="object",
            object_name="job-001",
        )

        assert int(sys_rows[0]["cnt"]) == 1
        assert int(obj_rows[0]["cnt"]) == 1
        assert (tmp_path / "app4" / "objects" / "job-001.db").exists()
    finally:
        store.close()


def test_scope_object_requires_object_name(tmp_path: Path) -> None:
    store = Store(str(tmp_path / "app5"))
    try:
        raised = False
        try:
            store.upsert_many("inventory", [{"id": "1"}], scope="object")
        except ValueError:
            raised = True
        assert raised is True
    finally:
        store.close()


def test_initialize_object_sqlite_applies_registered_type_ddl(tmp_path: Path) -> None:
    store = Store(
        str(tmp_path / "app6"),
        object_sqlite_types=[
            {"type": "AAA", "ddl_file": "ddl/object_aaa.sql"},
            {"type": "BBB", "ddl_file": "ddl/object_bbb.sql"},
        ],
    )
    try:
        db_path = store.initialize_object_sqlite("obj-001", "AAA")
        assert db_path.exists()

        rows = store.query(
            "SELECT COUNT(*) AS cnt FROM sqlite_master WHERE type='table' AND name='object_profile'",
            scope="object",
            object_name="obj-001",
        )
        assert int(rows[0]["cnt"]) == 1
    finally:
        store.close()


def test_initialize_object_sqlite_requires_registered_type(tmp_path: Path) -> None:
    store = Store(
        str(tmp_path / "app7"),
        object_sqlite_types=[
            {"type": "AAA", "ddl_file": "ddl/object_aaa.sql"},
        ],
    )
    try:
        raised = False
        try:
            store.initialize_object_sqlite("obj-002", "BBB")
        except ValueError:
            raised = True
        assert raised is True
    finally:
        store.close()


def test_finalize_object_sqlite_moves_file_to_done_dir(tmp_path: Path) -> None:
    """finalize 후 objects/done/ 으로 파일이 이동하고 원본은 사라진다."""
    store = Store(
        str(tmp_path / "app8"),
        object_sqlite_types=[{"type": "AAA", "ddl_file": "ddl/object_aaa.sql"}],
    )
    try:
        store.initialize_object_sqlite("obj-100", "AAA")
        store.upsert_many("object_profile", [{"obj_id": "obj-100", "source_table": "t1", "payload_json": "{}"}],
                          scope="object", object_name="obj-100")

        src_path = tmp_path / "app8" / "objects" / "obj-100.db"
        assert src_path.exists()

        dest_path = store.finalize_object_sqlite("obj-100")

        assert dest_path.exists()
        assert not src_path.exists()
        assert dest_path.parent == tmp_path / "app8" / "objects" / "done"
        assert dest_path.name == "obj-100.db"
    finally:
        store.close()


def test_finalize_object_sqlite_custom_dest_dir(tmp_path: Path) -> None:
    """dest_dir 인수로 이동 경로를 직접 지정할 수 있다."""
    custom_done = tmp_path / "archive"
    store = Store(
        str(tmp_path / "app9"),
        object_sqlite_types=[{"type": "BBB", "ddl_file": "ddl/object_bbb.sql"}],
    )
    try:
        store.initialize_object_sqlite("obj-200", "BBB")

        dest_path = store.finalize_object_sqlite("obj-200", dest_dir=str(custom_done))

        assert dest_path.parent == custom_done
        assert dest_path.exists()
    finally:
        store.close()


def test_finalize_object_sqlite_uses_config_done_dir(tmp_path: Path) -> None:
    """config의 object_sqlite_done_dir 이 기본 이동 경로로 사용된다."""
    configured_done = tmp_path / "configured_done"
    store = Store(
        str(tmp_path / "app10"),
        object_sqlite_types=[{"type": "AAA", "ddl_file": "ddl/object_aaa.sql"}],
        object_sqlite_done_dir=str(configured_done),
    )
    try:
        store.initialize_object_sqlite("obj-300", "AAA")

        dest_path = store.finalize_object_sqlite("obj-300")

        assert dest_path.parent == configured_done
        assert dest_path.exists()
    finally:
        store.close()


def test_finalize_object_sqlite_clears_connection_cache(tmp_path: Path) -> None:
    """finalize 후 해당 객체 write 연결이 캐시에서 제거된다."""
    store = Store(
        str(tmp_path / "app11"),
        object_sqlite_types=[{"type": "AAA", "ddl_file": "ddl/object_aaa.sql"}],
    )
    try:
        store.initialize_object_sqlite("obj-400", "AAA")
        conn_key = store._get_db_key_for_object("obj-400")
        assert conn_key in store._write_conns

        store.finalize_object_sqlite("obj-400")

        assert conn_key not in store._write_conns
    finally:
        store.close()
