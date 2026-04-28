from src.oracle_utils import makeDictFactory


def test_make_dict_factory_uses_cursor_description_lowercase() -> None:
    class FakeCursor:
        description = [("ID",), ("UPDATED_AT",), ("NAME",)]

    factory = makeDictFactory(FakeCursor())

    row = factory("1", "2026-04-28 00:00:00", "item")

    assert row == {
        "id": "1",
        "updated_at": "2026-04-28 00:00:00",
        "name": "item",
    }