import sys
from types import SimpleNamespace

from src.oracle_driver import get_cx_oracle


def test_get_cx_oracle_prefers_oracledb_alias(monkeypatch) -> None:
    fake_oracledb = SimpleNamespace(connect=lambda *args, **kwargs: None)
    monkeypatch.setitem(sys.modules, "oracledb", fake_oracledb)
    sys.modules.pop("cx_Oracle", None)

    module = get_cx_oracle()

    assert module is fake_oracledb