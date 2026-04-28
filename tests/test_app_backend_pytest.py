from pathlib import Path

from src.app import App
from src.failover_db import FailoverNode_db
from src.failover_zmq import FailoverNode_zmq


def _base_cfg(backend: str, base_dir: Path) -> str:
    cfg = f"""
node_id: node-test
weight: 100

failover:
  backend: {backend}
  heartbeat_interval: 1
  fail_after_missed_heartbeats: 2
  status_interval: 1
  port: 5599
  peers: []
  db:
    dsn: user/password@localhost:1521/ORCL

logging:
  log_base: {base_dir.as_posix()}/logs/app_test

sqlite:
  path: {base_dir.as_posix()}/data/app_test

replication:
  port: 5600
  peers: []

collectors:
  oracle:
    enabled: false
  splunk:
    enabled: false
"""
    return cfg


def test_app_selects_zmq_backend(tmp_path: Path) -> None:
    cfg_file = tmp_path / "cfg_zmq.yml"
    cfg_file.write_text(_base_cfg("zmq", tmp_path), encoding="utf-8")

    app = App(config_file=str(cfg_file))
    try:
        assert isinstance(app._node, FailoverNode_zmq)
    finally:
        app.stop()


def test_app_selects_db_backend(tmp_path: Path) -> None:
    cfg_file = tmp_path / "cfg_db.yml"
    cfg_file.write_text(_base_cfg("db", tmp_path), encoding="utf-8")

    app = App(config_file=str(cfg_file))
    try:
        assert isinstance(app._node, FailoverNode_db)
    finally:
        app.stop()


  def test_app_initializes_oracle_client_on_start(tmp_path: Path, monkeypatch) -> None:
    cfg_file = tmp_path / "cfg_zmq.yml"
    cfg_file.write_text(_base_cfg("zmq", tmp_path), encoding="utf-8")

    called_with: list[str] = []

    def fake_init(cfg: dict, logger=None) -> bool:
      called_with.append(str(cfg.get("node_id")))
      return True

    monkeypatch.setattr("src.app.init_oracle_client_from_config", fake_init)

    app = App(config_file=str(cfg_file))
    try:
      assert called_with == ["node-test"]
    finally:
      app.stop()


  def test_app_builds_failover_runtime_status(tmp_path: Path) -> None:
    cfg_file = tmp_path / "cfg_zmq.yml"
    cfg_file.write_text(_base_cfg("zmq", tmp_path), encoding="utf-8")

    app = App(config_file=str(cfg_file))
    try:
      status = app._build_failover_runtime_status()
      assert "main_queue=" in status
      assert "processing=" in status
      assert "sync=" in status
      assert "etc=" in status
      assert "collectors=[none]" in status
    finally:
      app.stop()
