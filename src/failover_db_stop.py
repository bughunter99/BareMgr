#!/usr/bin/env python3
"""실행 중인 failover_db 노드에 DB stop 명령을 전송하는 스크립트."""

import argparse
import json
import sys
from pathlib import Path

from .config_loader import load_config
from .failovernodedb import send_stop_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send stop command to failover_db node")
    parser.add_argument(
        "arg1",
        nargs="?",
        default="",
        help="config file path OR Oracle DSN, e.g. user/password@host:1521/ORCL",
    )
    parser.add_argument("node_id", nargs="?", default="", help="target node_id (dsn mode only)")
    parser.add_argument("--config", default="", help="config file path (.json/.yml/.yaml)")
    parser.add_argument("--table", default="FAILOVER_COMMANDS", help="command table name")
    parser.add_argument("--node-id-column", default="NODE_ID", help="node id column name")
    parser.add_argument("--command-column", default="COMMAND", help="command column name")
    parser.add_argument("--created-at-column", default="CREATED_AT", help="created_at column name")
    return parser.parse_args()


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _load_from_config(config_path: str) -> tuple[str, str, str, str, str, str]:
    cfg = load_config(config_path)
    failover = _as_dict(cfg.get("failover"))
    db = _as_dict(failover.get("db"))
    command = _as_dict(db.get("command"))
    cmd_cols = _as_dict(command.get("columns"))
    collectors = _as_dict(cfg.get("collectors"))
    oracle_col = _as_dict(collectors.get("oracle"))

    dsn = str(db.get("dsn") or oracle_col.get("dsn", ""))
    node_id = str(cfg.get("node_id", ""))
    table = str(command.get("table", "FAILOVER_COMMANDS"))
    node_id_column = str(cmd_cols.get("node_id", "NODE_ID"))
    command_column = str(cmd_cols.get("command", "COMMAND"))
    created_at_column = str(cmd_cols.get("created_at", "CREATED_AT"))
    return dsn, node_id, table, node_id_column, command_column, created_at_column


def main() -> int:
    args = parse_args()
    try:
        config_path = args.config.strip()
        if not config_path and args.arg1:
            p = Path(args.arg1)
            if p.is_file() and p.suffix.lower() in {".json", ".yml", ".yaml"}:
                config_path = args.arg1

        if config_path:
            dsn, node_id, table, node_id_column, command_column, created_at_column = _load_from_config(config_path)
        else:
            dsn, node_id = args.arg1, args.node_id
            table = args.table
            node_id_column = args.node_id_column
            command_column = args.command_column
            created_at_column = args.created_at_column

        if not dsn or not node_id:
            raise ValueError("dsn/node_id missing (provide config or dsn+node_id)")

        response = send_stop_db(
            dsn=dsn,
            node_id=node_id,
            table=table,
            node_id_column=node_id_column,
            command_column=command_column,
            created_at_column=created_at_column,
        )
        print(json.dumps(response, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"db stop request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
