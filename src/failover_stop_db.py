#!/usr/bin/env python3
"""Config-driven DB stop command sender for failover_db backend."""

from __future__ import annotations

import argparse
import json
import sys

from .config_loader import load_config
from .failover_db import send_stop_db


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send DB stop command by config")
    parser.add_argument("config", help="config file path (.json/.yml/.yaml)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        cfg = load_config(args.config)
        failover = _as_dict(cfg.get("failover"))
        db = _as_dict(failover.get("db"))
        command = _as_dict(db.get("command"))
        cmd_cols = _as_dict(command.get("columns"))
        collectors = _as_dict(cfg.get("collectors"))
        oracle_col = _as_dict(collectors.get("oracle"))

        dsn = str(db.get("dsn") or oracle_col.get("dsn", ""))
        node_id = str(cfg.get("node_id", ""))
        if not dsn or not node_id:
            print("db stop skipped: dsn or node_id missing in config", file=sys.stderr)
            return 1

        response = send_stop_db(
            dsn=dsn,
            node_id=node_id,
            table=str(command.get("table", "FAILOVER_COMMANDS")),
            node_id_column=str(cmd_cols.get("node_id", "NODE_ID")),
            command_column=str(cmd_cols.get("command", "COMMAND")),
            created_at_column=str(cmd_cols.get("created_at", "CREATED_AT")),
        )
        print(json.dumps(response, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"db stop request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
