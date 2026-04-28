#!/usr/bin/env python3
"""Config-aware failover stop dispatcher.

Loads config and routes stop command to failover_zmq_stop or failover_db_stop logic.
"""

from __future__ import annotations

import argparse
import json
import sys

from .config_loader import load_config
from .failover_db import send_stop_db
from .failover_zmq import send_stop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send graceful stop by config failover.backend")
    parser.add_argument("config", help="config file path (.json/.yml/.yaml)")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=3000,
        help="request timeout for zmq backend in milliseconds (default: 3000)",
    )
    return parser.parse_args()


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _load_stop_params(config_path: str) -> dict[str, str]:
    cfg = load_config(config_path)
    failover = _as_dict(cfg.get("failover"))
    db = _as_dict(failover.get("db"))
    command = _as_dict(db.get("command"))
    cmd_cols = _as_dict(command.get("columns"))
    collectors = _as_dict(cfg.get("collectors"))
    oracle_col = _as_dict(collectors.get("oracle"))

    return {
        "backend": str(failover.get("backend", "zmq")).lower(),
        "node_id": str(cfg.get("node_id", "")),
        "port": str(failover.get("port", 5555)),
        "dsn": str(db.get("dsn") or oracle_col.get("dsn", "")),
        "table": str(command.get("table", "FAILOVER_COMMANDS")),
        "node_id_column": str(cmd_cols.get("node_id", "NODE_ID")),
        "command_column": str(cmd_cols.get("command", "COMMAND")),
        "created_at_column": str(cmd_cols.get("created_at", "CREATED_AT")),
    }


def main() -> int:
    args = parse_args()

    try:
        params = _load_stop_params(args.config)
    except Exception as exc:
        print(f"config parse failed: {exc}", file=sys.stderr)
        return 1

    backend = params["backend"]

    try:
        if backend == "zmq":
            endpoint = f"127.0.0.1:{params['port']}"
            response = send_stop(endpoint, timeout_ms=args.timeout_ms)
            print(json.dumps({"backend": "zmq", "endpoint": endpoint, "response": response}, ensure_ascii=False))
            return 0

        if backend == "db":
            dsn = params["dsn"]
            node_id = params["node_id"]
            if not dsn:
                print("db backend requires dsn", file=sys.stderr)
                return 1
            if not node_id:
                print("db backend requires node_id", file=sys.stderr)
                return 1

            response = send_stop_db(
                dsn=dsn,
                node_id=node_id,
                table=params["table"],
                node_id_column=params["node_id_column"],
                command_column=params["command_column"],
                created_at_column=params["created_at_column"],
            )
            print(json.dumps({"backend": "db", "node_id": node_id, "response": response}, ensure_ascii=False))
            return 0

        print(f"unsupported backend: {backend}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"stop request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
