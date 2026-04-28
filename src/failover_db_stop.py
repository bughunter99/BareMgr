#!/usr/bin/env python3
"""실행 중인 failover_db 노드에 DB stop 명령을 전송하는 스크립트."""

import argparse
import json
import sys

from .failover_db import send_stop_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send stop command to failover_db node")
    parser.add_argument("dsn", help="Oracle DSN, e.g. user/password@host:1521/ORCL")
    parser.add_argument("node_id", help="target node_id")
    parser.add_argument("--table", default="FAILOVER_COMMANDS", help="command table name")
    parser.add_argument("--node-id-column", default="NODE_ID", help="node id column name")
    parser.add_argument("--command-column", default="COMMAND", help="command column name")
    parser.add_argument("--created-at-column", default="CREATED_AT", help="created_at column name")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        response = send_stop_db(
            dsn=args.dsn,
            node_id=args.node_id,
            table=args.table,
            node_id_column=args.node_id_column,
            command_column=args.command_column,
            created_at_column=args.created_at_column,
        )
        print(json.dumps(response, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"db stop request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
