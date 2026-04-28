#!/usr/bin/env python3
"""Config-driven stop command sender for failover_zmq backend."""

from __future__ import annotations

import argparse
import json
import sys

from .config_loader import load_config
from .failover_zmq import send_stop


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send ZMQ stop command by config")
    parser.add_argument("config", help="config file path (.json/.yml/.yaml)")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=3000,
        help="request timeout in milliseconds (default: 3000)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        cfg = load_config(args.config)
        failover = _as_dict(cfg.get("failover"))
        host = str(failover.get("host", "127.0.0.1"))
        port = int(failover.get("port", 5555))
        endpoint = f"{host}:{port}"
        response = send_stop(endpoint, timeout_ms=args.timeout_ms)
        print(json.dumps(response, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"stop request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
