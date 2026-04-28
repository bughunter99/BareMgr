#!/usr/bin/env python3
"""실행 중인 failover_zmq 노드에 stop 명령 전송 스크립트."""

import argparse
import json
import sys
from pathlib import Path

from .config_loader import load_config
from .failover_zmq import send_stop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send stop command to failover_zmq node")
    parser.add_argument(
        "endpoint_or_config",
        nargs="?",
        default="",
        help="target endpoint host:port OR config file path",
    )
    parser.add_argument(
        "--config",
        default="",
        help="config file path (.json/.yml/.yaml)",
    )
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
        endpoint = args.endpoint_or_config or "127.0.0.1:5555"
        config_path = args.config.strip()

        if config_path:
            cfg = load_config(config_path)
            failover = cfg.get("failover", {}) or {}
            host = str(failover.get("host", "127.0.0.1"))
            port = int(failover.get("port", 5555))
            endpoint = f"{host}:{port}"
        elif args.endpoint_or_config:
            p = Path(args.endpoint_or_config)
            if p.is_file() and p.suffix.lower() in {".json", ".yml", ".yaml"}:
                cfg = load_config(args.endpoint_or_config)
                failover = cfg.get("failover", {}) or {}
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
