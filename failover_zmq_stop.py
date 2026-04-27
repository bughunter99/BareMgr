#!/usr/bin/env python3
"""실행 중인 failover_zmq 노드에 stop 명령 전송 스크립트."""

import argparse
import json
import sys

from failover_zmq import send_stop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send stop command to failover_zmq node")
    parser.add_argument(
        "endpoint",
        nargs="?",
        default="127.0.0.1:5555",
        help="target endpoint host:port (default: 127.0.0.1:5555)",
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
        response = send_stop(args.endpoint, timeout_ms=args.timeout_ms)
        print(json.dumps(response, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"stop request failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
