#!/usr/bin/env python3
"""외부 스크립트로 failover_zmq 노드를 중지하는 예제.

사용 순서:
1) 터미널 A: python failover_zmq.py
2) 터미널 B: python failover_zmq_stop_example.py 127.0.0.1:5555
"""

import argparse

from failover_zmq import send_stop


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Example: stop failover_zmq node")
    parser.add_argument(
        "endpoint",
        nargs="?",
        default="127.0.0.1:5555",
        help="target endpoint host:port (default: 127.0.0.1:5555)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    response = send_stop(args.endpoint)
    print(f"stop response: {response}")


if __name__ == "__main__":
    main()
