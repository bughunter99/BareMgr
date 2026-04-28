#!/usr/bin/env python3
"""
main.py — 실행 진입점.

사용법:
    python main.py                               # config_app_node1.yml 사용
    python main.py --config config_app_node2.yml
"""

import argparse

from src.app import App


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Active/Standby failover app")
    parser.add_argument(
        "--config",
        default="config_app_node1.yml",
        help="config file path (default: config_app_node1.yml)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    App(config_file=args.config).start()
