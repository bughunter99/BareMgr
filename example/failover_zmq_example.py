#!/usr/bin/env python3
"""failovernodezmq.py 사용 예제

실행 전 준비:
1) config.json의 node_id/weight/peers/port를 노드별로 설정
2) 여러 터미널에서 각 노드를 실행
"""

from src.failovernodezmq import FailoverNode
from src.logger import Logger


def main() -> None:
    logger = Logger(name="failover-demo", log_base="logs/failover_demo")

    node = FailoverNode(config_file="config.json", logger=logger)
    try:
        node.start()
    finally:
        node.stop()


if __name__ == "__main__":
    main()