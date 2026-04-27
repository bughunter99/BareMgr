# Weight-Based Active/Standby Failover System

Python 기반 다중 IP 가중치 기반 능동/대기 페일오버 시스템입니다.

## 설치

```bash
workon v1
pip install pyzmq
```

## 설정 (config.json)

각 노드마다 config.json을 수정:

```json
{
  "node_id": "node1",
  "weight": 100,
  "port": 5555,
  "heartbeat_interval": 2,
  "peers": [
    "192.168.1.10:5555",
    "192.168.1.11:5555"
  ]
}
```

- `node_id`: 노드 고유 ID
- `weight`: 노드의 우선순위 (높을수록 active)
- `port`: 수신 포트
- `heartbeat_interval`: 하트비트 간격 (초)
- `peers`: 다른 노드의 주소들

## 실행

```bash
workon v1
python failover.py
```

## 동작 원리

1. 각 노드는 설정된 포트에서 하트비트를 수신
2. 주기적으로 모든 피어에 자신의 가중치를 포함한 하트비트 전송
3. 리더 선출: 가장 높은 가중치를 가진 노드가 `isActive=True`
4. 가중치가 같으면 node_id 문자열 기준으로 선출

## 사용 예시

```python
from failover import FailoverNode

node = FailoverNode('config.json')
node.start()

# isActive 속성으로 현재 상태 확인
print(f"Active: {node.isActive}")
```
