# Weight-Based Active/Standby Failover App

여러 서버(또는 로컬 다중 프로세스)에서 동일 코드를 실행하고,
가중치 기반으로 1개 노드만 Active가 되어 수집 작업을 수행하는 시스템입니다.

Active 노드는 수집한 데이터를 로컬 SQLite에 저장하고 Standby 노드에도 복제합니다.

## 설치

```bash
pip install -r requirements.txt
```

## 설정 파일 형식 (YAML)

앱은 JSON/YAML 모두 지원하지만, 기본은 YAML입니다.

샘플 파일:
- config_app_node1.yml
- config_app_node2.yml
- config_app_node3.yml

핵심 섹션:
- node_id, weight: 리더 선출 기준
- failover: heartbeat 포트/주기/피어
- replication: Active → Standby 복제 포트/피어
- sqlite: 로컬 DB 디렉토리 경로 (테이블별 .db 파일 생성)
- collectors: oracle/splunk 수집 설정

예시:

```yaml
node_id: node1
weight: 100

failover:
  port: 5555
  heartbeat_interval: 2
  peers:
    - 127.0.0.1:5557
    - 127.0.0.1:5559

logging:
  log_base: logs/app_node1

sqlite:
  path: data/app1

replication:
  port: 5556
  peers:
    - 127.0.0.1:5558
    - 127.0.0.1:5560

collectors:
  oracle:
    enabled: true
    interval_sec: 60
    dsn: user/password@192.168.1.100:1521/ORCL
    test:
      enabled: true
      rows: 5000
      emit_once: true
    jobs:
      - name: inventory
        sql: SELECT id, name, qty, updated_at FROM inventory WHERE updated_at > :last_ts
        table: inventory
```

## Oracle 없는 테스트 모드 (요청 반영)

Oracle DB가 없을 때 아래 플래그를 켜면,
OracleCollector가 실제 DB 대신 임의 list of dict 5000건을 생성해 수집한 것처럼 처리합니다.

```yaml
collectors:
  oracle:
    enabled: true
    test:
      enabled: true
      rows: 5000
      emit_once: true
```

- enabled: 테스트 모드 on/off
- rows: 생성할 더미 row 수
- emit_once: true면 1회만 생성, false면 주기마다 생성

생성된 데이터는 일반 수집 데이터와 동일하게:
1) Active 로컬 SQLite 저장
2) Standby로 복제 전송
3) Standby SQLite 저장

SQLite 저장 구조:
- `sqlite.path`를 폴더로 지정하면, 테이블마다 별도 파일이 생성됩니다.
- 예: `sqlite.path: data/app1` 이고 테이블이 `inventory`, `orders`면
  `data/app1/inventory.db`, `data/app1/orders.db`가 생성됩니다.

## 3노드 로컬 테스트

터미널 3개에서 각각 실행:

```bash
python main.py --config config_app_node1.yml
python main.py --config config_app_node2.yml
python main.py --config config_app_node3.yml
```

가중치(node1=100, node2=90, node3=80) 기준으로 node1이 Active가 됩니다.

## Active 종료 테스트

Active 노드의 failover 포트로 stop 명령을 보내면 graceful shutdown됩니다.

```bash
python failover_zmq_stop.py 127.0.0.1:5555
```

수초 내 Standby 노드 중 최고 가중치 노드가 Active로 승격됩니다.

## 실행 진입점

```bash
python main.py --config config_app_node1.yml
```
