# Weight-Based Active/Standby Failover App

여러 서버(또는 로컬 다중 프로세스)에서 동일 코드를 실행하고,
가중치 기반으로 1개 노드만 Active가 되어 수집 작업을 수행하는 시스템입니다.

Active 노드는 수집한 데이터를 로컬 SQLite에 저장하고 Standby 노드에도 복제합니다.

## 설치

```bash
pip install -r requirements.txt
```

Linux start/stop script:
```bash
chmod +x ProdBaremgrStart ProdBaremgrStop
./ProdBaremgrStart config_app_node1.yml
./ProdBaremgrStop
./ProdBaremgrStop config_app_node1.yml
```

- `ProdBaremgrStart`는 `.baremgr/run_*.state`에 `pid`, `config`, `console log`를 기록합니다.
- `ProdBaremgrStop`는 인자를 주지 않으면 마지막으로 시작한 config를 사용하고, 내부에서 `src.failover_db_stop` + `src.failover_zmq_stop`를 config 기반으로 호출합니다.
- 특정 config 인스턴스를 내리려면 같은 config 파일 경로를 인자로 넘기면 됩니다.

## 설정 파일 형식 (YAML)

앱은 JSON/YAML 모두 지원하지만, 기본은 YAML입니다.

샘플 파일:
- config_app_node1.yml
- config_app_node2.yml
- config_app_node3.yml

핵심 섹션:
- node_id, weight: 리더 선출 기준
- failover: backend(zmq/db) 및 heartbeat/DB 설정
- replication: Active → Standby 복제 포트/피어
- sqlite: 로컬 DB 디렉토리 경로 (테이블별 .db 파일 생성)
- collectors: oracle/splunk 수집 설정
- splunk는 기존 `host/username/password` 방식 또는 `base_url/credential_key/splunk_token` 기반 `SplunkSearch(query)` 방식 지원
- pipeline: 5/10분 후처리/동기화 스케줄 설정
- etc: 주기성 부가 작업(1분/5분 purge/heartbeat/oracle probe 등)

예시:

```yaml
node_id: node1
weight: 100

failover:
  backend: zmq
  port: 5555
  heartbeat_interval: 2
  peers:
    - 127.0.0.1:5557
    - 127.0.0.1:5559
  db:
    dsn: user/password@192.168.1.100:1521/ORCL
    table: FAILOVER_NODES
    ensure_table: true
    columns:
      node_id: NODE_ID
      weight: WEIGHT
      status: STATUS
      last_seen: LAST_SEEN
      updated_at: UPDATED_AT

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
  splunk:
    enabled: false
    base_url: https://splunk.internal:8089
    credential_key: my-credential-key
    splunk_token: my-splunk-token
    timeout_sec: 60

pipeline:
  processing:
    enabled: true
    interval_sec: 300
    run_on_active_only: true
    worker_mode: multiprocessing
    workers: 4
    batch_min: 6000
    batch_max: 8000
  sync:
    enabled: false
    interval_sec: 600
    run_on_active_only: true
    workers: 1
    mode: incremental
    source_dsn: user/password@192.168.1.100:1521/ORCL
    target_dsn: user/password@192.168.1.101:1521/ORCL
```

Optional Oracle client init config:
```yaml
oracle_client:
  lib_dir: ""
  config_dir: ""
  driver_name: BareMgr
```

pipeline 동작(1단계 구현):
1) processing/sync 스케줄러가 주기별 트리거를 발생시킨다.
2) run_on_active_only=true일 때 ACTIVE 노드에서만 실행된다.
3) processing은 원본 Oracle에서 6000~8000건을 조회해 내부 queue에 적재하고, worker가 queue를 소비해 처리한다.
4) 처리 결과는 SQLite/Drone/Oracle write 파이프라인으로 후처리한다.

processing 상세 옵션:
- worker_mode: multiprocessing 또는 thread
- workers: 병렬 작업자 수
- batch_min, batch_max: 실행당 객체 생성 수 범위
- source_oracle: main job 입력 원본 Oracle 조회 설정
- object_log.db_path: 객체 단위 SQLite 로그 파일 경로
- drone: HTTP Drone API 출력 설정(enabled, dry_run, url 등)
- current_oracle: Oracle 현재 데이터 read 설정
- oracle_write: 처리 결과 Oracle write 설정
- oracle_pool: Oracle SessionPool 공유 설정 (user/password/dsn/min/max 등)
- worker_oracle_lookup: worker 처리 중 Oracle lookup SQL 설정

Oracle connection 공유 전략:
- Python 3 상위 버전 호환을 위해 내부적으로 `oracledb as cx_Oracle`를 먼저 시도하고, 없으면 `cx_Oracle`를 사용합니다.
- 앱 시작 시 `init_oracle_client()`를 한 번 시도합니다. `oracle_client.lib_dir`, `config_dir`, `driver_name`가 있으면 그 값으로 초기화합니다.
- worker가 Oracle 조회를 자주 해야 하면 `processing.oracle_pool.enabled: true`를 사용합니다.
- 이 경우 worker들이 SessionPool을 공유해서 세션 수를 제한할 수 있습니다.
- `worker_oracle_lookup.enabled: true`일 때는 안전한 pool 공유를 위해 thread 모드로 실행됩니다.
- 즉, `worker_mode: multiprocessing`이더라도 worker lookup이 켜지면 내부적으로 thread 모드로 전환됩니다.

Failover 상태 로그:
- `failover.status_interval`마다 failover 상태 로그에 runtime 상태가 함께 기록됩니다.
- 로그에는 main job queue depth, processing worker 상태, collector active/alive 상태, sync 현재 테이블, etc active task 수가 포함됩니다.

main job queue 처리 구조:
1) `processing.source_oracle.sql`로 원본 Oracle 데이터를 batch_min~batch_max 범위에서 조회
2) 조회 결과를 내부 queue에 넣음
3) `processing.workers` 수의 worker가 queue에서 하나씩 꺼내 처리
4) 처리 완료 객체를 모아 SQLite 저장 + 객체 로그 + Drone + Oracle write 수행

sync 상세 옵션:
- workers: 테이블 단위 병렬 작업자 수
- mode: incremental 또는 full
- dry_run: true면 실제 반영 없이 건수/흐름만 검증
- tables: 동기화 대상 테이블 목록
- key_column: upsert 기준 키 컬럼
- timestamp_column: incremental 기준 컬럼
- batch_size: 배치 단위 fetch/upsert 크기
- ensure_table: 타깃 테이블 자동 생성 시도 여부
- checkpoint_db: 마지막 동기화 지점 저장 SQLite 파일

etc 상세 옵션:
- workers: task 단위 병렬 작업자 수
- interval_sec: etc 러너 tick 주기
- tasks[].interval_sec: task 개별 실행 주기(예: 60/300)
- tasks[].type:
  - sqlite_heartbeat
  - sqlite_purge_table
  - oracle_probe
- sqlite_log.db_path: etc 실행 이력 SQLite 파일

etc 동작:
1) `EtcManager`가 주기적으로 호출된다.
2) 각 task의 interval_sec 기준으로 due task만 실행한다.
3) 실행 이력은 `etc_runs` SQLite로 남기고, Store/Oracle 리소스를 내부에서 관리한다.

failover backend 선택:
- `failover.backend: zmq` : ZMQ heartbeat 기반 (기본값)
- `failover.backend: db` : Oracle 테이블 heartbeat 기반
- backend 값을 생략하면 기본으로 `zmq`가 사용됩니다.

DB failover 커스터마이징:
- `failover.db.table` : heartbeat 저장 테이블명
- `failover.db.columns.*` : 컬럼명 매핑
- `failover.db.command.*` : DB stop 명령 테이블/컬럼/poll 설정
- 테이블명/컬럼명은 대문자 영숫자/언더스코어 형식을 권장합니다.

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

config만 전달해서 backend별 stop 명령을 보낼 수 있습니다.

```bash
python -m src.failover_zmq_stop config_app_node1.yml
```

DB failover stop 테스트:

```bash
python -m src.failover_db_stop config_app_node1.yml
```

운영에서는 보통 아래처럼 Stop 스크립트 하나만 호출하면 됩니다.

```bash
./ProdBaremgrStop config_app_node1.yml
```

수초 내 Standby 노드 중 최고 가중치 노드가 Active로 승격됩니다.

## 실행 진입점

```bash
python main.py --config config_app_node1.yml
```

## 프로젝트 구조

- main.py (엔트리포인트)
- README.md
- config*.yml / config*.json
- src/ (애플리케이션 소스)
- tests/ (pytest + unittest 테스트)

## 테스트 실행

```bash
# pytest 테스트 실행
pytest -q

# unittest 테스트 실행
python -m unittest discover -s tests -p "test_*unittest.py"
```
