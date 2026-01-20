
# Yi-Rang MQ (Yi-Rang-MQ)

Legacy embedded 시스템을 위한 IPC를 위한 경량 메시지 큐(MQ) 데몬/서비스입니다.  
SQS처럼 단순한 사용성을 목표로 하며, 레거시 임베디드 환경에서 장비 변경 및 신규 프로세스 도입으로 복잡해지는 프로세스 간 메시지 통신을 저비용/저사양으로 처리하도록 설계되었습니다.
네트워크 통신이 아닌 파일/DB(SQLite) 및 파일 기반 Mailbox IPC를 통한 통신으로 최소 요구 사양을 요구하는 시스템 설계를 위한 소프트웨어 입니다.

- 메시지 및 설정/정책: **JSON**
- 운영 형태: **Manager Process(데몬) + Client(Process/CLI) + Mailbox IPC**
- IPC 채널: **Mailbox (File-based IPC)**
- 스토리지 백엔드(옵션): **FileSystem / SQLite / Hybrid**

> 목표: RabbitMQ 등 “무거운 브로커”를 도입하기 어려운 현장에서, 디바이스 내부 IPC 메시징을 더 단순하고 저렴하게 운영할 수 있게 합니다.

---

## Key Concepts

### Components
- **PublishCLI / Publish Process**
  - Mailbox에 publish 요청을 작성합니다.
- **ConsumerCLI**
  - Mailbox로 운영/상태 요청을 전송합니다. (pause/resume/status 등)
- **Consumer Process (Main Process)**
  - Mailbox로 ConsumeNext 요청을 전송하고 메시지를 처리합니다.
  - 메시지 validation 후 처리(handler)를 수행합니다.
- **Mailbox IPC (File-based)**
  - 요청/응답을 파일로 교환하는 로컬 IPC 채널입니다.
  - atomic rename을 이용해 요청/응답의 일관성을 확보합니다.
- **YiRangMessageQueue (Manager Process)**
  - Mailbox 요청을 수신하여 큐/정책/스토리지를 오케스트레이션합니다.
  - lease(visibility timeout), retry, DLQ를 포함한 큐 동작을 처리합니다.
  - 내부적으로 **Backend Adapter(FileSystem/SQLite/Hybrid)** 를 통해 저장소를 추상화합니다.

### Storage Backends (Options)
- **FileSystem**
  - 메시지는 특정 폴더에 JSON 파일로 저장됩니다.
  - 상태 전이는 폴더 이동/rename 중심으로 동작합니다.
- **SQLite**
  - 메시지와 상태(ready/inflight/dlq 등)를 SQLite DB 파일에 저장합니다.
  - 상태 전이는 트랜잭션 기반 업데이트로 처리됩니다.
- **Hybrid**
  - 메시지 payload는 파일로 저장하고, 상태/인덱스는 SQLite로 관리합니다.
  - 파일 가시성과 쿼리 성능의 절충안을 제공합니다.

---

## Mailbox IPC and Operation Modes

### Mailbox IPC (File-based)
- **요청/응답 파일 교환**: client는 `requests/`에 JSON 요청 파일을 작성하고, MainMQ는 처리 후 `responses/<clientId>/`에 응답 파일을 작성합니다.
- **원자성**: `temp` 파일 작성 후 `rename`으로 commit합니다.
- **재처리/복구**: 처리 지연 시 타임아웃 기준으로 재시도하거나, 오류 응답을 반환합니다.
- **권한 분리**: Mailbox 폴더에 대한 read/write 권한을 프로세스별로 제한할 수 있습니다.

### Operation Modes (Mailbox + Backend)
- **Mailbox + SQLite**
  - MainMQ가 SQLite에 단독 write를 수행하고, 모든 client는 Mailbox를 통해 요청합니다.
  - 안정적 lease/retry/metrics 구현에 유리합니다.
- **Mailbox + FileSystem**
  - MainMQ가 파일 기반 큐를 관리하고, client는 Mailbox 요청만 수행합니다.
  - 의존성 최소, 현장 디버깅 용이합니다.
- **Hybrid (Mailbox + FileSystem + SQLite)**
  - 메시지 payload는 파일, 상태/인덱스는 SQLite로 관리합니다.
  - 대용량 payload와 빠른 상태 조회를 함께 제공합니다.

---

## Architecture

### Flow (Mailbox-aware)

```mermaid
flowchart LR
  subgraph Clients["Clients"]
    PCLI["Publish Client"]
    CCLI["Consume/Control Client"]
  end

  subgraph IPC["Mailbox IPC (File-based)"]
    REQ["requests/"]
    RES["responses/<clientId>/"]
  end

  subgraph Manager["YiRangMessageQueue<br/>(Manager Process)<br/>- backend selection: FileSystem | SQLite | Hybrid<br/>- queue registry + policy + routing<br/>- lease/retry/dlq orchestration"]
    MAPI["Mailbox Handler"]
    BE["Backend Adapter<br/>(FS / SQLite / Hybrid)"]
  end

  subgraph Storage["Storage Backend"]
    subgraph FS["FileSystem Backend"]
      INBOX["Inbox Folder(s)"]
      META["Meta/State Folder(s)"]
      DLQ["DLQ Folder(s)"]
      ARCH["Archive Folder(s)"]
    end

    subgraph DB["SQLite Backend"]
      SQLITE["SQLite DB File<br/>(index/state/policies)"]
    end

    subgraph HYB["Hybrid Payload Storage"]
      PAYLOAD["Payload Files"]
    end
  end

  PCLI -->|"request file"| REQ
  CCLI -->|"request file"| REQ
  REQ --> MAPI
  MAPI --> RES
  MAPI --> BE
  BE --> INBOX
  BE --> META
  BE --> DLQ
  BE --> ARCH
  BE --> SQLITE
  BE --> PAYLOAD
````

### Publish → Consume Sequence (Mailbox-aware)

```mermaid
sequenceDiagram
  autonumber
  actor Publisher as Publish Client
  actor Consumer as Consume Client
  participant MB as Mailbox<br/>(requests/responses)
  participant Mgr as YiRangMessageQueue<br/>(Manager Process)
  participant BE as Backend Adapter<br/>(FS / SQLite / Hybrid)
  participant FS as FileSystem Folders
  participant DB as SQLite DB
  participant PAY as Payload Files

  Publisher->>MB: Write Publish request (JSON)
  MB->>Mgr: Deliver request (poll/watch)
  Mgr->>BE: Enqueue(message)
  alt backend = FileSystem
    BE->>FS: Atomic write into Inbox<br/>(temp -> fsync -> rename)
  else backend = SQLite
    BE->>DB: INSERT message row (state=ready)
  else backend = Hybrid
    BE->>PAY: Write payload file
    BE->>DB: INSERT index/state
  end
  Mgr-->>MB: Write Publish response

  Consumer->>MB: Write ConsumeNext request
  MB->>Mgr: Deliver request
  Mgr->>BE: LeaseNext()
  alt backend = FileSystem
    BE->>FS: Move to processing (lease)
  else backend = SQLite
    BE->>DB: SELECT+UPDATE inflight + lease_until
  else backend = Hybrid
    BE->>DB: SELECT+UPDATE inflight
    BE->>PAY: Read payload file
  end
  Mgr-->>MB: Deliver message + lease token

  Consumer->>MB: Write Ack/Nack request
  MB->>Mgr: Deliver request
  Mgr->>BE: Ack / Nack / Delay / DLQ
```

---

## Why Yi-Rang MQ?

### When it is valuable

* 레거시 임베디드에서 프로세스/서비스가 증가하며 IPC 통신 복잡도가 커지는 상황
* 외부 브로커(RabbitMQ 등) 도입이 부담스럽거나 과한 환경
* 저사양/저비용에서 “단순하고 운영 가능한” 큐가 필요한 경우
* 전원 차단/재시작이 발생할 수 있어 **lease/retry/DLQ**가 필요한 경우

### When it may be a poor fit

* 초저지연/하드 리얼타임이 절대적으로 중요한 경우
* 상시 고TPS/대용량 스트리밍이 필요한 경우
* 분산(멀티 디바이스) 메시징까지 목표인 경우 (현재 범위 밖)

---

## Backend Options: Pros / Cons and Operational Differences

### 1) FileSystem Backend

**Pros**

* 메시지가 파일로 남아 **현장 디버깅/수동 복구**가 쉬움
* 외부 의존성 최소 (파일시스템만 있으면 동작)
* “폴더를 보면 상태가 보이는” 직관적인 운영

**Cons**

* 전원 차단/비정상 종료에 대비한 **원자적 커밋 규칙**이 필수
* 동시성(여러 consumer/producer) 경합 제어가 까다로울 수 있음
* 파일 수가 많아지면 디렉토리 scan/메타 업데이트 비용이 증가 가능

**Operational behavior**

* publish: inbox에 JSON 파일을 atomic write (temp → fsync → rename)
* consume: inbox watch/poll → processing으로 rename(lease) → ack 시 archive, 실패 시 retry 또는 dlq

---

### 2) SQLite Backend

**Pros**

* 트랜잭션 기반으로 enqueue/dequeue/ack/nack가 명확하고 **crash recovery가 쉬움**
* lease/retry/dlq/지연큐/우선순위/필터링 등 기능 확장이 유리
* 대량 메시지에서 관리/집계가 상대적으로 안정적

**Cons**

* SQLite의 락/동시성 모델 이해가 필요 (WAL, busy_timeout, 짧은 트랜잭션 등)
* 스키마/마이그레이션 관리 필요
* 파일 기반 대비 메시지 “가시성”이 낮아 CLI/툴이 중요

**Operational behavior**

* publish: DB에 INSERT (state=ready)
* consume: 트랜잭션으로 SELECT+UPDATE하여 lease 확보 (state=ready→inflight, lease_until 설정)
* ack/nack: state 업데이트 및 실패 메타 기록

---

### 3) Hybrid Backend (File payload + SQLite index)

**Pros**

* 대용량 payload를 파일로 분리하여 DB 부하를 낮춤
* 상태/메트릭/검색은 SQLite로 빠르게 처리 가능
* 파일 가시성과 DB 조회 성능을 동시에 확보

**Cons**

* 파일-DB 간 일관성 유지 로직이 필요
* 장애 시 정합성 복구 절차가 추가됨
* 구현 복잡도 증가

**Operational behavior**

* publish: payload 파일 저장 + SQLite에 index/state 삽입 (원자성 보장 필요)
* consume: SQLite에서 lease 확보 후 payload 파일을 로드
* ack/nack: SQLite 상태 전이 + 필요 시 payload 파일 정리

**Key-Value model (current SQLite adapter)**

* SQLite는 `kv` 테이블에 메시지/정책을 JSON(TEXT)로 저장하고, `msg_index`로 큐 조회를 처리합니다.
* `msg_index`에는 `queue/state/priority/available_at/lease_until/attempt`가 포함됩니다.
* policy key는 `policy:{queue}` 형식으로 저장합니다. MQCLI는 `msg:{queue}:{guid}` 형식의 key를 사용합니다.

## SQLite KV Schema (Current Template)

KV로 원본 데이터를 저장하고, 큐 조회는 보조 인덱스 테이블을 사용합니다.
실제 스키마는 `MainMQ/sqlite_schema.sql` 템플릿에서 `{{kv_table}}`, `{{msg_index_table}}`을 치환합니다.

```sql
CREATE TABLE IF NOT EXISTS kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  value_type TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_kv_type ON kv(value_type);
CREATE INDEX IF NOT EXISTS idx_kv_expires ON kv(expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS msg_index (
  queue TEXT NOT NULL,
  state TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  available_at INTEGER NOT NULL,
  lease_until INTEGER,
  attempt INTEGER NOT NULL DEFAULT 0,
  message_key TEXT NOT NULL REFERENCES kv(key) ON DELETE CASCADE,
  PRIMARY KEY (message_key)
);

CREATE INDEX IF NOT EXISTS idx_msg_ready ON msg_index(queue, state, available_at, priority DESC);
CREATE INDEX IF NOT EXISTS idx_msg_lease ON msg_index(state, lease_until) WHERE state = 'inflight';
CREATE INDEX IF NOT EXISTS idx_msg_delayed ON msg_index(state, available_at) WHERE state = 'delayed';
CREATE INDEX IF NOT EXISTS idx_msg_dlq ON msg_index(queue, state) WHERE state = 'dlq';
```

Key 네임스페이스 예시:

* `policy:{queue}` (현재 사용)
* `msg:{queue}:{guid}` (MQCLI 기본 포맷)

---

## Message Model (JSON)

모든 메시지는 JSON Envelope 형태를 권장합니다.

예시:

```json
{
  "messageId": "9b2f2f2a-6f64-4f8b-a9b5-1e0d6f3a0d71",
  "timestamp": "2026-01-13T10:21:00+09:00",
  "queue": "telemetry",
  "type": "sensor.reading",
  "version": 1,
  "attributes": {
    "deviceId": "device-001",
    "priority": 3
  },
  "traceId": "trace-1234",
  "deduplicationKey": "device-001:1705112460",
  "payload": {
    "temp": 36.1,
    "rpm": 1200
  }
}
```

현재 SQLiteAdapter는 아래와 같은 최소 envelope를 저장합니다. (`payload`, `attributes`는 JSON string으로 저장됨)

```json
{
  "messageId": "uuid",
  "queue": "telemetry",
  "payload": "{...}",
  "attributes": "{...}",
  "priority": 0,
  "attempt": 0,
  "createdAt": 1700000000000
}
```

> 프로젝트는 JSON schema 기반 validation을 염두에 두며, validation 실패 시 DLQ로 전송할 수 있습니다.

---

## Queue Policy (JSON)

큐별 정책은 JSON으로 관리합니다(예: `policy.json`).

예시:

```json
{
  "visibilityTimeoutSec": 30,
  "retry": {
    "limit": 5,
    "backoff": "exponential",
    "initialDelaySec": 1,
    "maxDelaySec": 60
  },
  "dlq": {
    "enabled": true,
    "queue": "telemetry-dlq",
    "retentionDays": 14
  }
}
```

---

## MainMQ Configuration (Current + Draft)

`MainMQ/main_mq_configuration.json`에서 Manager 설정 스키마를 정의합니다.

* `schemaVersion`: 설정 스키마 버전
* `backend`: `sqlite` 또는 `filesystem`
* `mode`: `mailbox_sqlite` | `mailbox_fs` | `hybrid`
* `paths`: data/log 루트 경로
* `ipc`: Mailbox IPC 설정 (요청/응답 폴더, 타임아웃 등)
* `sqlite`: DB 경로, KV/인덱스 테이블명, 스키마 경로, WAL 관련 옵션
* `filesystem`: FS 루트 및 상태 폴더 구성
* `hybrid`: payload 저장 경로 및 정합성 옵션
* `policyDefaults`: 전역 기본 정책
* `queues`: 큐별 정책 오버라이드

현재 `MainMQ/main.cpp`와 `QueueManager`에서 실제로 사용하는 필드:

* `backend` (현재 sqlite만 지원)
* `paths.logRoot`
* `sqlite.dbPath`, `sqlite.kvTable`, `sqlite.messageIndexTable`
* `sqlite.busyTimeoutMs`, `sqlite.journalMode`, `sqlite.synchronous`
* `lease.visibilityTimeoutSec`, `lease.sweepIntervalMs`
* `policyDefaults`, `queues` (QueueManager 등록 및 정책 저장)

`sqlite.schemaPath`는 현재 `Configurations`에서 미사용입니다.
`filesystem`/`ipc`/`hybrid`는 파싱되지만 백엔드/IPC가 미구현 상태입니다.

---

## DLQ (Dead Letter Queue) Concept

DLQ는 다음과 같은 상황에 사용합니다.

* JSON schema validation 실패
* 핸들러 처리 실패/타임아웃
* retry limit 초과
* 반복적으로 동일 사유로 실패하는 poison message 탐지

**DLQ 메시지에는 실패 사유 JSON을 함께 기록**하는 것을 권장합니다.

예시:

```json
{
  "originalMessage": { "...": "..." },
  "failure": {
    "reasonCode": "VALIDATION_ERROR",
    "reasonMessage": "missing required field: payload.temp",
    "failedAt": "2026-01-13T10:25:12+09:00",
    "attempt": 1,
    "consumerId": "consumer-01"
  },
  "routing": {
    "sourceQueue": "telemetry",
    "dlqQueue": "telemetry-dlq"
  }
}
```

운영 기능으로는 다음을 권장합니다.

* DLQ 조회/집계(사유별 증가량)
* DLQ requeue(조건부 재처리)
* quarantine(격리), TTL 기반 purge/아카이브

---

## CLI (Current + Planned)

### MQCLI (Current, SQLite direct)

현재 SQLite 백엔드를 직접 사용하는 테스트용 CLI가 제공됩니다.
`Samples/MQCLI`에서 빌드되며, Manager API/IPC 없이 DB에 직접 접근합니다.

```bash
# publish
./build/out/MQCLI publish --queue telemetry --message '{"temp":36.5,"rpm":1200}' --priority 5

# consume
./build/out/MQCLI consume --queue telemetry --consumer-id consumer-01 --timeout 30

# status
./build/out/MQCLI status --queue telemetry

# ack / nack
./build/out/MQCLI ack --message-key <message-key>
./build/out/MQCLI nack --message-key <message-key> --reason "error" --requeue
```

테스트 스크립트: `test_mq.sh`

### yirangmq (Mailbox Client)

Mailbox IPC를 통해 MainMQ에 요청/응답하는 클라이언트입니다.
MainMQ가 반드시 실행 중이어야 합니다.

```bash
# MainMQ 시작 (백그라운드)
./build/out/MainMQ &

# Health 체크
./build/out/yirangmq health

# 메시지 발행
./build/out/yirangmq publish --queue telemetry --message '{"temp":36.5}'

# 메시지 소비
./build/out/yirangmq consume --queue telemetry --consumer-id consumer-01

# ACK
./build/out/yirangmq ack --message-key "msg:telemetry:xxx" --lease-id "xxx"

# NACK (requeue)
./build/out/yirangmq nack --message-key "msg:telemetry:xxx" --lease-id "xxx" --reason "error" --requeue

# 큐 상태 확인
./build/out/yirangmq status --queue telemetry

# Mailbox 메트릭
./build/out/yirangmq metrics

# DLQ 목록 조회
./build/out/yirangmq list-dlq --queue telemetry --limit 50

# DLQ 재처리
./build/out/yirangmq reprocess --message-key "msg:telemetry:xxx"
```

옵션:
* `--ipc-root`: IPC 루트 폴더 경로 (기본값: `./ipc`)
* `--timeout`: 응답 대기 타임아웃 (ms, 기본값: 30000)

---

## Reliability Principles (Design Goals)

현업 적용을 위한 핵심 원칙입니다.

* **Atomicity**

  * FileSystem: temp write → fsync → rename(커밋) 패턴
  * SQLite: 트랜잭션 commit으로 상태 전이 보장
* **Lease / Visibility Timeout**

  * 처리 중 소비자가 죽어도 메시지가 유실/정체되지 않도록 재전달
* **Retry with Backoff**

  * 재시도 횟수/백오프 정책을 JSON으로 관리
* **Observability**

  * backlog, inflight, dlq, 처리율, 실패율, 평균 처리시간 등 기본 메트릭 제공

---

## Implementation Status (Current)

### Core Components
* **Mailbox IPC**: 완전 구현 - 요청/응답 폴더 기반 IPC, 핸들러, atomic write/rename
* **SQLite backend**: 완전 구현 - enqueue/lease_next/ack/nack/extend_lease + policy load/save + metrics + lease/delayed 처리
* **FileSystem backend**: 완전 구현 - 파일 기반 메시지 저장, 상태 전이, lease sweep, delayed sweep
* **Hybrid backend**: 완전 구현 - SQLite index + File payload, 정합성 체크 및 복구 루틴
* **MainMQ**: 완전 구현 - Configurations 로드/검증, 큐 등록, QueueManager 실행, 시그널 처리
* **QueueManager**: 완전 구현 - lease sweep + delayed sweep + retry/DLQ 오케스트레이션

### Client Tools
* **yirangmq CLI**: 완전 구현 - Mailbox 기반 클라이언트 (publish/consume/ack/nack/status/health/metrics/list-dlq/reprocess)
* **MQCLI**: 유지 - SQLite 직접 접근 CLI (테스트용)

### Observability
* **Metrics**: queue별 ready/inflight/delayed/dlq 카운트, Mailbox 처리량/오류 메트릭
* **Health**: health 명령으로 서비스 상태 확인
* **DLQ 관리**: list-dlq/reprocess 명령으로 DLQ 조회 및 재처리

### Testing
* **SQLite 모드**: 통합 테스트 완료 (`test_mailbox.sh`)
* **FileSystem 모드**: 통합 테스트 완료 (`test_all_modes.sh`)
* **Hybrid 모드**: 통합 테스트 완료 (`test_all_modes.sh`)

---

## Roadmap

### Completed
* [x] Mailbox IPC (요청/응답 폴더, 핸들러, client)
* [x] Backend abstraction (interface + SQLite/FileSystem/Hybrid adapters)
* [x] Lease (visibility timeout) + crash recovery
* [x] Retry policy + DLQ + requeue
* [x] yirangmq CLI (Mailbox 기반 클라이언트)
* [x] Metrics/Health output (adapter metrics + Mailbox metrics)
* [x] All backend modes integration tests

### Remaining
* [ ] JSON Envelope + schema validation (MessageValidator 연결 부분 구현)
* [ ] Mailbox IPC 단위 테스트 (schema/timeout)
* [ ] Stress & fault tests (kill -9, reboot/power-loss scenarios)
* [ ] Documentation: 운영 매뉴얼, 장애 대응 가이드

### MainMQ Implementation Status

MainMQ는 Manager Process로서 큐/정책/백엔드를 오케스트레이션합니다.

1. [x] 설정 스키마 확정 + 로더 구현 (`Configurations` 연동/검증)
2. [x] 도메인 모델 정의 (`BackendAdapter.h` 구조체 정의)
3. [x] Backend Adapter 인터페이스 설계
4. [x] SQLite Adapter 구현: KV 테이블 + 상태/큐 인덱스 + 트랜잭션 경계
5. [x] Mailbox IPC 계층 구현 (요청/응답 폴더, 핸들러, 타임아웃/재처리)
6. [x] Manager API/IPC + yirangmq CLI 연동
7. [x] Lease/Retry/DLQ 스케줄러 구현 (lease/delayed sweep, retry/dlq 오케스트레이션)
8. [x] 관측/로그/메트릭 출력
9. [x] FileSystem Adapter 구현
10. [x] Hybrid Adapter 구현 + 정합성 체크/복구

---

## Contributing

* Issues/PR 환영합니다.
* 임베디드/레거시 환경에서의 제약(파일시스템 종류, 저장소 특성, 전원 차단 패턴)에 대한 경험 기반 피드백을 특히 환영합니다.
* 코드 스타일/테스트 정책은 추후 `CONTRIBUTING.md`로 확장 예정입니다.

---

## Name

“Yi-Rang(이랑)”은 프로젝트 이름(딸 이름)에서 유래했습니다.
