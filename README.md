````markdown
# Yi-Rang MQ (Yi-Rang-MQ)

Legacy embedded IPC를 위한 경량 메시지 큐(MQ) 데몬/서비스입니다.  
SQS처럼 단순한 사용성을 목표로 하며, 레거시 임베디드 환경에서 장비 변경 및 신규 프로세스 도입으로 복잡해지는 프로세스 간 메시지 통신을 저비용/저사양으로 처리하도록 설계되었습니다.

- 메시지 및 설정/정책: **JSON**
- 운영 형태: **Manager Process(데몬) + Consumer Process(Main Process) + CLI**
- 스토리지 백엔드(옵션): **FileSystem / SQLite**

> 목표: RabbitMQ 등 “무거운 브로커”를 도입하기 어려운 현장에서, 디바이스 내부 IPC 메시징을 더 단순하고 저렴하게 운영할 수 있게 합니다.

---

## Key Concepts

### Components
- **PublishCLI / Publish Process**
  - 메시지(JSON)를 발행합니다.
- **ConsumerCLI**
  - Consumer(Main Process)를 제어/운영합니다. (pause/resume/status 등)
- **Consumer Process (Main Process)**
  - 선택된 백엔드(FileSystem 또는 SQLite)에서 메시지를 consume합니다.
  - 메시지 validation 후 처리(handler)를 수행합니다.
- **YiRangMessageQueue (Manager Process)**
  - 큐/경로(또는 DB)와 정책을 관리합니다.
  - lease(visibility timeout), retry, DLQ를 포함한 큐 동작을 오케스트레이션합니다.
  - 내부적으로 **Backend Adapter(FileSystem/SQLite)** 를 통해 저장소를 추상화합니다.

### Storage Backends (Options)
- **FileSystem**
  - 메시지는 특정 폴더에 JSON 파일로 저장됩니다.
  - 상태 전이는 폴더 이동/rename 중심으로 동작합니다.
- **SQLite**
  - 메시지와 상태(ready/inflight/dlq 등)를 SQLite DB 파일에 저장합니다.
  - 상태 전이는 트랜잭션 기반 업데이트로 처리됩니다.

---

## Architecture

### Flow (Backend-aware)

```mermaid
flowchart LR
  subgraph Producers["Publish Side"]
    PCLI["PublishCLI"]
    PPROC["Publish Process"]
  end

  subgraph Consumers["Consume Side"]
    CCLI["ConsumerCLI"]
    CPROC["Consumer Process<br/>(Main Process)<br/>- consumes from selected backend<br/>(FileSystem follow/poll OR SQLite query/poll)<br/>- validates + processes messages"]
  end

  subgraph Manager["YiRangMessageQueue<br/>(Manager Process)<br/>- backend selection: FileSystem | SQLite<br/>- queue registry + policy + routing<br/>- lease/retry/dlq orchestration"]
    MAPI["Manager API/IPC<br/>(optional but recommended)"]
    BE["Backend Adapter<br/>(FS Adapter / SQLite Adapter)"]
  end

  subgraph Storage["Storage Backend (JSON)"]
    subgraph FS["FileSystem Backend"]
      INBOX["Inbox Folder(s)<br/>(JSON messages)"]
      META["Meta/State Folder(s)<br/>(JSON state, leases, offsets)"]
      DLQ["DLQ Folder(s)<br/>(JSON dead letters)"]
      ARCH["Archive Folder(s)<br/>(JSON processed)"]
      CFG["Config Folder<br/>(JSON configs/policies)"]
    end

    subgraph DB["SQLite Backend"]
      SQLITE["SQLite DB File<br/>(messages + state + policies)<br/>JSON stored as TEXT/JSON columns"]
    end
  end

  PCLI -->|"publish request"| MAPI
  PPROC -->|"publish request"| MAPI
  MAPI --> BE
  BE -->|"persist message"| INBOX
  BE -->|"persist message"| SQLITE

  CCLI -->|"control/operate"| CPROC
  CPROC -->|"consume loop"| MAPI
  MAPI --> BE
  BE -->|"read/lease messages"| INBOX
  BE -->|"read/lease messages"| SQLITE

  CPROC -->|"on success"| ARCH
  CPROC -->|"on failure"| DLQ
  CPROC -->|"update lease/retry state"| META
  CPROC -->|"update state (ack/nack/dlq)"| SQLITE
````

### Publish → Consume Sequence (Backend-aware)

```mermaid
sequenceDiagram
  autonumber
  actor Publisher as PublishCLI/Publish Process
  actor Operator as ConsumerCLI
  participant Mgr as YiRangMessageQueue<br/>(Manager Process)
  participant BE as Backend Adapter<br/>(FS / SQLite)
  participant FS as FileSystem Folders<br/>(JSON)
  participant DB as SQLite DB File<br/>(JSON as TEXT)
  participant Main as Consumer Process<br/>(Main Process)

  Publisher->>Mgr: Publish(message JSON)
  Mgr->>BE: Enqueue(message)
  alt backend = FileSystem
    BE->>FS: Atomic write JSON file into Inbox<br/>(temp -> fsync -> rename)
  else backend = SQLite
    BE->>DB: INSERT message row (state=ready)<br/>commit transaction
  end

  Main->>Mgr: ConsumeNext(leaseSeconds, consumerId)
  Mgr->>BE: Dequeue/Lease()
  alt backend = FileSystem
    BE->>FS: Watch/Poll Inbox & move to processing<br/>(rename as lease)
    BE-->>Mgr: Return message JSON
  else backend = SQLite
    BE->>DB: SELECT+UPDATE state=ready->inflight<br/>set lease_until (transaction)
    BE-->>Mgr: Return message JSON
  end

  Mgr-->>Main: Deliver message JSON + policy
  Main->>Main: Validate JSON schema + headers
  alt validation ok and processing ok
    Main->>Mgr: Ack(messageId)
    Mgr->>BE: Ack()
    alt backend = FileSystem
      BE->>FS: Move/Rename to Archive + write result metadata (JSON)
    else backend = SQLite
      BE->>DB: UPDATE state=archived (or delete)<br/>commit
    end
  else validation fail or processing fail
    Main->>Mgr: Nack(messageId, reason)
    Mgr->>BE: Nack()/RetryOrDLQ()
    alt retry available
      alt backend = FileSystem
        BE->>FS: Update retry metadata + reschedule<br/>(move back or delay folder)
      else backend = SQLite
        BE->>DB: UPDATE attempt + next_attempt_at<br/>state=ready (or delayed)
      end
    else move to DLQ
      alt backend = FileSystem
        BE->>FS: Move message to DLQ + append failure JSON
      else backend = SQLite
        BE->>DB: UPDATE state=dlq + store failure JSON<br/>commit
      end
    end
  end

  Operator->>Main: Control commands (pause/resume/status)
  Main-->>Operator: Report metrics/state
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

## CLI (Planned)

초기 목표는 백엔드가 달라도 동일한 커맨드 사용성을 제공하는 것입니다.

예시(스펙 제안):

```bash
# publish
yirangmq publish --backend fs --queue telemetry --file message.json
yirangmq publish --backend sqlite --db /data/yirang.db --queue telemetry --file message.json

# consume (run consumer main process)
yirangmq consume --backend fs --queue telemetry --path /var/yirang/queues
yirangmq consume --backend sqlite --db /data/yirang.db --queue telemetry

# status
yirangmq status --backend fs --queue telemetry
yirangmq status --backend sqlite --queue telemetry

# dlq operations
yirangmq dlq list --backend sqlite --queue telemetry-dlq
yirangmq dlq requeue --backend fs --queue telemetry-dlq --filter reasonCode=TIMEOUT
```

> 실제 옵션/명령은 구현에 따라 조정될 수 있습니다.

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

## Roadmap (Suggested)

* [ ] Backend abstraction (FS/SQLite Adapter)
* [ ] JSON Envelope + schema validation
* [ ] Lease (visibility timeout) + crash recovery
* [ ] Retry policy + DLQ + requeue
* [ ] ConsumerCLI 운영 기능(status/pause/resume/drain)
* [ ] Metrics/Health output
* [ ] Stress & fault tests (kill -9, reboot/power-loss scenarios)
* [ ] Documentation: backend 선택 가이드, 운영 매뉴얼

---

## Contributing

* Issues/PR 환영합니다.
* 임베디드/레거시 환경에서의 제약(파일시스템 종류, 저장소 특성, 전원 차단 패턴)에 대한 경험 기반 피드백을 특히 환영합니다.
* 코드 스타일/테스트 정책은 추후 `CONTRIBUTING.md`로 확장 예정입니다.

---

## Name

“Yi-Rang(이랑)”은 프로젝트 이름(딸 이름)에서 유래했습니다.
