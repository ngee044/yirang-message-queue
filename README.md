# Yi-Rang MQ

**레거시 임베디드 환경을 위한 경량 메시지 큐**

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## 왜 Yi-Rang MQ인가?

> "RabbitMQ는 너무 무겁고, 직접 만든 IPC는 관리가 안 된다"

레거시 임베디드 시스템에서 프로세스가 늘어날수록 IPC 통신은 복잡해집니다.
**Yi-Rang MQ**는 이런 환경을 위해 설계되었습니다:

| 문제 | Yi-Rang MQ 솔루션 |
|------|-------------------|
| 네트워크 스택 없음 | **파일 기반 Mailbox IPC** - TCP/IP 불필요 |
| 저사양 디바이스 | **SQLite/FileSystem** - 최소 의존성 |
| 전원 차단/재시작 | **Lease + Retry + DLQ** - 메시지 유실 방지 |
| 복잡한 브로커 설정 | **JSON 설정 파일 하나** - 즉시 시작 |
| 현장 디버깅 어려움 | **파일로 메시지 확인** - ls로 상태 파악 |

```
┌─────────────┐     File-based IPC      ┌─────────────┐
│  Producer   │ ──────────────────────> │   MainMQ    │
│  Process    │ <────────────────────── │   Daemon    │
└─────────────┘     (No Network!)       └─────────────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  SQLite /   │
                                        │  FileSystem │
                                        └─────────────┘
```

---

## Quick Start

### Option 1: Docker (권장)

```bash
# 클론
git clone https://github.com/your-repo/yirang-mq.git
cd yirang-mq/docker

# 서비스 시작
./docker-compose.sh up

# 테스트
./docker-compose.sh health
./docker-compose.sh publish telemetry '{"deviceId":"A1","timestamp":1700000000,"temp":25.5}'
./docker-compose.sh consume telemetry
```

### Option 2: 로컬 빌드

```bash
# 요구사항: CMake 3.21+, C++23 컴파일러, vcpkg

# 빌드
./build.sh

# 터미널 1: 데몬 실행
./build/out/MainMQ

# 터미널 2: 메시지 발행
./build/out/yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"temp":25.5}'

# 터미널 3: 메시지 소비
./build/out/yirangmq-cli-consumer
```

### Option 3: 임베디드 타깃 크로스컴파일

레거시/저사양 임베디드(ARM 등)로 배포할 때는 크로스 툴체인으로 빌드한다.

```bash
# 크로스 툴체인 + vcpkg triplet 지정, 작은 정적 바이너리로 빌드
./build.sh -m --static \
  --triplet arm64-linux \
  --toolchain /path/to/arm64-toolchain.cmake

# 또는 Docker buildx로 배포용 런타임 이미지를 크로스 빌드 (QEMU 에뮬레이션, 느림)
cd docker && ./docker-compose.sh buildx linux/arm64
```

- `--triplet`: vcpkg 타깃 triplet(예: `arm64-linux`, `arm-linux`). 의존성이 타깃 아키텍처로 빌드된다.
- `--toolchain`: CMake 크로스 툴체인 파일(vcpkg가 chainload). 타깃 컴파일러/시스루트를 지정.
- `--static`: 런타임 정적 링크(glibc/musl 버전 비의존) — 구형 임베디드 배포에 유용.
- `-m/--minsize`: MinSizeRel(최소 크기 바이너리).

> 실제 타깃별 툴체인 파일과 triplet은 배포 대상 하드웨어에 맞춰 준비한다. QEMU buildx 빌드는 CI/일회성 이미지 제작용이며 로컬 개발 루프에는 부적합하다.

---

## 핵심 기능

### 1. 파일 기반 IPC (Mailbox)

네트워크 없이 프로세스 간 통신:

```
ipc/
├── requests/           # 클라이언트 요청
└── responses/          # 서버 응답
    └── <clientId>/
```

- **Atomic Write**: temp → fsync → rename 패턴으로 데이터 무결성 보장
- **Zero Network**: TCP/IP 스택 없이 동작
- **현장 디버깅**: `ls`, `cat`으로 메시지 확인 가능

### 2. 3가지 스토리지 백엔드

| 백엔드 | 특징 | 적합한 환경 |
|--------|------|------------|
| **SQLite** | 트랜잭션, 빠른 조회 | 안정적인 lease/retry 필요 시 |
| **FileSystem** | 파일로 직접 확인 | 디버깅 중심, 의존성 최소화 |
| **Hybrid** | SQLite 인덱스 + 파일 payload | 대용량 메시지 + 빠른 조회 |

### 3. 신뢰성 보장

```
┌─────────┐   lease_next()   ┌─────────┐   timeout    ┌─────────┐
│  ready  │ ───────────────> │ inflight│ ──────────> │  ready  │
└─────────┘                  └─────────┘  (자동복구)   └─────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    │ ack()       │ nack()      │
                    ▼             ▼             ▼
               ┌─────────┐   ┌─────────┐   ┌─────────┐
               │ deleted │   │ delayed │   │   DLQ   │
               └─────────┘   └─────────┘   └─────────┘
```

- **Visibility Timeout**: Consumer 장애 시 메시지 자동 복구
- **Retry with Backoff**: exponential/linear/fixed 백오프
- **Dead Letter Queue**: 재시도 한도 초과 메시지 격리

### 4. Direct Addressing

특정 Consumer에게만 메시지를 전달할 수 있습니다:

```bash
# 일반 발행 (아무 Consumer나 처리)
yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"temp":36.5}'

# Direct Addressing (특정 Consumer만 처리)
yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"temp":36.5}' --target worker-01
```

| target 옵션 | 동작 |
|-------------|------|
| 미지정 | 아무 Consumer나 메시지 수신 가능 |
| `--target worker-01` | `consumer-id=worker-01`인 Consumer만 수신 |

---

## 로컬 테스트 가이드

### 1. 빌드

```bash
# Release 빌드
./build.sh

# 빌드 결과 확인
ls -la build/out/
# MainMQ                    - 메시지 큐 데몬
# yirangmq-cli-publisher    - Publisher CLI
# yirangmq-cli-consumer     - Consumer CLI
# *.json                    - 설정 파일
```

### 2. 데몬 실행 (터미널 1)

```bash
cd build/out
./MainMQ
```

출력 예시:
```
[START]
[INFORMATION] Yi-Rang MQ starting...
[INFORMATION] Backend: sqlite
[INFORMATION] IPC root: ./ipc
[INFORMATION] Mailbox handler started
```

### 3. Publisher 테스트 (터미널 2)

```bash
cd build/out

# 헬스 체크
./yirangmq-cli-publisher health

# 기본 큐 telemetry 는 스키마가 등록되어 있어 deviceId(문자열)가 필수이고,
# timestamp 는 있을 경우 숫자여야 한다. 위반 시 ERR_VALIDATION_FAILED 로 거부된다.

# 메시지 발행 (기본 - publish 명령 없이 바로 사용)
./yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"value":25.5}'

# 큐 지정 발행
./yirangmq-cli-publisher --queue telemetry --message '{"deviceId":"humidity-01","timestamp":1700000000,"value":65}'

# 특정 Consumer에게만 발행 (Direct Addressing)
./yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"alert":"high"}' --target worker-01

# 우선순위 지정 (값이 클수록 높은 우선순위, 기본값 0)
./yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"urgent":true}' --priority 10

# 지연 발행 (5초 후 ready 상태)
./yirangmq-cli-publisher --message '{"deviceId":"sensor-01","timestamp":1700000000,"scheduled":true}' --delay 5000

# 큐 상태 확인
./yirangmq-cli-publisher status --queue telemetry

# 서버 메트릭 조회
./yirangmq-cli-publisher metrics

# 도움말
./yirangmq-cli-publisher help
```

### 4. Consumer 테스트 (터미널 3)

```bash
cd build/out

# 메시지 소비 (기본 - consume 명령 없이 바로 사용)
./yirangmq-cli-consumer

# 큐 지정 소비
./yirangmq-cli-consumer consume --queue telemetry

# Consumer ID 지정
./yirangmq-cli-consumer consume --queue telemetry --consumer-id worker-01

# 도움말
./yirangmq-cli-consumer help
```

### 5. ACK/NACK 테스트

메시지 소비 후 받은 `messageKey`를 사용합니다:

```bash
# 소비 결과 예시:
# {
#   "lease": {
#     "messageKey": "msg:telemetry:ABC123",
#     "leaseId": "LEASE-XYZ"
#   },
#   "message": { ... }
# }

# ACK/NACK/extend-lease 는 lease 를 보유한 consumer 만 수행할 수 있다(3개 백엔드 공통).
# consume 때 사용한 것과 동일한 --consumer-id 를 넘겨야 하며, 생략 시
# consumer_configuration.json 의 consumerId 가 사용된다. --lease-id 를 함께 주면
# 만료·재리스된 stale 토큰까지 차단된다.

# 처리 성공 → ACK (메시지 삭제)
./yirangmq-cli-consumer ack --message-key msg:telemetry:ABC123 --lease-id LEASE-XYZ --consumer-id worker-01

# 처리 실패 → NACK (재시도 또는 DLQ)
./yirangmq-cli-consumer nack --message-key msg:telemetry:ABC123 --lease-id LEASE-XYZ --consumer-id worker-01 --reason "DB error" --requeue
```

### 6. DLQ (Dead Letter Queue) 테스트

```bash
# DLQ 메시지 목록 조회
./yirangmq-cli-consumer list-dlq --queue telemetry

# DLQ 메시지 재처리
./yirangmq-cli-consumer reprocess --message-key msg:telemetry:ABC123
```

### 7. 전체 시나리오 테스트

```bash
cd build/out

# 터미널 1: MainMQ 실행
./MainMQ

# 터미널 2: Publisher
./yirangmq-cli-publisher --message '{"test":1}'
./yirangmq-cli-publisher --message '{"test":2}'
./yirangmq-cli-publisher --message '{"test":3}'
./yirangmq-cli-publisher status

# 터미널 3: Consumer
./yirangmq-cli-consumer                              # 메시지 1 수신
./yirangmq-cli-consumer ack --message-key <key1>     # ACK

./yirangmq-cli-consumer                              # 메시지 2 수신
./yirangmq-cli-consumer nack --message-key <key2> --reason "error" --requeue  # NACK

./yirangmq-cli-consumer                              # 메시지 3 수신
# (30초 후 자동 복구 - ACK/NACK 없이 timeout)

# 상태 확인
./yirangmq-cli-publisher status
./yirangmq-cli-consumer list-dlq
```

---

## CLI 옵션

### yirangmq-cli-publisher

| 명령 | 설명 |
|------|------|
| (기본) | 메시지 발행 (publish와 동일) |
| `status` | 큐 상태 조회 |
| `health` | 서버 헬스 체크 |
| `metrics` | 서버 메트릭 조회 |
| `help` | 도움말 표시 |

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--message <json>` | 메시지 페이로드 (필수) | - |
| `--queue <name>` | 큐 이름 | 설정 파일 값 |
| `--target <id>` | 대상 Consumer ID | 미지정 (모두) |
| `--priority <n>` | 우선순위 (값이 클수록 높음) | 0 |
| `--delay <ms>` | 지연 시간 (ms) | 0 |
| `--ipc-root <path>` | IPC 폴더 경로 | ./ipc |
| `--timeout <ms>` | 응답 대기 시간 | 30000 |

### yirangmq-cli-consumer

| 명령 | 설명 |
|------|------|
| (기본) | 메시지 소비 (consume와 동일) |
| `consume` | 메시지 소비 |
| `ack` | 메시지 처리 완료 확인 |
| `nack` | 메시지 처리 실패 알림 |
| `extend-lease` | Lease 가시성 시간 연장 |
| `list-dlq` | DLQ 메시지 목록 |
| `reprocess` | DLQ 메시지 재처리 |
| `help` | 도움말 표시 |

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--queue <name>` | 큐 이름 | 설정 파일 값 |
| `--consumer-id <id>` | Consumer ID | 설정 파일 값 |
| `--message-key <key>` | 메시지 키 (ack/nack 필수) | - |
| `--reason <text>` | NACK 사유 | 빈 문자열 |
| `--requeue` | NACK 시 재시도 여부 | false |
| `--visibility <sec>` | Visibility timeout | 30 |

---

## 설정 파일

### MainMQ (main_mq_configuration.json)

```json
{
  "schemaVersion": "0.1",
  "nodeId": "device-01",
  "backend": "sqlite",

  "paths": {
    "dataRoot": "./data",
    "logRoot": "./logs"
  },

  "ipc": {
    "root": "./ipc",
    "requestsDir": "requests",
    "responsesDir": "responses",
    "pollIntervalMs": 100,
    "staleTimeoutMs": 30000
  },

  "sqlite": {
    "dbPath": "./data/yirangmq.db",
    "journalMode": "WAL",
    "busyTimeoutMs": 5000
  },

  "lease": {
    "visibilityTimeoutSec": 30,
    "sweepIntervalMs": 1000
  },

  "policyDefaults": {
    "visibilityTimeoutSec": 30,
    "retry": {
      "limit": 5,
      "backoff": "exponential",
      "initialDelaySec": 1,
      "maxDelaySec": 60
    },
    "dlq": {
      "enabled": true,
      "retentionDays": 14
    }
  }
}
```

### Publisher (publisher_configuration.json)

```json
{
  "ipc": {
    "root": "./ipc",
    "requestsDir": "requests",
    "responsesDir": "responses",
    "timeoutMs": 30000
  },
  "publisher": {
    "queue": "telemetry",
    "target": "",
    "priority": 0
  },
  "logging": {
    "writeConsole": 3,
    "writeFile": 0,
    "logRoot": "./logs"
  },
  "clientId": "publisher-01"
}
```

### Consumer (consumer_configuration.json)

```json
{
  "ipc": {
    "root": "./ipc",
    "requestsDir": "requests",
    "responsesDir": "responses",
    "timeoutMs": 30000
  },
  "consumer": {
    "queue": "telemetry",
    "consumerId": "worker-01",
    "visibilityTimeoutSec": 30
  },
  "logging": {
    "writeConsole": 3,
    "writeFile": 0,
    "logRoot": "./logs"
  }
}
```

### 백엔드 선택

```json
// SQLite (권장)
"backend": "sqlite"

// FileSystem
"backend": "filesystem"

// Hybrid (SQLite 인덱스 + 파일 payload)
"backend": "hybrid"
```

---

## Docker 사용법

```bash
cd docker

# 서비스 관리
./docker-compose.sh up          # 시작
./docker-compose.sh down        # 중지
./docker-compose.sh logs -f     # 로그 확인
./docker-compose.sh status      # 상태 확인

# Publisher 명령
./docker-compose.sh publish telemetry '{"deviceId":"sensor-01","timestamp":1700000000,"temp":25.5}'
./docker-compose.sh publish telemetry '{"deviceId":"sensor-01","timestamp":1700000000,"temp":25.5}' --target worker-01
./docker-compose.sh health
./docker-compose.sh metrics
./docker-compose.sh queue-status telemetry

# Consumer 명령
./docker-compose.sh consume telemetry
./docker-compose.sh ack msg:telemetry:abc123
./docker-compose.sh nack msg:telemetry:abc123 --reason "error" --requeue
./docker-compose.sh list-dlq telemetry
./docker-compose.sh reprocess msg:telemetry:abc123

# 정리
./docker-compose.sh clean
```

---

## 아키텍처

### 컴포넌트 구조

```
┌────────────────────────────────────────────────────────────┐
│                        MainMQ Daemon                        │
├──────────────┬──────────────┬──────────────┬───────────────┤
│MailboxHandler│ QueueManager │Configurations│MessageValidator│
│  (IPC 처리)  │(Lease/Retry) │  (설정 로드)  │   (스키마)     │
└──────┬───────┴──────┬───────┴──────────────┴───────────────┘
       │              │
       └──────────────┘
                │
                ▼
      ┌─────────────────┐
      │  BackendAdapter │
      │ (추상 인터페이스) │
      └────────┬────────┘
               │
  ┌────────────┼────────────┬────────────┐
  ▼            ▼            ▼            ▼
┌───────┐  ┌───────┐  ┌───────┐  ┌───────────┐
│SQLite │  │  FS   │  │Hybrid │  │  (확장)   │
│Adapter│  │Adapter│  │Adapter│  │           │
└───────┘  └───────┘  └───────┘  └───────────┘
```

### 메시지 흐름

```
Publisher                MainMQ                    Backend
   │                        │                         │
   │ 1. Request JSON 작성    │                         │
   │ ──────────────────────>│                         │
   │    ipc/requests/       │                         │
   │                        │ 2. enqueue()            │
   │                        │ ───────────────────────>│
   │                        │    state = "ready"      │
   │ 3. Response JSON       │                         │
   │ <──────────────────────│                         │
   │    ipc/responses/      │                         │


Consumer                 MainMQ                    Backend
   │                        │                         │
   │ 1. consume_next 요청    │                         │
   │ ──────────────────────>│                         │
   │                        │ 2. lease_next()         │
   │                        │ ───────────────────────>│
   │                        │    state = "inflight"   │
   │                        │    lease_until = now+30s│
   │ 3. message + lease     │                         │
   │ <──────────────────────│                         │
   │                        │                         │
   │ 4. ack/nack            │                         │
   │ ──────────────────────>│ 5. ack() / nack()      │
   │                        │ ───────────────────────>│
```

---

## 빌드

### 요구사항

- CMake 3.21+
- C++23 지원 컴파일러 (GCC 13+, Clang 16+)
- vcpkg

### 의존성

```json
{
  "dependencies": [
    "lz4",
    "efsw",
    "sqlite3",
    "nlohmann-json"
  ]
}
```

### 빌드 명령

```bash
# Release 빌드 (기본)
./build.sh

# Debug 빌드
./build.sh -d

# Clean 빌드
./build.sh -c

# 병렬 빌드 (8 jobs)
./build.sh -j 8

# vcpkg 경로 지정
./build.sh -v ~/vcpkg
```

### 출력물

```
build/
├── out/
│   ├── MainMQ                  # 데몬 서비스
│   ├── yirangmq-cli-publisher  # Publisher CLI
│   ├── yirangmq-cli-consumer   # Consumer CLI
│   └── *.json                  # 설정 파일
└── lib/
    ├── libBackendAdapter.a
    ├── libUtilities.a
    └── ...
```

---

## 테스트

### Docker 테스트

```bash
cd docker
./docker-compose.sh up
./docker-compose.sh health
./docker-compose.sh publish telemetry '{"test":true}'
./docker-compose.sh consume telemetry
./docker-compose.sh down
```

### 통합 테스트

로컬 macOS는 efsw vcpkg 빌드가 불가하므로 테스트는 Docker로 실행한다.

```bash
cd docker

# 단위 + 통합 전체
./docker-compose.sh test

# 단위만 / 통합만
./docker-compose.sh test-unit
./docker-compose.sh test-integration
```

---

## 적합한 사용 사례

### 추천

- 레거시 임베디드에서 프로세스 간 메시징이 필요한 경우
- 네트워크 스택 없이 IPC가 필요한 경우
- RabbitMQ/Kafka 등이 과한 저사양 환경
- 전원 차단/재시작이 빈번한 산업용 장비
- 현장에서 파일로 직접 메시지를 확인해야 하는 경우

### 비추천

- 초저지연/하드 리얼타임이 필수인 경우
- 대용량 스트리밍이 필요한 경우
- 멀티 디바이스 분산 메시징 (현재 범위 외)

---

## 프로젝트 구조

```
yirang-mq/
├── MainMQ/                       # 데몬 서비스
│   ├── BackendAdapter/           # 스토리지 백엔드
│   │   ├── BackendAdapter.h      # 추상 인터페이스
│   │   ├── SQLiteAdapter.*       # SQLite 구현
│   │   ├── FileSystemAdapter.*   # FileSystem 구현
│   │   └── HybridAdapter.*       # Hybrid 구현
│   ├── MailboxHandler.*          # IPC 처리
│   ├── QueueManager.*            # Lease/Retry 관리
│   ├── Configurations.*          # 설정 로더
│   └── main.cpp                  # 진입점
├── Samples/
│   ├── yirangmq-cli-publisher/   # Publisher CLI
│   └── yirangmq-cli-consumer/    # Consumer CLI
├── CommonLibrary/                # 공용 라이브러리
│   ├── Utilities/                # Logger, File, JSON 등
│   ├── DataBase/                 # SQLite 래퍼
│   └── ThreadPool/               # 스레드풀
├── docker/                       # Docker 설정
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── docker-compose.sh
└── docs/                         # 문서
    ├── OPERATIONS.md             # 운영 가이드
    └── TROUBLESHOOTING.md        # 장애 대응
```

---

## 문서

- [운영 가이드](docs/OPERATIONS.md) - 디렉토리 구조, 백업, 모니터링
- [장애 대응](docs/TROUBLESHOOTING.md) - 문제 해결 시나리오
- [코드 컨벤션](docs/CODE_CONVENTIONS.md) - 개발 가이드라인
- [기술 부채 우선순위](docs/TECH_DEBT_PRIORITIES.md) - 기술 부채 관리
- [TODO](docs/TODO.md) - 작업 추적

---

## Contributing

Issues와 PR을 환영합니다.
특히 임베디드/레거시 환경에서의 실제 사용 경험에 기반한 피드백을 환영합니다.

---

## License

MIT License

---

## Name

"Yi-Rang(이랑)"은 프로젝트 이름이자, 딸의 이름에서 유래했습니다.
