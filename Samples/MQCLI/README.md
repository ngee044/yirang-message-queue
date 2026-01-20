# MQCLI - Yi-Rang MQ Command Line Interface

Yi-Rang MQ를 테스트하고 관리하기 위한 CLI 도구입니다.
현재 MQCLI는 SQLite에 직접 접근하는 테스트용 도구이며, Mailbox IPC 기반 클라이언트는 별도 구현 예정입니다.

## 빌드

프로젝트 루트에서:

```bash
./build.sh
```

실행 파일: `./build/out/MQCLI`

## 사용법

### Publish - 메시지 발행

```bash
MQCLI publish --queue <queue-name> --message <json> [--priority <n>]
```

**예시:**
```bash
./build/out/MQCLI publish --queue telemetry --message '{"temp":36.5,"rpm":1200}' --priority 5
```

**옵션:**
- `--queue`: 큐 이름 (필수)
- `--message`: JSON 메시지 페이로드 (필수)
- `--priority`: 우선순위 (선택, 기본값: 0, 높을수록 먼저 처리)

### Consume - 메시지 소비

```bash
MQCLI consume --queue <queue-name> --consumer-id <id> [--timeout <sec>]
```

**예시:**
```bash
./build/out/MQCLI consume --queue telemetry --consumer-id consumer-01 --timeout 30
```

**옵션:**
- `--queue`: 큐 이름 (필수)
- `--consumer-id`: 소비자 ID (필수)
- `--timeout`: Visibility timeout 초 (선택, 기본값: 30)

**출력:**
- Message ID
- Message Key (ack/nack에 사용)
- Queue
- Priority
- Attempt
- Payload
- Lease ID
- Lease Until

### Status - 큐 상태 조회

```bash
MQCLI status --queue <queue-name>
```

**예시:**
```bash
./build/out/MQCLI status --queue telemetry
```

**출력:**
- Ready: 처리 대기 중인 메시지 수
- Inflight: 처리 중인 메시지 수
- Delayed: 지연된 메시지 수
- DLQ: Dead Letter Queue 메시지 수
- Total: 전체 메시지 수

### Ack - 메시지 확인 (성공 처리)

```bash
MQCLI ack --message-key <message-key>
```

**예시:**
```bash
./build/out/MQCLI ack --message-key msg:telemetry:12345678-abcd-efgh
```

메시지를 성공적으로 처리했음을 확인합니다. 메시지는 큐에서 삭제됩니다.

### Nack - 메시지 거부 (실패 처리)

#### 재시도 (Requeue)

```bash
MQCLI nack --message-key <message-key> --reason <reason> --requeue
```

**예시:**
```bash
./build/out/MQCLI nack --message-key msg:telemetry:12345678 --reason "processing failed" --requeue
```

메시지를 ready 상태로 되돌려 재시도합니다.

#### DLQ로 이동

```bash
MQCLI nack --message-key <message-key> --reason <reason>
```

**예시:**
```bash
./build/out/MQCLI nack --message-key msg:telemetry:12345678 --reason "validation error"
```

메시지를 DLQ (Dead Letter Queue)로 이동합니다.

## 공통 옵션

- `--db <path>`: SQLite 데이터베이스 경로 (기본값: `<binary_dir>/../data/yirangmq.db`)
- `--schema <path>`: SQLite 스키마 파일 경로 (기본값: `<binary_dir>/sqlite_schema.sql`)

## 전체 워크플로우 예시

```bash
# 1. 메시지 발행
./build/out/MQCLI publish --queue telemetry --message '{"temp":36.5,"rpm":1200}' --priority 5

# 2. 큐 상태 확인
./build/out/MQCLI status --queue telemetry

# 3. 메시지 소비
./build/out/MQCLI consume --queue telemetry --consumer-id consumer-01

# 4a. 성공 처리
./build/out/MQCLI ack --message-key msg:telemetry:xxxxx

# 또는 4b. 실패 처리 (재시도)
./build/out/MQCLI nack --message-key msg:telemetry:xxxxx --reason "temporary failure" --requeue

# 또는 4c. 실패 처리 (DLQ로 이동)
./build/out/MQCLI nack --message-key msg:telemetry:xxxxx --reason "invalid format"

# 5. 최종 상태 확인
./build/out/MQCLI status --queue telemetry
```

## 자동 테스트

프로젝트 루트에 `test_mq.sh` 스크립트가 있습니다:

```bash
chmod +x test_mq.sh
./test_mq.sh
```

이 스크립트는 전체 publish → consume → ack/nack 워크플로우를 자동으로 테스트합니다.
