#!/bin/bash
# Yi-Rang MQ 데모 러너 — 데몬과 데모 프로세스로 6개 장면을 순서대로 재현한다.
#
# 합격 판정은 데모 프로세스의 종료 코드와 데몬이 보고하는 큐 상태로만 한다. 프로세스
# stdout 을 파싱하지 않으므로 출력 문구가 바뀌어도 판정이 흔들리지 않는다.
#
# Usage:
#   ./run-demo.sh                # 데몬 기동 여부를 스스로 판단해 전체 실행
#   ./run-demo.sh --act 3        # 특정 장면만
#   ./run-demo.sh --attach       # 이미 떠 있는 데몬에 붙는다 (새로 띄우지 않음)
#   ./run-demo.sh --reset        # 데모 전용 데이터를 지우고 시작
#   ./run-demo.sh --bin-dir /app # 실행 파일 위치 지정 (Docker)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# build/out 또는 Docker /app 으로 복사된 경우엔 스크립트 옆에 데몬이 있다.
if [[ -x "$SCRIPT_DIR/MainMQ" ]]; then
	DEFAULT_BIN_DIR="$SCRIPT_DIR"
else
	DEFAULT_BIN_DIR="$SCRIPT_DIR/../../build/out"
fi

BIN_DIR="$DEFAULT_BIN_DIR"
QUEUE="telemetry"
ONLY_ACT=""
ATTACH=false
RESET=false

print_usage()
{
	sed -n '2,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--bin-dir) BIN_DIR="$2"; shift 2 ;;
		--queue)   QUEUE="$2"; shift 2 ;;
		--act)     ONLY_ACT="$2"; shift 2 ;;
		--attach)  ATTACH=true; shift ;;
		--reset)   RESET=true; shift ;;
		-h|--help) print_usage; exit 0 ;;
		*) echo "알 수 없는 옵션: $1" >&2; exit 2 ;;
	esac
done

RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'
CYAN=$'\033[0;36m'
NC=$'\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
FAILED_ACTS=""

log_info()  { echo "${CYAN}[..]${NC} $1"; }
log_warn()  { echo "${YELLOW}[!!]${NC} $1"; }
log_error() { echo "${RED}[XX]${NC} $1"; }
log_pass()  { echo "${GREEN}[OK]${NC} $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
log_fail()  { echo "${RED}[NG]${NC} $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); FAILED_ACTS="${FAILED_ACTS}\n  - $1"; }

scene()
{
	echo ""
	echo "${CYAN}══════════════════════════════════════════════════════════════${NC}"
	echo "${CYAN}  $1${NC}"
	echo "${CYAN}══════════════════════════════════════════════════════════════${NC}"
}

# ── 실행 위치 확정 ────────────────────────────────────────────────────────────
# IPC 루트(./ipc)는 작업 디렉터리 기준 상대 경로다. 데몬과 데모가 같은 디렉터리에서
# 실행되지 않으면 서로 다른 ipc 트리를 보게 되고, 오류 없이 타임아웃만 발생한다.
if [[ ! -d "$BIN_DIR" ]]; then
	log_error "빌드 산출물 디렉터리를 찾을 수 없습니다: $BIN_DIR"
	log_error "먼저 ./build.sh 로 빌드하거나 --bin-dir 로 경로를 지정하십시오."
	exit 2
fi

cd "$BIN_DIR" || exit 2
BIN_DIR="$PWD"

for binary in MainMQ yirangmq-demo-sensor yirangmq-demo-worker yirangmq-demo-monitor; do
	if [[ ! -x "./$binary" ]]; then
		log_error "실행 파일이 없습니다: $BIN_DIR/$binary"
		exit 2
	fi
done

SENSOR="./yirangmq-demo-sensor"
WORKER="./yirangmq-demo-worker"
MONITOR="./yirangmq-demo-monitor"

DAEMON_PID=""
WATCH_PID=""

probe_daemon() { $MONITOR once --queue "$QUEUE" --timeout 1500 >/dev/null 2>&1; }

stop_watch()
{
	if [[ -n "$WATCH_PID" ]]; then
		kill "$WATCH_PID" 2>/dev/null
		wait "$WATCH_PID" 2>/dev/null
		WATCH_PID=""
	fi
}

start_watch()
{
	$MONITOR watch --queue "$QUEUE" --interval-ms 300 &
	WATCH_PID=$!
}

cleanup()
{
	local status=$?
	trap - EXIT INT TERM

	stop_watch

	if [[ -n "$DAEMON_PID" ]]; then
		log_info "데몬 종료 (SIGTERM)"
		kill -TERM "$DAEMON_PID" 2>/dev/null
		wait "$DAEMON_PID" 2>/dev/null
	fi

	exit $status
}
trap cleanup EXIT INT TERM

echo "${CYAN}╔════════════════════════════════════════════════════════════╗${NC}"
echo "${CYAN}║            Yi-Rang MQ 동작 시연 (run-demo.sh)              ║${NC}"
echo "${CYAN}╚════════════════════════════════════════════════════════════╝${NC}"
log_info "실행 위치 : $BIN_DIR"
log_info "IPC 루트  : $BIN_DIR/ipc"
log_info "대상 큐   : $QUEUE"

# ── 데몬 준비 ────────────────────────────────────────────────────────────────
if probe_daemon; then
	log_info "이미 응답하는 데몬이 있어 그대로 사용합니다"
	log_warn "이전 실습 메시지가 큐에 남아 있으면 장면 판정이 흔들릴 수 있습니다"
else
	if [[ "$ATTACH" == true ]]; then
		log_error "--attach 를 주었으나 응답하는 데몬이 없습니다"
		exit 2
	fi

	if [[ "$RESET" == true ]]; then
		log_info "데모 전용 데이터를 삭제합니다"
		rm -rf ./data/demo ./data/demo.db ./data/demo.db-wal ./data/demo.db-shm
	fi

	mkdir -p ./data/demo

	# 데모 전용 DB 로 띄워 사용자의 기존 데이터를 건드리지 않는다. 데몬에는 --ipc-root
	# 옵션이 없으므로 IPC 루트는 ./ipc 를 그대로 쓴다.
	log_info "데몬을 기동합니다 (--db-path ./data/demo.db, 콘솔 로그 끔)"
	./MainMQ --db-path ./data/demo.db --data-root ./data/demo --log-root ./logs --write-console-log 0 &
	DAEMON_PID=$!

	READY=false
	for _ in $(seq 1 40); do
		if probe_daemon; then READY=true; break; fi
		sleep 0.25
	done

	if [[ "$READY" != true ]]; then
		log_error "데몬이 응답하지 않습니다"
		exit 1
	fi

	# 장면 합격 수에 섞이지 않도록 정보 로그로 남긴다.
	log_info "데몬 기동 완료 (PID $DAEMON_PID)"
fi

$MONITOR once --queue "$QUEUE"

want_act() { [[ -z "$ONLY_ACT" || "$ONLY_ACT" == "$1" ]]; }

# ── 장면 1: 발행 → 소비 → 확정 ───────────────────────────────────────────────
act_1()
{
	scene "장면 1 — 센서가 발행하고 워커가 처리한다"

	$SENSOR --queue "$QUEUE" --count 3 --interval-ms 200 || { log_fail "장면 1: 발행 실패"; return; }

	if ! $MONITOR wait --queue "$QUEUE" --state ready --count 3 --timeout-ms 10000; then
		log_fail "장면 1: ready 3건이 되지 않았습니다"
		return
	fi

	if ! $WORKER --queue "$QUEUE" --consumer-id demo-worker-01 --max-messages 3 --idle-exit-ms 5000; then
		log_fail "장면 1: 워커가 예상 외 결과로 종료했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state ready --max 0 --timeout-ms 5000; then
		log_fail "장면 1: 큐가 비워지지 않았습니다"
		return
	fi

	log_pass "장면 1: 3건이 발행되고 모두 ack 되었습니다"
}

# ── 장면 2: 스키마 검증 ──────────────────────────────────────────────────────
act_2()
{
	scene "장면 2 — 스키마를 위반한 프레임은 발행 시점에 거부된다"

	log_info "deviceId 를 뺀 프레임을 일부러 발행합니다 (telemetry 스키마의 필수 필드)"

	if ! $SENSOR --queue "$QUEUE" --count 1 --bad-frame 0; then
		log_fail "장면 2: 거부되어야 할 프레임이 통과했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state ready --max 0 --timeout-ms 3000; then
		log_fail "장면 2: 거부된 프레임이 큐에 적재되었습니다"
		return
	fi

	log_pass "장면 2: ERR_VALIDATION_FAILED 로 거부되고 큐에 적재되지 않았습니다"
}

# ── 장면 3: lease 만료 재배달 + 늦은 ack 거부 ────────────────────────────────
act_3()
{
	scene "장면 3 — 처리 중 멈춘 워커의 메시지가 다른 워커에게 넘어간다"

	$SENSOR --queue "$QUEUE" --count 1 || { log_fail "장면 3: 발행 실패"; return; }

	log_info "worker-01 이 가시성 2초로 소비한 뒤 6초 멈춥니다"
	start_watch

	$WORKER --queue "$QUEUE" --consumer-id demo-worker-01 --visibility 2 --stall-ms 6000 \
		--max-messages 1 --idle-exit-ms 15000 &
	local stalled_pid=$!

	# 대기 워커는 worker-01 이 lease 를 잡은 뒤에 띄운다. 그래야 누가 먼저 집을지에
	# 대한 경쟁이 사라지고 장면이 결정적으로 재현된다.
	if ! $MONITOR wait --queue "$QUEUE" --state inflight --count 1 --timeout-ms 15000; then
		stop_watch
		kill "$stalled_pid" 2>/dev/null
		log_fail "장면 3: worker-01 이 lease 를 잡지 못했습니다"
		return
	fi

	log_info "이제 대기 워커를 띄웁니다 — lease 가 만료되면 이쪽이 이어받아야 합니다"
	$WORKER --queue "$QUEUE" --consumer-id demo-worker-standby --visibility 15 \
		--max-messages 1 --idle-exit-ms 15000 &
	local standby_pid=$!

	wait "$stalled_pid"; local stalled_rc=$?
	wait "$standby_pid"; local standby_rc=$?

	stop_watch

	if [[ $stalled_rc -ne 0 ]]; then
		log_fail "장면 3: 만료된 lease 의 ack 이 거부되지 않았습니다"
		return
	fi

	if [[ $standby_rc -ne 0 ]]; then
		log_fail "장면 3: 대기 워커가 재배달을 처리하지 못했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state ready --max 0 --timeout-ms 5000; then
		log_fail "장면 3: 메시지가 정산되지 않고 큐에 남았습니다"
		return
	fi

	log_pass "장면 3: 재배달로 대기 워커가 처리하고, 늦은 ack 은 거부되었습니다"
}

# ── 장면 4: 재시도 소진 → DLQ ────────────────────────────────────────────────
act_4()
{
	scene "장면 4 — 반복 실패한 메시지는 DLQ 로 격리된다"

	log_info "워커가 처리 불가로 판정할 fault 프레임을 발행합니다"

	$SENSOR --queue "$QUEUE" --count 1 --fault-frame 0 || { log_fail "장면 4: 발행 실패"; return; }

	log_info "워커가 nack requeue=true 로 재시도를 요청하고, 정책 한도에 닿으면 데몬이 DLQ 로 보냅니다"

	if ! $WORKER --queue "$QUEUE" --consumer-id demo-worker-01 --idle-exit-ms 4000; then
		log_fail "장면 4: 워커가 예상 외 결과로 종료했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state dlq --count 1 --timeout-ms 5000; then
		log_fail "장면 4: DLQ 로 이동하지 않았습니다"
		return
	fi

	$MONITOR dlq --queue "$QUEUE"

	log_pass "장면 4: 재시도가 소진되어 DLQ 로 격리되었습니다"
}

# ── 장면 5: DLQ 재처리 ───────────────────────────────────────────────────────
act_5()
{
	scene "장면 5 — 원인을 조치한 뒤 DLQ 메시지를 재투입한다"

	if ! $MONITOR reprocess --queue "$QUEUE" --first; then
		log_fail "장면 5: 재처리 요청이 실패했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state ready --count 1 --timeout-ms 5000; then
		log_fail "장면 5: 재처리한 메시지가 ready 로 돌아오지 않았습니다"
		return
	fi

	log_info "설비를 정비했다고 보고 --ignore-fault 로 다시 처리합니다"

	if ! $WORKER --queue "$QUEUE" --consumer-id demo-worker-01 --ignore-fault \
		--max-messages 1 --idle-exit-ms 5000; then
		log_fail "장면 5: 재투입 처리에 실패했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state dlq --max 0 --timeout-ms 5000; then
		log_fail "장면 5: DLQ 가 비워지지 않았습니다"
		return
	fi

	log_pass "장면 5: reprocess 후 attempt 가 초기화되고 정상 처리되었습니다"
}

# ── 장면 6: 지정 배달 ────────────────────────────────────────────────────────
act_6()
{
	scene "장면 6 — 지정한 워커만 받을 수 있다"

	$SENSOR --queue "$QUEUE" --count 1 --target demo-worker-01 || { log_fail "장면 6: 발행 실패"; return; }

	log_info "대상이 아닌 워커를 먼저 띄웁니다 — 이 워커는 끝까지 받지 못해야 합니다"

	$WORKER --queue "$QUEUE" --consumer-id demo-worker-other --max-messages 1 --idle-exit-ms 3000

	if ! $MONITOR wait --queue "$QUEUE" --state ready --count 1 --timeout-ms 3000; then
		log_fail "장면 6: 대상이 아닌 워커가 메시지를 가져갔습니다"
		return
	fi

	log_info "지정된 워커를 띄웁니다"

	if ! $WORKER --queue "$QUEUE" --consumer-id demo-worker-01 --max-messages 1 --idle-exit-ms 5000; then
		log_fail "장면 6: 지정된 워커가 처리하지 못했습니다"
		return
	fi

	if ! $MONITOR wait --queue "$QUEUE" --state ready --max 0 --timeout-ms 5000; then
		log_fail "장면 6: 메시지가 정산되지 않았습니다"
		return
	fi

	log_pass "장면 6: 지정 대상만 수신하고 다른 워커는 받지 못했습니다"
}

for act in 1 2 3 4 5 6; do
	if want_act "$act"; then
		"act_$act"
	fi
done

# ── 결과 ─────────────────────────────────────────────────────────────────────
scene "시연 결과"

echo "  통과 : ${GREEN}${PASS_COUNT}${NC}"
echo "  실패 : ${RED}${FAIL_COUNT}${NC}"

if [[ $FAIL_COUNT -gt 0 ]]; then
	echo ""
	echo "${RED}실패한 장면:${NC}"
	printf '%b\n' "$FAILED_ACTS"
	exit 1
fi

echo ""
echo "${GREEN}모든 장면이 기대한 대로 재현되었습니다${NC}"
exit 0
