#!/bin/bash

# ==============================================================================
#   Yi-Rang MQ Test Script
# ==============================================================================
#
# 흩어진 검증 진입점(ctest / docker-compose.sh test / run-demo.sh)을 하나로 묶는다.
#
# 세 단계를 실행한다.
#   unit         gtest 단위 스위트 (ctest)
#   integration  데몬 + CLI 종단 시나리오 (IT-01~09 + graceful shutdown)
#   demo         애플리케이션 계층 프로세스 시연 장면 (DS-01~06)
#
# 기본 엔진은 Docker다. macOS에서는 efsw의 vcpkg 빌드가 arm64-osx에서 실패해 로컬
# 빌드가 불가하므로, Docker 없이는 검증할 수 없다.

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"

ENGINE="docker"
RUN_UNIT=false
RUN_INTEGRATION=false
RUN_DEMO=false
ANY_PHASE_SELECTED=false
REBUILD_IMAGES=true
GTEST_FILTER=""
DEMO_ACT=""
LIST_ONLY=false

UNIT_IMAGE="yirangmq-test:unit"
INTEGRATION_IMAGE="yirangmq-test:integration"

print_usage() {
    echo "==============================================================================="
    echo "  Yi-Rang MQ Test Script"
    echo "==============================================================================="
    echo ""
    echo "DESCRIPTION:"
    echo "  단위 테스트, 데몬+CLI 통합 시나리오, 시연 장면을 한 번에 실행하고 집계한다."
    echo "  단계를 지정하지 않으면 셋 모두 실행한다."
    echo ""
    echo "USAGE:"
    echo "  $0 [options]"
    echo ""
    echo "PHASES:"
    echo "  -u, --unit          단위 테스트만 (ctest, 12개 스위트)"
    echo "  -i, --integration   통합 시나리오만 (IT-01~09 + graceful shutdown)"
    echo "  -m, --demo          시연 장면만 (DS-01~06)"
    echo ""
    echo "OPTIONS:"
    echo "      --local         Docker 대신 기존 $BUILD_DIR 트리를 사용한다 (Linux 전용)"
    echo "      --docker        Docker 엔진을 사용한다 (기본값)"
    echo "  -n, --no-build      Docker 이미지를 다시 빌드하지 않고 기존 이미지를 쓴다"
    echo "  -f, --filter PAT    단위 테스트 필터 (ctest -R 정규식)"
    echo "  -l, --list          단위 테스트 이름 목록을 출력하고 종료한다"
    echo "      --act N         시연 장면 하나만 실행 (1~6)"
    echo "  -h, --help          도움말"
    echo ""
    echo "EXAMPLES:"
    echo "  $0                                  # 전체 (unit + integration + demo)"
    echo "  $0 --unit --filter 'QueueManagerTest\\.'"
    echo "  $0 --unit --filter 'SQLiteAdapterTest\\.Lease'"
    echo "  $0 --demo --act 3                   # lease 만료 + fencing 장면만"
    echo "  $0 --no-build                       # 이미지 재빌드 없이 전체 재실행"
    echo "  $0 --local --unit                   # Linux에서 기존 build/ 트리로 ctest"
    echo ""
    echo "NOTES:"
    echo "  * 필터는 ctest 테스트 이름과 매칭한다. 이 이름은 소스 파일명이 아니라 gtest"
    echo "    스위트명이다 — 예를 들어 Tests/TestQueueManager.cpp 의 케이스는"
    echo "    'QueueManagerTest.<케이스>' 로 등록된다. 'TestQueueManager' 로는 하나도"
    echo "    매칭되지 않으며, 그 경우 --no-tests=error 에 의해 통과가 아니라 실패로 잡힌다."
    echo "    이름 목록은 --list 로 확인한다."
    echo "  * macOS 로컬 빌드는 efsw(arm64-osx) 실패로 불가하다. --local 은 Linux 전용이다."
    echo "  * --local 에서는 통합 시나리오를 건너뛴다. integration-test.sh 가 컨테이너의"
    echo "    /app 경로를 전제하기 때문이다."
    echo ""
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -u|--unit)        RUN_UNIT=true;        ANY_PHASE_SELECTED=true; shift ;;
        -i|--integration) RUN_INTEGRATION=true; ANY_PHASE_SELECTED=true; shift ;;
        -m|--demo)        RUN_DEMO=true;        ANY_PHASE_SELECTED=true; shift ;;
        --local)          ENGINE="local"; shift ;;
        --docker)         ENGINE="docker"; shift ;;
        -n|--no-build)    REBUILD_IMAGES=false; shift ;;
        -f|--filter)      GTEST_FILTER="${2:-}"; shift 2 ;;
        -l|--list)        LIST_ONLY=true; shift ;;
        --act)            DEMO_ACT="${2:-}"; shift 2 ;;
        -h|--help)        print_usage; exit 0 ;;
        *) echo -e "${RED}Unknown option: $1${NC}" >&2; echo "Try '$0 --help'." >&2; exit 2 ;;
    esac
done

if [[ "$LIST_ONLY" == true ]]; then
    if [[ "$ENGINE" == "local" ]]; then
        ctest --test-dir "$BUILD_DIR" -N
        exit $?
    fi

    if [[ "$REBUILD_IMAGES" == true ]]; then
        docker build --target test-runner -t "$UNIT_IMAGE" -f "$SCRIPT_DIR/docker/Dockerfile" "$SCRIPT_DIR" >/dev/null || exit 1
    fi

    # ENTRYPOINT 가 ctest 이지만 --test-dir 이 이미 박혀 있어 -N 만 덧붙이면 되고,
    # 이름 목록만 뽑을 때는 entrypoint 를 그대로 써도 무해하다.
    docker run --rm "$UNIT_IMAGE" -N
    exit $?
fi

if [[ "$ANY_PHASE_SELECTED" != true ]]; then
    RUN_UNIT=true
    RUN_INTEGRATION=true
    RUN_DEMO=true
fi

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
RESULTS=""

log_info() { echo -e "${CYAN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }

record_pass() { PASS_COUNT=$((PASS_COUNT + 1)); RESULTS="${RESULTS}\n  ${GREEN}PASS${NC}  $1"; echo -e "${GREEN}[PASS]${NC} $1"; }
record_fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); RESULTS="${RESULTS}\n  ${RED}FAIL${NC}  $1"; echo -e "${RED}[FAIL]${NC} $1"; }
record_skip() { SKIP_COUNT=$((SKIP_COUNT + 1)); RESULTS="${RESULTS}\n  ${YELLOW}SKIP${NC}  $1 — $2"; echo -e "${YELLOW}[SKIP]${NC} $1 — $2"; }

phase() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

echo -e "${CYAN}╔═════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║                 Yi-Rang MQ Test Runner                      ║${NC}"
echo -e "${CYAN}╚═════════════════════════════════════════════════════════════╝${NC}"
log_info "엔진   : $ENGINE"
log_info "단계   : $([[ "$RUN_UNIT" == true ]] && printf 'unit ')$([[ "$RUN_INTEGRATION" == true ]] && printf 'integration ')$([[ "$RUN_DEMO" == true ]] && printf 'demo')"
[[ -n "$GTEST_FILTER" ]] && log_info "필터   : $GTEST_FILTER"
[[ -n "$DEMO_ACT" ]] && log_info "장면   : $DEMO_ACT"

# ==============================================================================
#   Docker 엔진
# ==============================================================================
run_docker_engine() {
    if ! command -v docker >/dev/null 2>&1; then
        log_error "docker 명령을 찾을 수 없습니다. --local 을 쓰거나 Docker를 설치하십시오."
        exit 2
    fi

    if ! docker info >/dev/null 2>&1; then
        log_error "Docker 데몬에 연결할 수 없습니다."
        exit 2
    fi

    # 단위 이미지는 test-runner 스테이지(ENTRYPOINT ctest), 통합 이미지는 integration-test
    # 스테이지다. 통합 이미지는 build/out 전체를 복사하므로 데모 바이너리와 run-demo.sh도
    # 함께 들어 있어, 시연 단계도 이 이미지로 실행한다.
    if [[ "$REBUILD_IMAGES" == true ]]; then
        if [[ "$RUN_UNIT" == true ]]; then
            phase "이미지 빌드 — test-runner"
            if ! docker build --target test-runner -t "$UNIT_IMAGE" -f "$SCRIPT_DIR/docker/Dockerfile" "$SCRIPT_DIR"; then
                record_fail "이미지 빌드 (test-runner)"
                return
            fi
        fi

        if [[ "$RUN_INTEGRATION" == true || "$RUN_DEMO" == true ]]; then
            phase "이미지 빌드 — integration-test"
            if ! docker build --target integration-test -t "$INTEGRATION_IMAGE" -f "$SCRIPT_DIR/docker/Dockerfile" "$SCRIPT_DIR"; then
                record_fail "이미지 빌드 (integration-test)"
                return
            fi
        fi
    else
        log_warn "이미지 재빌드를 생략합니다 (--no-build). 소스 변경이 반영되지 않을 수 있습니다."
    fi

    if [[ "$RUN_UNIT" == true ]]; then
        phase "단계 1/3 — 단위 테스트 (ctest)"

        # ENTRYPOINT 가 ctest 이므로 추가 인자가 그대로 붙는다. --no-tests=error 가 이미
        # 지정되어 있어, 필터가 아무것도 매칭하지 않으면 통과가 아니라 실패로 잡힌다.
        local unit_rc=0
        if [[ -n "$GTEST_FILTER" ]]; then
            docker run --rm "$UNIT_IMAGE" -R "$GTEST_FILTER" || unit_rc=$?
        else
            docker run --rm "$UNIT_IMAGE" || unit_rc=$?
        fi

        if [[ $unit_rc -eq 0 ]]; then record_pass "단위 테스트"; else record_fail "단위 테스트"; fi
    fi

    if [[ "$RUN_INTEGRATION" == true ]]; then
        phase "단계 2/3 — 통합 시나리오 (IT-01~09)"

        if docker run --rm "$INTEGRATION_IMAGE" --integration-only; then
            record_pass "통합 시나리오"
        else
            record_fail "통합 시나리오"
        fi
    fi

    if [[ "$RUN_DEMO" == true ]]; then
        phase "단계 3/3 — 시연 장면 (DS-01~06)"

        local demo_args=(--reset)
        [[ -n "$DEMO_ACT" ]] && demo_args+=(--act "$DEMO_ACT")

        if docker run --rm --entrypoint /app/run-demo.sh "$INTEGRATION_IMAGE" "${demo_args[@]}"; then
            record_pass "시연 장면"
        else
            record_fail "시연 장면"
        fi
    fi
}

# ==============================================================================
#   로컬 엔진 (Linux)
# ==============================================================================
run_local_engine() {
    if [[ ! -d "$BUILD_DIR" ]]; then
        log_error "빌드 디렉터리가 없습니다: $BUILD_DIR"
        log_error "먼저 ./build.sh 로 빌드하십시오."
        exit 2
    fi

    if [[ "$(uname -s)" == "Darwin" ]]; then
        log_warn "macOS 에서는 efsw(arm64-osx) vcpkg 빌드가 실패해 로컬 빌드가 유지되지 않습니다."
        log_warn "결과가 낡은 산출물에 근거할 수 있습니다. Docker 엔진을 권장합니다."
    fi

    if [[ "$RUN_UNIT" == true ]]; then
        phase "단계 1/3 — 단위 테스트 (ctest)"

        # --no-tests=error: ctest 는 아무것도 매칭하지 않아도 0 을 반환하므로,
        # 빈 실행이 통과로 집계되는 것을 막는다.
        local unit_rc=0
        if [[ -n "$GTEST_FILTER" ]]; then
            ctest --test-dir "$BUILD_DIR" --output-on-failure --no-tests=error -R "$GTEST_FILTER" || unit_rc=$?
        else
            ctest --test-dir "$BUILD_DIR" --output-on-failure --no-tests=error || unit_rc=$?
        fi

        if [[ $unit_rc -eq 0 ]]; then record_pass "단위 테스트"; else record_fail "단위 테스트"; fi
    fi

    if [[ "$RUN_INTEGRATION" == true ]]; then
        phase "단계 2/3 — 통합 시나리오"
        record_skip "통합 시나리오" "docker/integration-test.sh 가 컨테이너의 /app 경로를 전제한다. --docker 로 실행하십시오"
    fi

    if [[ "$RUN_DEMO" == true ]]; then
        phase "단계 3/3 — 시연 장면 (DS-01~06)"

        local runner="$BUILD_DIR/out/run-demo.sh"
        if [[ ! -x "$runner" ]]; then
            record_skip "시연 장면" "$runner 가 없습니다. ./build.sh 로 빌드하십시오"
        else
            local demo_args=(--reset)
            [[ -n "$DEMO_ACT" ]] && demo_args+=(--act "$DEMO_ACT")

            if "$runner" "${demo_args[@]}"; then
                record_pass "시연 장면"
            else
                record_fail "시연 장면"
            fi
        fi
    fi
}

if [[ "$ENGINE" == "docker" ]]; then
    run_docker_engine
else
    run_local_engine
fi

# ==============================================================================
#   결과
# ==============================================================================
phase "결과 요약"

printf '%b\n' "$RESULTS"
echo ""
echo -e "  통과: ${GREEN}${PASS_COUNT}${NC}   실패: ${RED}${FAIL_COUNT}${NC}   건너뜀: ${YELLOW}${SKIP_COUNT}${NC}"
echo ""

if [[ $FAIL_COUNT -gt 0 ]]; then
    echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}  검증 실패${NC}"
    echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
    exit 1
fi

if [[ $PASS_COUNT -eq 0 ]]; then
    echo -e "${YELLOW}실행된 단계가 없습니다.${NC}"
    exit 2
fi

echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  모든 검증 통과${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
exit 0
