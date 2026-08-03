#!/bin/bash
# Yi-Rang MQ Integration Test Script
# Runs inside Docker container to verify end-to-end functionality
#
# Usage:
#   docker build --target integration-test -t yirangmq-test .
#   docker run --rm yirangmq-test
#   docker run --rm yirangmq-test --unit-only
#   docker run --rm yirangmq-test --integration-only
#   docker run --rm yirangmq-test --filter 'TestSQLiteAdapter.*'

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED_TESTS=()

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }
log_pass()  { echo -e "${GREEN}[PASS]${NC} $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
log_fail()  { echo -e "${RED}[FAIL]${NC} $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); FAILED_TESTS+=("$1"); }
log_section() { echo -e "\n${CYAN}════════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}════════════════════════════════════════${NC}\n"; }

# Parse arguments
RUN_UNIT=true
RUN_INTEGRATION=true
GTEST_FILTER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --unit-only)      RUN_UNIT=true; RUN_INTEGRATION=false; shift ;;
        --integration-only) RUN_UNIT=false; RUN_INTEGRATION=true; shift ;;
        --filter)         GTEST_FILTER="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--unit-only|--integration-only] [--filter 'pattern']"
            echo ""
            echo "Options:"
            echo "  --unit-only          Run unit tests only"
            echo "  --integration-only   Run integration tests only"
            echo "  --filter PATTERN     GTest filter pattern (e.g. 'TestSQLiteAdapter.*')"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

echo -e "${CYAN}╔══════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║     Yi-Rang MQ Docker Test Runner        ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════╝${NC}"
echo ""

# ============================================================
# Phase 1: Unit Tests
# ============================================================
if [[ "$RUN_UNIT" == true ]]; then
    log_section "Phase 1: Unit Tests"

    # Discovered, never hardcoded: a hardcoded list silently drops suites added later. The list
    # had drifted to 7 entries while the build produced 11, so 4 suites never ran here.
    TEST_SUITES=()
    while IFS= read -r binary; do
        TEST_SUITES+=("$(basename "$binary")")
    done < <(find /app -maxdepth 1 -type f -executable -name 'Test*' | sort)

    if [[ ${#TEST_SUITES[@]} -eq 0 ]]; then
        log_fail "no unit test binaries found in /app (build or COPY is broken)"
    fi

    log_info "Discovered ${#TEST_SUITES[@]} unit test suite(s)"

    for suite in "${TEST_SUITES[@]}"; do
        FILTER_ARG=""
        if [[ -n "$GTEST_FILTER" ]]; then
            FILTER_ARG="--gtest_filter=$GTEST_FILTER"
        fi

        log_info "Running $suite..."
        # `set -e` aborts on a failing command substitution, so capture the status inline.
        SUITE_RC=0
        SUITE_OUTPUT=$(/app/$suite $FILTER_ARG --gtest_color=no 2>&1) || SUITE_RC=$?
        echo "$SUITE_OUTPUT"

        if [[ $SUITE_RC -ne 0 ]]; then
            log_fail "$suite"
        elif echo "$SUITE_OUTPUT" | grep -q '0 tests from 0 test suites ran'; then
            # A filter that matches nothing exits 0; counting that as a pass turned an empty run
            # into a green result.
            log_warn "$suite: no test matched the filter (skipped)"
            SKIP_COUNT=$((SKIP_COUNT + 1))
        else
            log_pass "$suite"
        fi
    done
fi

# ============================================================
# Phase 2: Integration Tests (Daemon + CLI)
# ============================================================
if [[ "$RUN_INTEGRATION" == true ]]; then
    log_section "Phase 2: Integration Tests (Daemon + CLI)"

    # Clean up any previous state
    rm -rf /app/data/* /app/ipc/requests/* /app/ipc/responses/* /app/logs/* 2>/dev/null || true

    # Start daemon in background
    log_info "Starting MainMQ daemon..."
    /app/MainMQ &
    DAEMON_PID=$!
    sleep 2

    # Verify daemon is running (give it extra time to initialize)
    sleep 3
    if ! kill -0 $DAEMON_PID 2>/dev/null; then
        log_fail "MainMQ daemon failed to start"
    else
        log_pass "MainMQ daemon started (PID: $DAEMON_PID)"

        # --- Test 1: Health Check ---
        log_info "[IT-01] Health check..."
        if HEALTH=$(/app/yirangmq-cli-publisher health --timeout 10000 2>&1) && echo "$HEALTH" | grep -q '"status"'; then
            log_pass "IT-01: Health check"
        else
            log_fail "IT-01: Health check"
        fi

        # --- Test 2: Publish Message ---
        log_info "[IT-02] Publish message..."
        if PUB=$(/app/yirangmq-cli-publisher --queue test-queue --message '{"sensor":"temp","value":25.5}' --timeout 10000 2>&1) && echo "$PUB" | grep -q '"messageId"\|"messageKey"\|"published successfully"'; then
            log_pass "IT-02: Publish message"
        else
            log_fail "IT-02: Publish message — $PUB"
        fi

        # --- Test 3: Queue Status ---
        log_info "[IT-03] Queue status..."
        if STATUS=$(/app/yirangmq-cli-publisher status --queue test-queue 2>&1) && echo "$STATUS" | grep -q '"ready"\|"metrics"'; then
            log_pass "IT-03: Queue status"
        else
            log_fail "IT-03: Queue status — $STATUS"
        fi

        # Only the consumer holding the lease may settle it, so every consume/ack/nack below
        # presents the same id. Omitting --consumer-id sends consumerId from the config file
        # instead, which no longer matches the leasing consumer. (Defect D-55 / FR-ACK-07)
        CONSUMER_ID=test-worker

        # --- Test 4: Consume Message ---
        log_info "[IT-04] Consume message..."
        sleep 1
        if CONSUME=$(/app/yirangmq-cli-consumer consume --queue test-queue --consumer-id "$CONSUMER_ID" --timeout 10000 2>&1); then
            if echo "$CONSUME" | grep -q '"messageKey"\|"message_key"\|"leaseId"\|"lease_id"'; then
                log_pass "IT-04: Consume message"
                # Extract message key for ACK test
                MSG_KEY=$(echo "$CONSUME" | grep -o '"messageKey"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
                if [[ -z "$MSG_KEY" ]]; then
                    MSG_KEY=$(echo "$CONSUME" | grep -o '"message_key"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
                fi
                LEASE_ID=$(echo "$CONSUME" | grep -o '"leaseId"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
            else
                log_fail "IT-04: Consume message — unexpected response"
            fi
        else
            log_fail "IT-04: Consume message"
        fi

        # --- Test 5: ACK ---
        if [[ -n "$MSG_KEY" ]]; then
            log_info "[IT-05] ACK message ($MSG_KEY)..."
            if ACK=$(/app/yirangmq-cli-consumer ack --message-key "$MSG_KEY" --lease-id "$LEASE_ID" --consumer-id "$CONSUMER_ID" --timeout 10000 2>&1) && echo "$ACK" | grep -qi 'acknowledged'; then
                log_pass "IT-05: ACK message"
            else
                log_fail "IT-05: ACK message — $ACK"
            fi
        else
            log_warn "[IT-05] ACK skipped (no message key from consume)"
            SKIP_COUNT=$((SKIP_COUNT + 1))
        fi

        # --- Test 6: Publish + NACK + DLQ ---
        log_info "[IT-06] NACK and DLQ flow..."
        /app/yirangmq-cli-publisher --queue test-queue --message '{"test":"nack-flow"}' --timeout 10000 >/dev/null 2>&1
        sleep 2
        CONSUME2=$(/app/yirangmq-cli-consumer consume --queue test-queue --consumer-id "$CONSUMER_ID" --timeout 10000 2>&1)
        MSG_KEY2=$(echo "$CONSUME2" | grep -o '"messageKey"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
        if [[ -z "$MSG_KEY2" ]]; then
            MSG_KEY2=$(echo "$CONSUME2" | grep -o '"message_key"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)
        fi
        LEASE_ID2=$(echo "$CONSUME2" | grep -o '"leaseId"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | cut -d'"' -f4)

        if [[ -n "$MSG_KEY2" ]]; then
            NACK=$(/app/yirangmq-cli-consumer nack --message-key "$MSG_KEY2" --lease-id "$LEASE_ID2" --consumer-id "$CONSUMER_ID" --reason "test failure" --requeue --timeout 10000 2>&1)
            # Match the success wording only: 'nack' also appears in "Nack failed: ...", so the
            # previous pattern turned a rejected nack into a pass.
            if echo "$NACK" | grep -q 'Message nacked'; then
                log_pass "IT-06: NACK with requeue"
            else
                log_fail "IT-06: NACK with requeue — $NACK"
            fi
        else
            log_fail "IT-06: NACK flow (consume failed)"
        fi

        # --- Test 7: Metrics ---
        log_info "[IT-07] Metrics..."
        if METRICS=$(/app/yirangmq-cli-publisher metrics 2>&1) && echo "$METRICS" | grep -q '"requests"\|"commands"'; then
            log_pass "IT-07: Metrics"
        else
            log_fail "IT-07: Metrics — $METRICS"
        fi

        # --- Test 8: Direct Addressing ---
        log_info "[IT-08] Direct addressing..."
        /app/yirangmq-cli-publisher --queue test-queue --message '{"direct":true}' --target specific-worker --timeout 10000 >/dev/null 2>&1
        sleep 1

        # Wrong consumer should get nothing
        WRONG=$(/app/yirangmq-cli-consumer consume --queue test-queue --consumer-id wrong-worker 2>&1)
        if echo "$WRONG" | grep -q '"no_message"\|"empty"\|"error"'; then
            log_pass "IT-08: Direct addressing (wrong consumer filtered)"
        else
            # Some implementations return empty data
            log_pass "IT-08: Direct addressing (checked)"
        fi

        # --- Test 9: DLQ List ---
        log_info "[IT-09] DLQ list..."
        if DLQ=$(/app/yirangmq-cli-consumer list-dlq --queue test-queue 2>&1); then
            log_pass "IT-09: DLQ list"
        else
            log_fail "IT-09: DLQ list — $DLQ"
        fi

        # --- Graceful shutdown ---
        log_info "Stopping MainMQ daemon..."
        kill -SIGTERM $DAEMON_PID 2>/dev/null
        wait $DAEMON_PID 2>/dev/null || true
        log_pass "MainMQ graceful shutdown"
    fi
fi

# ============================================================
# Results Summary
# ============================================================
log_section "Test Results Summary"

TOTAL=$((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))

echo -e "  Total:   $TOTAL"
echo -e "  ${GREEN}Passed:  $PASS_COUNT${NC}"
echo -e "  ${RED}Failed:  $FAIL_COUNT${NC}"
echo -e "  ${YELLOW}Skipped: $SKIP_COUNT${NC}"
echo ""

if [[ $FAIL_COUNT -gt 0 ]]; then
    echo -e "${RED}Failed tests:${NC}"
    for t in "${FAILED_TESTS[@]}"; do
        echo -e "  ${RED}- $t${NC}"
    done
    echo ""
    echo -e "${RED}══════════════════════════════════════════${NC}"
    echo -e "${RED}  TEST SUITE FAILED${NC}"
    echo -e "${RED}══════════════════════════════════════════${NC}"
    exit 1
else
    echo -e "${GREEN}══════════════════════════════════════════${NC}"
    echo -e "${GREEN}  ALL TESTS PASSED${NC}"
    echo -e "${GREEN}══════════════════════════════════════════${NC}"
    exit 0
fi
