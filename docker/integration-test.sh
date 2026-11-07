#!/bin/bash
# Unit binaries and isolated real-daemon operational scenarios.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR=/app
RUN_UNIT=true
RUN_INTEGRATION=true
GTEST_FILTER=""
BACKEND=all
MODE=all
PASSED=0
FAILED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --filter|--bin-dir|--backend|--mode)
            if [[ $# -lt 2 || -z "$2" || "$2" == -* ]]; then
                echo "Option $1 requires a value." >&2
                exit 2
            fi
            ;;
    esac
    case "$1" in
        --unit-only) RUN_UNIT=true; RUN_INTEGRATION=false; shift ;;
        --integration-only) RUN_UNIT=false; RUN_INTEGRATION=true; shift ;;
        --filter) GTEST_FILTER="$2"; shift 2 ;;
        --bin-dir) BIN_DIR="$2"; shift 2 ;;
        --backend) BACKEND="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--unit-only|--integration-only] [--filter 'SQLiteAdapterTest.*']"
            echo "          [--bin-dir PATH] [--backend all|sqlite|filesystem|hybrid] [--mode all|poll|event]"
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

BIN_DIR="$(cd "$BIN_DIR" && pwd)" || exit 2
if [[ "$RUN_UNIT" == true ]]; then
    MATCHED=0
    shopt -s nullglob
    for binary in "$BIN_DIR"/Test*; do
        [[ -f "$binary" && -x "$binary" ]] || continue
        status=0
        output=$(cd "$BIN_DIR" && "$binary" "--gtest_filter=${GTEST_FILTER:-*}" --gtest_color=no 2>&1) || status=$?
        echo "$output"
        if [[ $status -ne 0 ]]; then
            FAILED=$((FAILED + 1))
        elif [[ "$output" == *"0 tests from 0 test suites ran"* ]]; then
            echo "SKIP $(basename "$binary"): no matching tests"
        else
            MATCHED=$((MATCHED + 1))
        fi
    done
    if [[ $MATCHED -eq 0 ]]; then
        echo "FAIL: no unit test suite executed" >&2
        FAILED=$((FAILED + 1))
    else
        PASSED=$((PASSED + 1))
    fi
fi

if [[ "$RUN_INTEGRATION" == true ]]; then
    scenarios="$SCRIPT_DIR/integration_scenarios.py"
    [[ -f "$scenarios" ]] || scenarios="$SCRIPT_DIR/../Tests/integration_scenarios.py"
    if python3 "$scenarios" --bin-dir "$BIN_DIR" --backend "$BACKEND" --mode "$MODE"; then
        PASSED=$((PASSED + 1))
    else
        FAILED=$((FAILED + 1))
    fi
fi

echo "Test phases: $PASSED passed, $FAILED failed"
[[ $FAILED -eq 0 && $PASSED -gt 0 ]]
