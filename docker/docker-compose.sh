#!/bin/bash
# Yi-Rang MQ Docker Compose Helper Script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

print_usage() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}  Yi-Rang MQ Docker Management${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Service Commands:"
    echo "  up        Start services (builds if needed)"
    echo "  down      Stop and remove services"
    echo "  build     Build/rebuild images"
    echo "  logs      Show service logs"
    echo "  status    Show service status"
    echo "  clean     Remove all containers, images, volumes"
    echo ""
    echo "Publisher Commands:"
    echo "  publish   Publish a message to a queue"
    echo "  health    Check service health"
    echo "  metrics   Get server metrics"
    echo "  queue-status  Get queue status"
    echo ""
    echo "Consumer Commands:"
    echo "  consume       Consume a message from a queue"
    echo "  ack           Acknowledge a message"
    echo "  nack          Negative acknowledge a message"
    echo "  extend-lease  Extend visibility timeout for a leased message"
    echo "  list-dlq      List messages in dead letter queue"
    echo "  reprocess     Reprocess a DLQ message"
    echo ""
    echo "Demo Commands:"
    echo "  demo      Run the demo scenes (publish -> consume -> lease recovery -> DLQ -> reprocess)"
    echo ""
    echo "Test Commands:"
    echo "  test            Run all tests (unit + integration + wrapper round-trip)"
    echo "  test-unit       Run unit tests only"
    echo "  test-integration Run integration tests only"
    echo "  test-wrapper    Run host wrapper publish round-trip only"
    echo ""
    echo "Interactive:"
    echo "  cli       Open CLI shell (interactive)"
    echo ""
    echo "Cross-build (embedded):"
    echo "  buildx [platform]  Cross-build runtime image (default linux/arm64; QEMU, slow)"
    echo ""
    echo "Environment:"
    echo "  YIRANGMQ_CONSUMER_ID   Consumer id used by consume/ack/nack/extend-lease"
    echo "                         (default: docker-consumer). A settle is only accepted from the"
    echo "                         consumer holding the lease, so it must match across commands."
    echo ""
    echo "Examples:"
    echo "  $0 up                                    # Start service"
    echo "  $0 logs -f                              # Follow logs"
    echo "  $0 publish telemetry '{\"deviceId\":\"sensor-01\",\"timestamp\":1700000000,\"temp\":25.5}'"
    echo "  $0 publish telemetry '{\"deviceId\":\"sensor-01\",\"timestamp\":1700000000,\"temp\":25.5}' --target worker-01"
    echo "  $0 consume telemetry"
    echo "  $0 ack msg:telemetry:abc123"
    echo "  $0 nack msg:telemetry:abc123 --reason 'error' --requeue"
    echo "  $0 extend-lease msg:telemetry:abc123 --lease-id <id> --visibility 60"
    echo "  $0 list-dlq telemetry"
    echo "  $0 reprocess msg:telemetry:abc123"
    echo "  $0 queue-status telemetry"
    echo "  $0 demo                                 # Run all demo scenes"
    echo "  $0 demo --act 3                         # Lease expiry + fencing scene only"
    echo ""
}

# A settle (ack/nack/extend-lease) is only accepted from the consumer holding the lease. Passed
# BEFORE "$@" because ArgumentParser keeps the last occurrence, so a caller's value must win.
CONSUMER_ID="${YIRANGMQ_CONSUMER_ID:-docker-consumer}"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_pass_or_fail() {
    if [ "${2:-1}" -eq 0 ]; then
        log_info "$1: PASS"
    else
        log_error "$1: FAIL (exit ${2:-1})"
        exit "${2:-1}"
    fi
}

# ctest runs in the container: a build-time RUN layer is cache-hit and passes without running. (D-56)
run_unit_tests() {
    docker compose build yirangmq-unit-test
    local rc=0
    docker compose run --rm yirangmq-unit-test || rc=$?
    log_pass_or_fail "Unit tests" "$rc"
}

# Host-side wrapper round-trip: verifies that `docker-compose.sh publish` passes a JSON
# payload through docker-exec + shell layers intact. Regression guard for the default-value
# brace bug (MESSAGE="${3:-{...}}" appended a stray '}') that broke every wrapper publish.
run_wrapper_publish_test() {
    log_info "Phase 3: Wrapper publish round-trip..."
    local marker='wrap-roundtrip-marker'
    # Payload includes braces/quotes inside a string to stress shell quoting.
    local payload="{\"deviceId\":\"$marker\",\"timestamp\":1,\"note\":\"a{b}c\"}"

    if ! docker compose up -d --build --wait yirangmq >/dev/null 2>&1; then
        log_error "Wrapper round-trip: daemon failed to start"
        exit 1
    fi

    local pub
    pub=$("$0" publish telemetry "$payload" 2>&1)
    if ! echo "$pub" | grep -q 'published successfully'; then
        log_error "Wrapper round-trip: publish did not succeed — $pub"
        docker compose down -v >/dev/null 2>&1
        exit 1
    fi

    local con
    con=$("$0" consume telemetry 2>&1)
    docker compose down -v >/dev/null 2>&1

    if echo "$con" | grep -q "$marker"; then
        log_info "Wrapper publish round-trip (JSON payload intact): PASS"
    else
        log_error "Wrapper round-trip: payload not delivered intact — $con"
        exit 1
    fi
}

case "${1:-help}" in
    up)
        log_info "Starting Yi-Rang MQ..."
        docker compose up -d --build
        log_info "Service started. Use '$0 logs -f' to view logs."
        ;;

    down)
        log_info "Stopping Yi-Rang MQ..."
        docker compose down
        ;;

    build)
        log_info "Building Yi-Rang MQ image..."
        docker compose build --no-cache
        ;;

    buildx)
        # Cross-build the deployable runtime image for an embedded target platform. (F-14)
        # Uses QEMU emulation via buildx, so the vcpkg dependency build is slow — intended for
        # CI or a one-off image bake, not the local dev loop. Requires: docker buildx + binfmt.
        PLATFORM="${2:-linux/arm64}"
        TAG="yirangmq:${PLATFORM//\//-}"
        log_info "Cross-building runtime image for $PLATFORM (QEMU emulation — this is slow)..."
        docker buildx build --platform "$PLATFORM" --target runtime -f Dockerfile -t "$TAG" --load ..
        log_pass_or_fail "buildx $PLATFORM" $?
        ;;

    logs)
        shift
        docker compose logs "${@:--f}"
        ;;

    status)
        docker compose ps
        echo ""
        log_info "Health check:"
        docker compose exec -T yirangmq /app/yirangmq-cli-publisher health 2>/dev/null || log_warn "Service not running"
        ;;

    cli)
        log_info "Opening CLI shell..."
        docker compose run --rm yirangmq-cli
        ;;

    # Publisher commands
    publish)
        QUEUE="${2:-telemetry}"
        # Set the default separately: a JSON default inside ${3:-...} would let the inner '}'
        # terminate the parameter expansion early and append a stray '}' to the message.
        MESSAGE="${3-}"
        if [ -z "$MESSAGE" ]; then
            MESSAGE='{"test":true}'
        fi
        # A failed `shift 3` shifts nothing, leaving the subcommand and queue in "$@" as stray args.
        shift $(( $# >= 3 ? 3 : $# ))
        log_info "Publishing to queue: $QUEUE"
        docker compose exec -T yirangmq /app/yirangmq-cli-publisher --queue "$QUEUE" --message "$MESSAGE" "$@"
        ;;

    health)
        docker compose exec -T yirangmq /app/yirangmq-cli-publisher health
        ;;

    metrics)
        docker compose exec -T yirangmq /app/yirangmq-cli-publisher metrics
        ;;

    queue-status)
        QUEUE="${2:-telemetry}"
        docker compose exec -T yirangmq /app/yirangmq-cli-publisher status --queue "$QUEUE"
        ;;

    # Consumer commands
    consume)
        QUEUE="${2:-telemetry}"
        # A failed `shift 2` shifts nothing, leaving the subcommand in "$@".
        shift $(( $# >= 2 ? 2 : $# ))
        # Positional form `consume <queue> <consumer-id>` stays supported, but an option must not
        # be swallowed as the positional id.
        if [ $# -ge 1 ] && [ "${1#-}" = "$1" ]; then
            CONSUMER_ID="$1"
            shift
        fi
        log_info "Consuming from queue: $QUEUE (consumer: $CONSUMER_ID)"
        docker compose exec -T yirangmq /app/yirangmq-cli-consumer consume --queue "$QUEUE" --consumer-id "$CONSUMER_ID" "$@"
        ;;

    ack)
        MESSAGE_KEY="${2}"
        if [ -z "$MESSAGE_KEY" ]; then
            log_error "Usage: $0 ack <message-key> [--lease-id <id>] [--consumer-id <id>]"
            exit 1
        fi
        shift 2
        log_info "Acknowledging message: $MESSAGE_KEY"
        docker compose exec -T yirangmq /app/yirangmq-cli-consumer ack --message-key "$MESSAGE_KEY" --consumer-id "$CONSUMER_ID" "$@"
        ;;

    nack)
        MESSAGE_KEY="${2}"
        if [ -z "$MESSAGE_KEY" ]; then
            log_error "Usage: $0 nack <message-key> [--reason 'text'] [--requeue] [--consumer-id <id>]"
            exit 1
        fi
        shift 2
        log_info "Nacking message: $MESSAGE_KEY"
        docker compose exec -T yirangmq /app/yirangmq-cli-consumer nack --message-key "$MESSAGE_KEY" --consumer-id "$CONSUMER_ID" "$@"
        ;;

    extend-lease)
        MESSAGE_KEY="${2}"
        if [ -z "$MESSAGE_KEY" ]; then
            log_error "Usage: $0 extend-lease <message-key> [--lease-id <id>] [--visibility <sec>] [--consumer-id <id>]"
            exit 1
        fi
        shift 2
        log_info "Extending lease for message: $MESSAGE_KEY"
        docker compose exec -T yirangmq /app/yirangmq-cli-consumer extend-lease --message-key "$MESSAGE_KEY" --consumer-id "$CONSUMER_ID" "$@"
        ;;

    list-dlq)
        QUEUE="${2:-telemetry}"
        log_info "Listing DLQ for queue: $QUEUE"
        docker compose exec -T yirangmq /app/yirangmq-cli-consumer list-dlq --queue "$QUEUE"
        ;;

    reprocess)
        MESSAGE_KEY="${2}"
        if [ -z "$MESSAGE_KEY" ]; then
            log_error "Usage: $0 reprocess <message-key>"
            exit 1
        fi
        log_info "Reprocessing DLQ message: $MESSAGE_KEY"
        docker compose exec -T yirangmq /app/yirangmq-cli-consumer reprocess --message-key "$MESSAGE_KEY"
        ;;

    # Demo
    demo)
        shift
        log_info "Running the demo scenes against the running daemon..."
        # --attach: the compose service already owns /app/ipc, so the demo must not start a
        # second daemon on the same IPC root.
        docker compose exec -T yirangmq /app/run-demo.sh --attach --bin-dir /app "$@"
        ;;

    # Test commands
    test)
        log_info "Running all tests (unit + integration) in Docker..."
        log_info "Phase 1: Unit tests..."
        run_unit_tests
        log_info "Phase 2: Integration tests..."
        docker compose build yirangmq-integration-test
        INTEGRATION_RC=0
        # Phase 1 already ran the full ctest suite; without this the integration image repeats it.
        docker compose run --rm yirangmq-integration-test --integration-only || INTEGRATION_RC=$?
        log_pass_or_fail "Integration tests" "$INTEGRATION_RC"
        run_wrapper_publish_test
        ;;

    test-wrapper)
        run_wrapper_publish_test
        ;;

    test-unit)
        log_info "Running unit tests in Docker..."
        run_unit_tests
        ;;

    test-integration)
        # Without shift the subcommand reached the container and died with "Unknown option". (D-59)
        shift
        log_info "Running integration tests in Docker..."
        docker compose run --rm yirangmq-integration-test "$@"
        ;;

    clean)
        log_warn "This will remove all containers, images, and volumes!"
        read -p "Are you sure? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker compose down -v --rmi all
            log_info "Cleaned up."
        fi
        ;;

    help|--help|-h)
        print_usage
        ;;

    *)
        log_error "Unknown command: $1"
        print_usage
        exit 1
        ;;
esac
