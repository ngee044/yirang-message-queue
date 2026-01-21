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
    echo "Commands:"
    echo "  up        Start services (builds if needed)"
    echo "  down      Stop and remove services"
    echo "  build     Build/rebuild images"
    echo "  logs      Show service logs"
    echo "  status    Show service status"
    echo "  cli       Open CLI shell (interactive)"
    echo "  publish   Publish a test message"
    echo "  consume   Consume a message"
    echo "  health    Check service health"
    echo "  clean     Remove all containers, images, volumes"
    echo ""
    echo "Examples:"
    echo "  $0 up                    # Start service"
    echo "  $0 logs -f               # Follow logs"
    echo "  $0 cli                   # Interactive CLI"
    echo "  $0 publish telemetry '{\"temp\":25.5}'"
    echo "  $0 consume telemetry"
    echo ""
}

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

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

    logs)
        shift
        docker compose logs "${@:--f}"
        ;;

    status)
        docker compose ps
        echo ""
        log_info "Health check:"
        docker compose exec yirangmq /app/yirangmq-cli health 2>/dev/null || log_warn "Service not running"
        ;;

    cli)
        log_info "Opening CLI shell..."
        docker compose run --rm yirangmq-cli
        ;;

    publish)
        QUEUE="${2:-telemetry}"
        MESSAGE="${3:-{\"test\":true}}"
        log_info "Publishing to queue: $QUEUE"
        docker compose exec yirangmq /app/yirangmq-cli publish --queue "$QUEUE" --message "$MESSAGE"
        ;;

    consume)
        QUEUE="${2:-telemetry}"
        log_info "Consuming from queue: $QUEUE"
        docker compose exec yirangmq /app/yirangmq-cli consume --queue "$QUEUE" --consumer-id "docker-cli"
        ;;

    health)
        docker compose exec yirangmq /app/yirangmq-cli health
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
