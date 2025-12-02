#!/bin/bash
set -euo pipefail

# Start hl-agent monitoring stack (Prometheus + Grafana)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if we need sudo for docker
if ! docker ps >/dev/null 2>&1; then
    DOCKER="sudo docker"
    DOCKER_COMPOSE="sudo docker compose"
else
    DOCKER="docker"
    DOCKER_COMPOSE="docker compose"
fi

echo "Starting hl-agent monitoring stack..."
echo ""
echo "Services:"
echo "  - Prometheus: http://localhost:9091"
echo "  - Grafana:    http://localhost:3000 (admin/admin)"
echo ""

$DOCKER_COMPOSE up -d

echo ""
echo "Waiting for services to be ready..."
sleep 5

# Check if services are running
if $DOCKER_COMPOSE ps | grep -q "Up"; then
    echo ""
    echo "✓ Monitoring stack is running!"
    echo ""
    echo "Access Grafana at: http://localhost:3000"
    echo "  Username: admin"
    echo "  Password: admin"
    echo ""
    echo "The 'HL-Agent Metrics' dashboard should be available automatically."
    echo ""
    echo "To view logs:  $DOCKER_COMPOSE logs -f"
    echo "To stop:       $DOCKER_COMPOSE down"
else
    echo ""
    echo "✗ Failed to start services. Check logs:"
    $DOCKER_COMPOSE logs
    exit 1
fi
