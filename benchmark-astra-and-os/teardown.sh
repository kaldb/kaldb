#!/usr/bin/env bash
set -eu
# Stop and remove benchmark containers.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

echo "Stopping OpenSearch..."
docker rm -f benchmark_opensearch 2>/dev/null || true

echo "Stopping KalDB/OpenSearch compose stack..."
pushd "$REPO_ROOT" > /dev/null || exit 1
  docker compose -f "$COMPOSE_FILE" --profile single-node down --remove-orphans 2>/dev/null || true
  docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
popd > /dev/null

echo "Teardown complete."
