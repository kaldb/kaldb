#!/usr/bin/env bash
set -eu
# Stop and remove benchmark containers.

echo "Stopping OpenSearch..."
docker rm -f benchmark_opensearch 2>/dev/null || true

echo "Stopping KalDB/OpenSearch compose stack..."
pushd .. > /dev/null || exit 1
  docker compose down --remove-orphans 2>/dev/null || true
popd > /dev/null

echo "Teardown complete."
