#!/usr/bin/env bash
set -eu
# Stop and remove benchmark containers.

echo "Stopping OpenSearch..."
docker rm -f benchmark_opensearch 2>/dev/null || true

echo "Stopping Astra..."
pushd .. > /dev/null || exit 1
  docker compose down
popd > /dev/null

echo "Teardown complete."
