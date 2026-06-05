#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

source common.sh

wait_for_url() {
  local name="$1" url="$2"
  local max_attempts="${3:-60}"
  echo -n "Waiting for $name"
  for i in $(seq 1 $max_attempts); do
    if curl -sf -o /dev/null "${os_curl_tls_args[@]}" "${os_curl_auth_args[@]}" "$url" 2>/dev/null; then
      echo " ready."
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo " timed out after $((max_attempts * 2))s!"
  return 1
}

wait_for_plain_url() {
  local name="$1" url="$2" max_attempts="${3:-60}"
  echo -n "Waiting for $name"
  for i in $(seq 1 "$max_attempts"); do
    if curl -sf -o /dev/null "$url" 2>/dev/null; then
      echo " ready."
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo " timed out after $((max_attempts * 2))s!"
  return 1
}

pushd .. || exit 1
  docker rm -f benchmark_opensearch 2>/dev/null || true
  docker compose --profile single-node down --remove-orphans 2>/dev/null || true
  docker compose down --remove-orphans 2>/dev/null || true
  docker build -t slackhq/astra .
  if [ "$ASTRA_DEPLOYMENT" = "single" ]; then
    docker compose --profile single-node up -d \
      zookeeper \
      kafka \
      s3 \
      openzipkin \
      opensearch \
      astra_single
  elif [ "$ASTRA_DEPLOYMENT" = "split" ]; then
    docker compose up -d \
      zookeeper \
      kafka \
      s3 \
      openzipkin \
      opensearch \
      astra_preprocessor \
      astra_index \
      astra_manager \
      astra_query \
      astra_cache \
      astra_recovery
  else
    echo "Unknown ASTRA_DEPLOYMENT: $ASTRA_DEPLOYMENT" >&2
    exit 1
  fi
popd

wait_for_url "OpenSearch" "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/_cluster/health"
wait_for_plain_url "KalDB preprocessor" "http://${ASTRA_BULK_HOST}:${ASTRA_BULK_PORT}/metrics"
wait_for_plain_url "KalDB query" "http://${ASTRA_QUERY_HOST}:${ASTRA_QUERY_PORT}/metrics"
