#!/usr/bin/env bash
set -eu
# cluster setup bits
# -----
source common.sh

wait_for_url() {
  local name="$1" url="$2" curl_opts="${3:-}"
  local max_attempts=30
  echo -n "Waiting for $name"
  for i in $(seq 1 $max_attempts); do
    if curl -sf -o /dev/null $curl_opts "$url" 2>/dev/null; then
      echo " ready."
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo " timed out after $((max_attempts * 2))s!"
  return 1
}

# build & start opensearch
docker run -d --name benchmark_opensearch \
  -p 9200:9200 -p 9600:9600 \
  -e "discovery.type=single-node" \
  -e "OPENSEARCH_INITIAL_ADMIN_PASSWORD=$OS_PW" \
  opensearchproject/opensearch:latest

# build & start astra
pushd .. || exit 1
  docker build -t slackhq/astra .
  docker compose up -d
popd

# wait for services to be healthy before returning
wait_for_url "OpenSearch" "https://localhost:9200/_cluster/health" "-ku admin:$OS_PW"
wait_for_url "Astra" "http://localhost:8080/_msearch" ""
