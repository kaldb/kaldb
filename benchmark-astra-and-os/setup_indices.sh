#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
cd "$SCRIPT_DIR"

# Setup and fill indices in OpenSearch and KalDB clusters.
# Usage:
#   ./setup_indices.sh [count] [target]
#   count  - number of data files to load (default: 1280)
#   target - "os", "astra", or "both" (default: both)
#
# Environment variables:
#   OS_SCHEME   - "http" (default) or "https"
#   OS_PORT     - OpenSearch port (default: 9200)
#   ASTRA_BULK_PORT - KalDB bulk ingest port (default: 8086)
#   ASTRA_BULK_PATH - KalDB bulk endpoint path (default: /_bulk)
#   DATA_DIR        - directory containing ndjson files (default: data/ready)
#   ASTRA_DATASET_THROUGHPUT_BYTES - KalDB ingest throughput for the benchmark dataset
#   BULK_GROUP_SIZE - number of data files per bulk request (default: 1)
#   OS_NUMBER_OF_SHARDS - OpenSearch primary shard count for ClickBench hits (default: 10)
#   EXPECTED_DOCS - expected visible document count; computed from loaded files by default
#   INDEX_VISIBILITY_TIMEOUT_SECONDS - max seconds to wait for loaded docs to become searchable
#   INDEX_VISIBILITY_INTERVAL_SECONDS - seconds between visibility checks
#   ASTRA_REQUIRE_DURABLE_AFTER_LOAD - true to require persisted/cache-backed KalDB data before returning
#   ASTRA_DURABILITY_TIMEOUT_SECONDS - max seconds to wait for persisted/cache-backed KalDB data
#   ASTRA_BENCHMARK_NODE_ROLES_AFTER_LOAD - optional single-node roles to recreate KalDB with after load
#   ASTRA_DURABILITY_INTERVAL_SECONDS - seconds between durability checks
source common.sh

load_ct_splits=${1:-1280}
target=${2:-both}
bulk_group_size=${BULK_GROUP_SIZE:-1}
visibility_timeout_seconds=${INDEX_VISIBILITY_TIMEOUT_SECONDS:-1800}
visibility_interval_seconds=${INDEX_VISIBILITY_INTERVAL_SECONDS:-5}
astra_require_durable_after_load=${ASTRA_REQUIRE_DURABLE_AFTER_LOAD:-false}
astra_durability_timeout_seconds=${ASTRA_DURABILITY_TIMEOUT_SECONDS:-7200}
astra_durability_interval_seconds=${ASTRA_DURABILITY_INTERVAL_SECONDS:-30}

if ! [[ "$bulk_group_size" =~ ^[0-9]+$ ]] || [ "$bulk_group_size" -lt 1 ]; then
  echo "BULK_GROUP_SIZE must be a positive integer"
  exit 1
fi

if ! [[ "$OS_NUMBER_OF_SHARDS" =~ ^[0-9]+$ ]] || [ "$OS_NUMBER_OF_SHARDS" -lt 1 ]; then
  echo "OS_NUMBER_OF_SHARDS must be a positive integer"
  exit 1
fi

os_mapping_payload() {
  jq --argjson shards "$OS_NUMBER_OF_SHARDS" \
    '.settings.index.number_of_shards = $shards' \
    schema/clickbench_hits_mapping.json
}

selected_data_files() {
  find "$DATA_DIR" -type f | sort | head -n "$load_ct_splits"
}

verify_selected_data_files() {
  local available
  available=$(find "$DATA_DIR" -type f | wc -l | tr -d ' ')
  if [ "$available" -lt "$load_ct_splits" ]; then
    echo "Only $available data files are available in $DATA_DIR, but $load_ct_splits were requested." >&2
    echo "Run setup_data.sh with enough MAX_RECORDS/SPLITS_COUNT before loading indices." >&2
    return 1
  fi
}

expected_doc_count() {
  if [ -n "${EXPECTED_DOCS:-}" ]; then
    echo "$EXPECTED_DOCS"
    return 0
  fi

  local total=0
  local file
  while IFS= read -r file; do
    local lines
    lines=$(wc -l < "$file" | tr -d ' ')
    total=$((total + lines / 2))
  done < <(selected_data_files)
  echo "$total"
}

is_true() {
  case "${1,,}" in
    true|1|yes|y) return 0 ;;
    *) return 1 ;;
  esac
}

visible_doc_count() {
  local subject="$1"
  local request_file
  local response_file
  request_file="$(mktemp)"
  response_file="$(mktemp)"
  printf '{"index":"%s"}\n{"size":0,"track_total_hits":true}\n' "$INDEX_NAME" > "$request_file"

  local http_code
  case "$subject" in
    os)
      http_code="$(curl -sS -o "$response_file" -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -XPOST "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/_msearch" \
        --data-binary "@$request_file" \
        "${os_curl_tls_args[@]}" \
        "${os_curl_auth_args[@]}" || true)"
      ;;
    astra)
      http_code="$(curl -sS -o "$response_file" -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -XPOST "http://${ASTRA_QUERY_HOST}:${ASTRA_QUERY_PORT}/_msearch" \
        --data-binary "@$request_file" || true)"
      ;;
    *)
      echo "Unknown visibility subject: $subject" >&2
      rm -f "$request_file" "$response_file"
      return 1
      ;;
  esac

  if [[ "$http_code" != 2* ]]; then
    echo "Visibility check for $subject failed with HTTP $http_code" >&2
    cat "$response_file" >&2
    rm -f "$request_file" "$response_file"
    return 1
  fi

  jq -r '.responses[0].hits.total.value // 0' "$response_file"
  rm -f "$request_file" "$response_file"
}

refresh_os() {
  curl --fail -sS -XPOST "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/${INDEX_NAME}/_refresh" \
    "${os_curl_tls_args[@]}" \
    "${os_curl_auth_args[@]}" \
    -o /dev/null
}

wait_for_visible_docs() {
  local subject="$1"
  local expected="$2"
  local deadline=$((SECONDS + visibility_timeout_seconds))
  local label
  case "$subject" in
    os) label="OpenSearch" ;;
    astra) label="KalDB" ;;
    *) label="$subject" ;;
  esac

  if [ "$expected" -le 0 ]; then
    echo "Skipping $label visibility wait because expected document count is $expected."
    return 0
  fi

  echo "Waiting for $label to expose $expected loaded documents."
  while true; do
    if [ "$subject" = "os" ]; then
      refresh_os
    fi

    local visible
    visible="$(visible_doc_count "$subject" || echo 0)"
    echo "$label visible documents: $visible/$expected"
    if [ "$visible" -ge "$expected" ]; then
      return 0
    fi

    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Timed out waiting for $label to expose $expected loaded documents." >&2
      return 1
    fi

    sleep "$visibility_interval_seconds"
  done
}

zk_children() {
  local path="$1"
  docker exec dep_zookeeper zkCli.sh -server localhost:2181 ls "$path" 2>/dev/null \
    | awk '/^\[/{line=$0} END{gsub(/^\[/, "", line); gsub(/\]$/, "", line); gsub(/,/, "", line); print line}'
}

zk_json() {
  local path="$1"
  docker exec dep_zookeeper zkCli.sh -server localhost:2181 get "$path" 2>/dev/null \
    | awk 'seen || /^\{/ {seen=1; print; if (/^}/) exit}'
}

persisted_snapshot_ids_covering_count() {
  local expected="$1"
  local expected_offset=$((expected - 1))
  local partition
  for partition in $(zk_children /ASTRA/partitioned_snapshot); do
    if [ "$partition" = "LIVE" ]; then
      continue
    fi

    local snapshot
    for snapshot in $(zk_children "/ASTRA/partitioned_snapshot/$partition"); do
      local metadata
      metadata="$(zk_json "/ASTRA/partitioned_snapshot/$partition/$snapshot")"
      if [ -z "$metadata" ]; then
        continue
      fi

      if printf '%s\n' "$metadata" \
          | jq -e --argjson expected_offset "$expected_offset" \
              '(.sizeInBytes | tonumber) > 0 and (.maxOffset | tonumber) >= $expected_offset' >/dev/null; then
        printf '%s\n' "$metadata" | jq -r '.snapshotId // .name'
      fi
    done
  done
}

has_searchable_non_live_search_metadata() {
  local snapshot_id="$1"
  local search_node
  for search_node in $(zk_children /ASTRA/search); do
    local metadata
    metadata="$(zk_json "/ASTRA/search/$search_node")"
    if [ -z "$metadata" ]; then
      continue
    fi

    if printf '%s\n' "$metadata" \
        | jq -e --arg snapshot_id "$snapshot_id" \
            '(.snapshotName == $snapshot_id) and ((.searchable // true) == true)' >/dev/null; then
      return 0
    fi
  done
  return 1
}

wait_for_durable_astra_docs() {
  local expected="$1"
  local deadline=$((SECONDS + astra_durability_timeout_seconds))

  if [ "$expected" -le 0 ]; then
    echo "Skipping KalDB durability wait because expected document count is $expected."
    return 0
  fi

  echo "Waiting for KalDB to persist and cache data covering $expected loaded documents."
  while true; do
    local snapshot_ids
    snapshot_ids="$(persisted_snapshot_ids_covering_count "$expected" || true)"
    local ready_snapshot=""
    local snapshot_id
    for snapshot_id in $snapshot_ids; do
      if has_searchable_non_live_search_metadata "$snapshot_id"; then
        ready_snapshot="$snapshot_id"
        break
      fi
    done

    if [ -n "$ready_snapshot" ]; then
      echo "KalDB persisted/cache-backed snapshot is ready: $ready_snapshot"
      return 0
    fi

    local snapshot_count
    snapshot_count="$(printf '%s\n' "$snapshot_ids" | sed '/^$/d' | wc -l | tr -d ' ')"
    echo "KalDB durable snapshots covering $expected docs: $snapshot_count; waiting for searchable cache metadata."

    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Timed out waiting for KalDB persisted/cache-backed data covering $expected loaded documents." >&2
      return 1
    fi

    sleep "$astra_durability_interval_seconds"
  done
}

wait_for_url() {
  local name="$1"
  local url="$2"
  local timeout_seconds="${3:-600}"
  local deadline=$((SECONDS + timeout_seconds))

  echo -n "Waiting for $name"
  while true; do
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      echo " ready."
      return 0
    fi

    if [ "$SECONDS" -ge "$deadline" ]; then
      echo " timed out."
      return 1
    fi

    echo -n "."
    sleep 2
  done
}

restart_astra_and_validate_docs() {
  local expected="$1"
  echo "Restarting KalDB before benchmark readiness validation."

  if [ "$ASTRA_DEPLOYMENT" = "single" ]; then
    if [ -n "${ASTRA_BENCHMARK_NODE_ROLES_AFTER_LOAD:-}" ]; then
      echo "Recreating KalDB with NODE_ROLES=${ASTRA_BENCHMARK_NODE_ROLES_AFTER_LOAD} for benchmark readiness validation."
      (
        cd "$REPO_ROOT"
        ASTRA_SINGLE_NODE_ROLES="$ASTRA_BENCHMARK_NODE_ROLES_AFTER_LOAD" \
          docker compose -f "$COMPOSE_FILE" --profile single-node up -d --force-recreate --no-deps astra_single >/dev/null
      )
    else
      docker restart astra_single >/dev/null
    fi
    wait_for_url "KalDB query" "http://${ASTRA_QUERY_HOST}:${ASTRA_QUERY_PORT}/metrics" 600
  else
    echo "ASTRA_REQUIRE_DURABLE_AFTER_LOAD currently supports ASTRA_DEPLOYMENT=single only." >&2
    return 1
  fi

  wait_for_visible_docs astra "$expected"
}

require_durable_astra_after_load() {
  local expected="$1"
  if ! is_true "$astra_require_durable_after_load"; then
    return 0
  fi

  wait_for_durable_astra_docs "$expected"
  restart_astra_and_validate_docs "$expected"
}

load_os() {
  curl -XDELETE "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/${INDEX_NAME}" "${os_curl_tls_args[@]}" "${os_curl_auth_args[@]}" -s -o /dev/null || true
  os_mapping_payload | curl --fail -X PUT "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/${INDEX_NAME}" \
    -H "Content-Type: application/json" \
    "${os_curl_tls_args[@]}" \
    "${os_curl_auth_args[@]}" \
    --data-binary @-
  echo

  echo "Loading $load_ct_splits files into OpenSearch index $INDEX_NAME ($bulk_group_size files per bulk request)"
  local failed=0
  local response_file
  local files
  mapfile -t files < <(selected_data_files)
  for ((i = 0; i < ${#files[@]}; i += bulk_group_size)); do
    local group=("${files[@]:i:bulk_group_size}")
    local last_index=$((${#group[@]} - 1))
    local label="${group[0]}"
    if [ "${#group[@]}" -gt 1 ]; then
      label="${group[0]}..${group[$last_index]}"
    fi

    response_file="$(mktemp)"
    echo -n "$label OpenSearch: "
    local http_code
    http_code="$(curl -sS -o "$response_file" -w "%{http_code}" -H "Content-Type: application/x-ndjson" -XPOST "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/_bulk" \
       --data-binary @- "${os_curl_tls_args[@]}" "${os_curl_auth_args[@]}" < <(cat "${group[@]}") || true)"
    if [[ "$http_code" == 2* ]] && jq -e '.errors == false' "$response_file" >/dev/null; then
      echo "DONE."
    else
      echo "FAILED."
      echo "HTTP $http_code"
      cat "$response_file"
      rm -f "$response_file"
      return 1
    fi
    rm -f "$response_file"
  done
  if [ "$failed" -gt 0 ]; then
    echo "WARNING: $failed bulk requests failed to load into OpenSearch"
    return 1
  fi
  wait_for_visible_docs os "$(expected_doc_count)"
}

provision_astra() {
  ./provision_astra_dataset.sh
}

load_astra() {
  provision_astra
  echo "Loading $load_ct_splits files into KalDB index $INDEX_NAME (port ${ASTRA_BULK_PORT}, path ${ASTRA_BULK_PATH}, $bulk_group_size files per bulk request)"
  local failed=0
  local response_file
  local files
  mapfile -t files < <(selected_data_files)
  for ((i = 0; i < ${#files[@]}; i += bulk_group_size)); do
    local group=("${files[@]:i:bulk_group_size}")
    local last_index=$((${#group[@]} - 1))
    local label="${group[0]}"
    if [ "${#group[@]}" -gt 1 ]; then
      label="${group[0]}..${group[$last_index]}"
    fi

    response_file="$(mktemp)"
    echo -n "$label KalDB: "
    local http_code
    http_code="$(curl -sS --max-time "$ASTRA_BULK_CURL_MAX_TIME" -o "$response_file" -w "%{http_code}" -H "Content-Type: application/x-ndjson" -XPOST "http://${ASTRA_BULK_HOST}:${ASTRA_BULK_PORT}${ASTRA_BULK_PATH}" \
       --data-binary @- < <(cat "${group[@]}") || true)"
    if [[ "$http_code" == 2* ]] && jq -e '(.failedDocs // 0) == 0 and (.errorMsg // "") == ""' "$response_file" >/dev/null; then
      echo "DONE."
    else
      echo "FAILED."
      echo "HTTP $http_code"
      cat "$response_file"
      rm -f "$response_file"
      return 1
    fi
    rm -f "$response_file"
  done
  if [ "$failed" -gt 0 ]; then
    echo "WARNING: $failed bulk requests failed to load into KalDB"
    return 1
  fi
  local expected
  expected="$(expected_doc_count)"
  wait_for_visible_docs astra "$expected"
  require_durable_astra_after_load "$expected"
}

case "$target" in
  os)    verify_selected_data_files && load_os ;;
  astra) verify_selected_data_files && load_astra ;;
  both)  verify_selected_data_files && load_os && load_astra ;;
  *)     echo "Unknown target: $target (use os, astra, or both)"; exit 1 ;;
esac
