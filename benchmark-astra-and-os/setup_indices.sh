#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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
source common.sh

load_ct_splits=${1:-1280}
target=${2:-both}
bulk_group_size=${BULK_GROUP_SIZE:-1}

if ! [[ "$bulk_group_size" =~ ^[0-9]+$ ]] || [ "$bulk_group_size" -lt 1 ]; then
  echo "BULK_GROUP_SIZE must be a positive integer"
  exit 1
fi

os_mapping_payload() {
  case "$DATASET" in
    clickbench_hits)
      cat schema/clickbench_hits_mapping.json
      ;;
    nyc_taxis)
      cat <<'JSON'
{
  "mappings": {
    "properties": {
      "dropoff_datetime": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
      "pickup_datetime": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
      "total_amount": {"type": "double"},
      "improvement_surcharge": {"type": "double"},
      "tolls_amount": {"type": "double"},
      "fare_amount": {"type": "double"},
      "extra": {"type": "double"},
      "trip_distance": {"type": "double"},
      "tip_amount": {"type": "double"},
      "mta_tax": {"type": "double"},
      "passenger_count": {"type": "integer"}
    }
  }
}
JSON
      ;;
  esac
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
  mapfile -t files < <(find "$DATA_DIR" -type f | sort | head -n "$load_ct_splits")
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
  mapfile -t files < <(find "$DATA_DIR" -type f | sort | head -n "$load_ct_splits")
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
}

case "$target" in
  os)    load_os ;;
  astra) load_astra ;;
  both)  load_os && load_astra ;;
  *)     echo "Unknown target: $target (use os, astra, or both)"; exit 1 ;;
esac
