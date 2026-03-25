#!/usr/bin/env bash

set -eu

# Setup and fill indices in OpenSearch and Astra clusters.
# Usage:
#   ./setup_indices.sh [count] [target]
#   count  - number of data files to load (default: 10000)
#   target - "os", "astra", or "both" (default: both)
#
# Environment variables:
#   OS_SCHEME   - "https" (default) or "http"
#   OS_PORT     - OpenSearch port (default: 9200)
#   ASTRA_BULK_PORT - Astra bulk ingest port (default: 8080)
#   ASTRA_BULK_PATH - Astra bulk endpoint path (default: /_local_bulk)
#   DATA_DIR        - directory containing ndjson files (default: data/ready)
source common.sh

load_ct_splits=${1:-10000}
target=${2:-both}

OS_SCHEME="${OS_SCHEME:-https}"
OS_PORT="${OS_PORT:-9200}"
ASTRA_BULK_PORT="${ASTRA_BULK_PORT:-8080}"
ASTRA_BULK_PATH="${ASTRA_BULK_PATH:-/_local_bulk}"
DATA_DIR="${DATA_DIR:-data/ready}"

os_auth=""
if [ "$OS_SCHEME" = "https" ]; then
  os_auth="-ku admin:$OS_PW"
fi

load_os() {
  # delete and recreate index
  curl -XDELETE "${OS_SCHEME}://localhost:${OS_PORT}/test" $os_auth -s -o /dev/null || true
  curl -X PUT "${OS_SCHEME}://localhost:${OS_PORT}/test" -H "Content-Type: application/json" $os_auth \
    -d '{ "mappings": { "properties": { "dropoff_datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss" }}}}'
  echo

  echo "Loading $load_ct_splits files into OpenSearch"
  local failed=0
  for f in $(ls ${DATA_DIR}/* | head -n "$load_ct_splits"); do
    echo -n "$f OpenSearch: "
    if curl --fail -H "Content-Type: application/x-ndjson" -XPOST "${OS_SCHEME}://localhost:${OS_PORT}/_bulk" \
       --data-binary "@$f" $os_auth -s > /dev/null; then
      echo "DONE."
    else
      echo "FAILED."
      failed=$((failed + 1))
    fi
  done
  if [ "$failed" -gt 0 ]; then
    echo "WARNING: $failed files failed to load into OpenSearch"
    return 1
  fi
}

load_astra() {
  echo "Loading $load_ct_splits files into Astra (port ${ASTRA_BULK_PORT}, path ${ASTRA_BULK_PATH})"
  local failed=0
  for f in $(ls ${DATA_DIR}/* | head -n "$load_ct_splits"); do
    echo -n "$f Astra: "
    if curl --fail -H "Content-Type: application/x-ndjson" -XPOST "http://localhost:${ASTRA_BULK_PORT}${ASTRA_BULK_PATH}" \
       --data-binary "@$f" -s > /dev/null; then
      echo "DONE."
    else
      echo "FAILED."
      failed=$((failed + 1))
    fi
  done
  if [ "$failed" -gt 0 ]; then
    echo "WARNING: $failed files failed to load into Astra"
    return 1
  fi
}

case "$target" in
  os)    load_os ;;
  astra) load_astra ;;
  both)  load_os && load_astra ;;
  *)     echo "Unknown target: $target (use os, astra, or both)"; exit 1 ;;
esac
