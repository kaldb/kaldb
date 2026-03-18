#!/usr/bin/env bash

set -eu

# Setup and fill indices in OpenSearch and Astra clusters.
# Usage:
#   ./setup_indices.sh [count] [target]
#   count  - number of data files to load (default: 10000)
#   target - "os", "astra", or "both" (default: both)
source common.sh

load_ct_splits=${1:-10000}
target=${2:-both}

load_os() {
  # delete and recreate index
  curl -XDELETE "https://localhost:9200/test" -ku admin:$OS_PW -s -o /dev/null || true
  curl -X PUT "https://localhost:9200/test" -H "Content-Type: application/json" -ku admin:$OS_PW \
    -d '{ "mappings": { "properties": { "dropoff_datetime": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss" }}}}'
  echo

  echo "Loading $load_ct_splits files into OpenSearch"
  local failed=0
  for f in $(ls data/ready/* | head -n "$load_ct_splits"); do
    echo -n "$f OpenSearch: "
    if curl --fail -H "Content-Type: application/x-ndjson" -XPOST "https://localhost:9200/_bulk" \
       --data-binary "@$f" -ku admin:$OS_PW -s > /dev/null; then
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
  echo "Loading $load_ct_splits files into Astra"
  local failed=0
  for f in $(ls data/ready/* | head -n "$load_ct_splits"); do
    echo -n "$f Astra: "
    if curl --fail -H "Content-Type: application/x-ndjson" -k -XPOST "http://localhost:8080/_local_bulk" \
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
