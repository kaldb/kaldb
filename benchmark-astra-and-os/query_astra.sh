#!/usr/bin/env bash
set -eu
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

source common.sh

request_file="${1:?usage: ./query_astra.sh clickbench/queries/q00.json}"
astra_index_name="${ASTRA_INDEX_NAME:-$INDEX_NAME}"

{
  jq -cn --arg index "$astra_index_name" '{index: $index}'
  jq -c . "$request_file"
} | curl -s --fail -X POST -H 'Content-Type: application/json' \
  "http://${ASTRA_QUERY_HOST}:${ASTRA_QUERY_PORT}/_msearch" --data-binary @-
