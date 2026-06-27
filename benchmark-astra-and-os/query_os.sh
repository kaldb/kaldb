#!/usr/bin/env bash
set -eu
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

source common.sh

request_file="${1:?usage: ./query_os.sh clickbench/queries/q00.json}"

{
  jq -cn --arg index "$INDEX_NAME" --argjson request_cache "$OS_REQUEST_CACHE" \
    '{index: $index, request_cache: $request_cache}'
  jq -c . "$request_file"
} | curl -s --fail "${os_curl_tls_args[@]}" "${os_curl_auth_args[@]}" -XPOST "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/_msearch" \
  -H 'Content-Type: application/json' --data-binary @-
