#!/usr/bin/env bash
set -eu
source common.sh

while IFS= read -r header; do
  IFS= read -r body || break
  jq -c --argjson request_cache "$OS_REQUEST_CACHE" '. + {request_cache: $request_cache}' <<<"$header"
  printf '%s\n' "$body"
done < "$1" | curl -s --fail "${os_curl_tls_args[@]}" "${os_curl_auth_args[@]}" -XGET "${OS_SCHEME}://${OS_HOST}:${OS_PORT}/_msearch" \
  -H 'Content-Type: application/json' --data-binary @-
