#!/usr/bin/env bash
set -euo pipefail

DEMO_STATE_DIR="${STATE_DIR:-/tmp/kaldb-enterprise-demo}"

demo_state_file() {
  local index="$1"
  printf '%s/%s.json\n' "$DEMO_STATE_DIR" "$index"
}

ensure_demo_state_dir() {
  mkdir -p "$DEMO_STATE_DIR"
}

require_demo_state_file() {
  local index="$1"
  local state_file
  state_file="$(demo_state_file "$index")"
  if [[ ! -f "$state_file" ]]; then
    echo "ERROR: demo state file not found for index '$index': $state_file" >&2
    exit 1
  fi
  printf '%s\n' "$state_file"
}

manager_post_json() {
  local manager_url="$1"
  local endpoint="$2"
  local payload="$3"
  local attempts="${4:-20}"
  local response=""

  for _ in $(seq 1 "$attempts"); do
    response="$(
      curl -sS -XPOST \
        -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
        "$manager_url/$endpoint" \
        -d "$payload" 2>/dev/null || true
    )"
    if printf '%s' "$response" | python3 -c 'import json,sys; json.load(sys.stdin)' >/dev/null 2>&1; then
      printf '%s' "$response"
      return 0
    fi
    sleep 1
  done

  echo "ERROR: manager endpoint '$endpoint' did not return valid JSON after $attempts attempt(s)." >&2
  return 1
}

query_total_hits() {
  local query_url="$1"
  local index="$2"
  local payload='{"size":0,"aggs":{"per_day":{"date_histogram":{"field":"@timestamp","calendar_interval":"1d"}}},"query":{"match_all":{}}}'

  printf '%s' "$payload" \
    | curl -sS -H 'Content-Type: application/json' -X POST "$query_url/$index/_search" --data-binary @- \
    | python3 -c 'import json,sys; payload=json.load(sys.stdin); aggs=payload.get("aggregations") or {}; buckets=(aggs.get("per_day") or {}).get("buckets",[]); print(sum(bucket.get("doc_count",0) for bucket in buckets))'
}

query_event_window_hits() {
  local query_url="$1"
  local index="$2"
  local window_name="$3"
  local payload='{"size":0,"aggs":{"per_window":{"terms":{"field":"event_window","size":10}}},"query":{"match_all":{}}}'

  printf '%s' "$payload" \
    | curl -sS -H 'Content-Type: application/json' -X POST "$query_url/$index/_search" --data-binary @- \
    | python3 -c 'import json,sys; payload=json.load(sys.stdin); target=sys.argv[1]; buckets=((payload.get("aggregations") or {}).get("per_window") or {}).get("buckets",[]); print(next((bucket.get("doc_count",0) for bucket in buckets if bucket.get("key")==target), 0))' "$window_name"
}

wait_for_http() {
  local name="$1"
  local url="$2"
  local attempts="${3:-90}"

  for _ in $(seq 1 "$attempts"); do
    if curl -sS -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: $name did not become reachable at $url" >&2
  return 1
}

wait_for_health() {
  local name="$1"
  local url="$2"
  local attempts="${3:-90}"
  local response=""

  for _ in $(seq 1 "$attempts"); do
    response="$(curl -sS "$url" 2>/dev/null || true)"
    if [[ "$response" == *'"healthy":true'* ]]; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: $name did not become healthy at $url" >&2
  return 1
}
