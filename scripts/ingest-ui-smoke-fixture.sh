#!/usr/bin/env bash
set -euo pipefail

DEFAULT_USE_DOCKER_EXEC=0
if [[ "$(uname -s)" == "Darwin" ]]; then
  DEFAULT_USE_DOCKER_EXEC=1
fi
USE_DOCKER_EXEC="${USE_DOCKER_EXEC:-$DEFAULT_USE_DOCKER_EXEC}"
SMOKE_RUN_ID="${SMOKE_RUN_ID:-ui-smoke-$(date -u +%s)-$$}"
TARGET_INDEX_NAME="${TARGET_INDEX_NAME:-test}"
NOISE_INDEX_NAME="${NOISE_INDEX_NAME:-ui-test}"
TMP_TEST_FIXTURE=""
TMP_UI_TEST_FIXTURE=""

HOST_BULK_URL="http://localhost:8086/_bulk"
DOCKER_BULK_URL="http://astra_preprocessor:8086/_bulk"

if [[ "$USE_DOCKER_EXEC" == "1" ]]; then
  BULK_URL="$DOCKER_BULK_URL"
else
  BULK_URL="$HOST_BULK_URL"
fi

run_curl() {
  if [[ "$USE_DOCKER_EXEC" == "1" ]]; then
    docker exec -i dep_opensearch_dashboards curl "$@"
  else
    curl "$@"
  fi
}

iso_at_offset() {
  local offset_secs="$1"
  local epoch_secs
  epoch_secs="$(($(date -u +%s) + offset_secs))"

  if date -u -r "$epoch_secs" '+%Y-%m-%dT%H:%M:%SZ' >/dev/null 2>&1; then
    date -u -r "$epoch_secs" '+%Y-%m-%dT%H:%M:%SZ'
    return
  fi

  if date -u -d "@$epoch_secs" '+%Y-%m-%dT%H:%M:%SZ' >/dev/null 2>&1; then
    date -u -d "@$epoch_secs" '+%Y-%m-%dT%H:%M:%SZ'
    return
  fi

  echo "ERROR: unable to format UTC timestamp for epoch $epoch_secs" >&2
  exit 1
}

write_fixture() {
  local fixture_file="$1"
  local index_name="$2"
  local id_one="$3"
  local id_two="$4"
  local message_one="$5"
  local message_two="$6"
  local level_one="$7"
  local level_two="$8"
  local host_one="$9"
  local host_two="${10}"
  local duration_one="${11}"
  local duration_two="${12}"
  local offset_one="${13}"
  local offset_two="${14}"
  local t1 t2
  t1="$(iso_at_offset "$offset_one")"
  t2="$(iso_at_offset "$offset_two")"

  cat >"$fixture_file" <<EOF
{"index":{"_index":"$index_name","_id":"$id_one"}}
{"@timestamp":"$t1","message":"$message_one","level":"$level_one","host":"$host_one","duration_ms":$duration_one,"smoke_run":"$SMOKE_RUN_ID"}
{"index":{"_index":"$index_name","_id":"$id_two"}}
{"@timestamp":"$t2","message":"$message_two","level":"$level_two","host":"$host_two","duration_ms":$duration_two,"smoke_run":"$SMOKE_RUN_ID"}
EOF
}

ingest_fixture() {
  local fixture_file="$1"
  local expected_docs="$2"
  local response raw_response http_status total_docs failed_docs
  raw_response="$(
    cat "$fixture_file" \
      | run_curl -sS -w $'\n%{http_code}' -H 'Content-Type: application/x-ndjson' \
          "$BULK_URL" --data-binary @-
  )"

  http_status="${raw_response##*$'\n'}"
  response="${raw_response%$'\n'*}"

  if [[ "$http_status" != "200" ]]; then
    echo "ERROR: bulk ingest returned HTTP $http_status" >&2
    printf '%s\n' "$response" >&2
    exit 1
  fi

  if ! jq -e . >/dev/null 2>&1 <<<"$response"; then
    echo "ERROR: bulk ingest returned non-JSON output" >&2
    printf '%s\n' "$response" >&2
    exit 1
  fi

  total_docs="$(jq -r '.totalDocs // 0' <<<"$response")"
  failed_docs="$(jq -r '.failedDocs // 0' <<<"$response")"

  if [[ "$total_docs" != "$expected_docs" ]]; then
    echo "ERROR: expected $expected_docs ingested docs but got $total_docs" >&2
    exit 1
  fi

  if [[ "$failed_docs" != "0" ]]; then
    echo "ERROR: expected 0 failed docs but got $failed_docs" >&2
    exit 1
  fi
}

main() {
  if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage:
  ./scripts/ingest-ui-smoke-fixture.sh

Environment:
  USE_DOCKER_EXEC=1   Send the bulk request through the Dashboards container.
  SMOKE_RUN_ID=...    Override the generated smoke fixture id.
  TARGET_INDEX_NAME=...  Override the target dataset/index name (default: test).
  NOISE_INDEX_NAME=...   Override the noise dataset/index name (default: ui-test).

What it writes:
  - 2 docs to the target dataset/index
  - 2 docs to the noise dataset/index

Each document includes:
  - @timestamp
  - message
  - level
  - host
  - duration_ms
  - smoke_run
EOF
    exit 0
  fi

  TMP_TEST_FIXTURE="$(mktemp)"
  TMP_UI_TEST_FIXTURE="$(mktemp)"
  trap 'rm -f "${TMP_TEST_FIXTURE:-}" "${TMP_UI_TEST_FIXTURE:-}"' EXIT

  write_fixture \
    "$TMP_TEST_FIXTURE" \
    "$TARGET_INDEX_NAME" \
    "test-ui-1-$SMOKE_RUN_ID" \
    "test-ui-2-$SMOKE_RUN_ID" \
    "Test dataset log one" \
    "Test dataset log two" \
    "INFO" \
    "ERROR" \
    "host-a" \
    "host-b" \
    "42" \
    "84" \
    "-120" \
    "-90"

  write_fixture \
    "$TMP_UI_TEST_FIXTURE" \
    "$NOISE_INDEX_NAME" \
    "ui-test-ui-1-$SMOKE_RUN_ID" \
    "ui-test-ui-2-$SMOKE_RUN_ID" \
    "UI test dataset log one" \
    "UI test dataset log two" \
    "INFO" \
    "WARN" \
    "host-c" \
    "host-d" \
    "21" \
    "63" \
    "-60" \
    "-30"

  ingest_fixture "$TMP_TEST_FIXTURE" "2"
  ingest_fixture "$TMP_UI_TEST_FIXTURE" "2"

  printf 'Smoke run id:\n  %s\n' "$SMOKE_RUN_ID"
}

main "$@"
