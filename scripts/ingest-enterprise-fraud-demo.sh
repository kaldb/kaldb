#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/enterprise-fraud-demo-common.sh"

INDEX="${INDEX:-enterprise_fraud_demo}"
BATCH_SIZE="${BATCH_SIZE:-250}"
SEED="${SEED:-42}"
HISTORICAL_DOCS="${HISTORICAL_DOCS:-750}"
LIVE_DOCS="${LIVE_DOCS:-400}"
HISTORICAL_DAYS_AGO="${HISTORICAL_DAYS_AGO:-6}"
HISTORICAL_WINDOW_MINS="${HISTORICAL_WINDOW_MINS:-180}"
LIVE_WINDOW_MINS="${LIVE_WINDOW_MINS:-90}"
NOW_EPOCH_MS="${NOW_EPOCH_MS:-$(python3 -c 'import time; print(int(time.time() * 1000))')}"
HISTORICAL_SNAPSHOT_SETTLE_SECS="${HISTORICAL_SNAPSHOT_SETTLE_SECS:-2}"
HISTORICAL_SNAPSHOT_WAIT_ATTEMPTS="${HISTORICAL_SNAPSHOT_WAIT_ATTEMPTS:-45}"
KAFKA_COMMIT_SETTLE_SECS="${KAFKA_COMMIT_SETTLE_SECS:-2}"

BULK_URL="${BULK_URL:-http://localhost:8086/_bulk}"
MANAGER_URL="${MANAGER_URL:-http://localhost:8083}"
QUERY_URL="${QUERY_URL:-http://localhost:8081}"
INDEXER_HEALTH_URL="${INDEXER_HEALTH_URL:-http://localhost:8080/health}"
QUERY_READY_ATTEMPTS="${QUERY_READY_ATTEMPTS:-120}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --index) INDEX="$2"; shift 2 ;;
    --index=*) INDEX="${1#*=}"; shift ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --batch-size=*) BATCH_SIZE="${1#*=}"; shift ;;
    --seed) SEED="$2"; shift 2 ;;
    --seed=*) SEED="${1#*=}"; shift ;;
    --historical-docs) HISTORICAL_DOCS="$2"; shift 2 ;;
    --historical-docs=*) HISTORICAL_DOCS="${1#*=}"; shift ;;
    --live-docs) LIVE_DOCS="$2"; shift 2 ;;
    --live-docs=*) LIVE_DOCS="${1#*=}"; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --help)
      cat <<'EOF'
Ingest the synthetic enterprise fraud fixture into Astra via the bulk API.

Usage:
  bash scripts/ingest-enterprise-fraud-demo.sh [OPTIONS]

Options:
  --index NAME            Target dataset name
  --batch-size N          Docs per bulk request (default: 250)
  --seed N                Deterministic generator seed (default: 42)
  --historical-docs N     Historical window document count (default: 750)
  --live-docs N           Live window document count (default: 400)
  --dry-run               Print the first 20 generated lines and exit
  --help                  Show this help message
EOF
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

generator_metadata() {
  python3 "$SCRIPT_DIR/generate-enterprise-fraud-demo.py" \
    --index "$INDEX" \
    --metadata \
    --seed "$SEED" \
    --now-epoch-ms "$NOW_EPOCH_MS" \
    --historical-docs "$HISTORICAL_DOCS" \
    --live-docs "$LIVE_DOCS" \
    --historical-days-ago "$HISTORICAL_DAYS_AGO" \
    --historical-window-mins "$HISTORICAL_WINDOW_MINS" \
    --live-window-mins "$LIVE_WINDOW_MINS"
}

stream_bulk_fixture() {
  local window="${1:-all}"
  python3 "$SCRIPT_DIR/generate-enterprise-fraud-demo.py" \
    --index "$INDEX" \
    --window "$window" \
    --seed "$SEED" \
    --now-epoch-ms "$NOW_EPOCH_MS" \
    --historical-docs "$HISTORICAL_DOCS" \
    --live-docs "$LIVE_DOCS" \
    --historical-days-ago "$HISTORICAL_DAYS_AGO" \
    --historical-window-mins "$HISTORICAL_WINDOW_MINS" \
    --live-window-mins "$LIVE_WINDOW_MINS"
}

get_dataset_state() {
  local response
  response="$(manager_post_json "$MANAGER_URL" "slack.proto.astra.ManagerApiService/ListDatasetMetadata" '{}')" || return 1
  python3 -c '
import json
import sys

target_index = sys.argv[1]
payload = json.load(sys.stdin)

for dataset in payload.get("datasetMetadata", []):
    if dataset.get("name") != target_index:
        continue
    for partition_config in dataset.get("partitionConfigs", []):
        if partition_config.get("endTimeEpochMs") == "9223372036854775807":
            partitions = ",".join(partition_config.get("partitions", []))
            start_time = partition_config.get("startTimeEpochMs", "")
            print(f"{start_time}|{partitions}")
            raise SystemExit(0)
    print("|")
    raise SystemExit(0)
' "$INDEX" <<<"$response"
}

ingest_window() {
  local window="$1"
  local docs_expected="$2"
  local label="$3"
  local total_batches=$(( (docs_expected + BATCH_SIZE - 1) / BATCH_SIZE ))
  local batch=0
  local ingested=0
  local failed=0

  if (( docs_expected <= 0 )); then
    return 0
  fi

  echo "Ingesting $label window ..."

  exec 3< <(stream_bulk_fixture "$window")
  while true; do
    local batch_data=""
    local docs_in_batch=0
    local response=""
    local batch_ok=0
    local batch_fail=0

    while (( docs_in_batch < BATCH_SIZE )); do
      if ! IFS= read -r action_line <&3; then
        break
      fi
      if ! IFS= read -r doc_line <&3; then
        echo "ERROR: generated bulk payload ended unexpectedly." >&2
        exit 1
      fi
      batch_data+="$action_line"$'\n'
      batch_data+="$doc_line"$'\n'
      (( docs_in_batch++ )) || true
    done

    if (( docs_in_batch == 0 )); then
      break
    fi

    (( batch++ )) || true
    response="$(printf '%s' "$batch_data" | curl -sS -H 'Content-Type: application/x-ndjson' "$BULK_URL" --data-binary @- 2>&1)"
    batch_ok="$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('totalDocs',0))" 2>/dev/null || echo 0)"
    batch_fail="$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('failedDocs',0))" 2>/dev/null || echo 0)"

    ingested=$(( ingested + batch_ok ))
    failed=$(( failed + batch_fail ))
    printf "\r  %s batch %d/%d  ingested=%d  failed=%d" "$label" "$batch" "$total_batches" "$ingested" "$failed"

    if (( docs_in_batch < BATCH_SIZE )); then
      break
    fi
  done
  exec 3<&-

  echo
  echo "  $label window complete: $ingested docs ingested, $failed failed."

  if (( failed > 0 )); then
    echo "ERROR: bulk ingest reported failures for the $label window." >&2
    exit 1
  fi

  if (( ingested != docs_expected )); then
    echo "ERROR: expected $docs_expected docs for the $label window but ingested $ingested." >&2
    exit 1
  fi
}

wait_for_historical_to_go_cold() {
  local attempts="${1:-60}"
  local all_hits historical_hits

  for _ in $(seq 1 "$attempts"); do
    all_hits="$(query_total_hits "$QUERY_URL" "$INDEX" 2>/dev/null || echo 0)"
    historical_hits="$(query_event_window_hits "$QUERY_URL" "$INDEX" "historical" 2>/dev/null || echo 0)"

    if [[ "$all_hits" == "0" && "$historical_hits" == "0" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: historical data remained query-visible after restarting the indexer." >&2
  echo "  all_hits=$all_hits historical_hits=$historical_hits" >&2
  return 1
}

wait_for_historical_snapshot() {
  local baseline_count="$1"
  local attempts="${2:-30}"
  local snapshot_count

  for _ in $(seq 1 "$attempts"); do
    snapshot_count="$(docker logs astra_index 2>&1 | grep -c "Finished RW chunk snapshot to S3" || true)"
    if (( snapshot_count > baseline_count )); then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: historical chunk snapshot did not complete before the indexer restart." >&2
  echo "  baseline_snapshot_count=$baseline_count current_snapshot_count=${snapshot_count:-0}" >&2
  docker logs astra_index --tail 80 >&2 || true
  return 1
}

wait_for_pre_restore_shape() {
  local attempts="${1:-60}"
  local all_hits live_hits historical_hits

  for _ in $(seq 1 "$attempts"); do
    all_hits="$(query_total_hits "$QUERY_URL" "$INDEX" 2>/dev/null || echo 0)"
    live_hits="$(query_event_window_hits "$QUERY_URL" "$INDEX" "live" 2>/dev/null || echo 0)"
    historical_hits="$(query_event_window_hits "$QUERY_URL" "$INDEX" "historical" 2>/dev/null || echo 0)"

    if [[ "$all_hits" == "$EXPECTED_VISIBLE_BEFORE_RESTORE" \
       && "$live_hits" == "$EXPECTED_VISIBLE_BEFORE_RESTORE" \
       && "$historical_hits" == "0" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: dataset never reached the expected pre-restore visibility shape." >&2
  echo "  all_hits=$all_hits live_hits=$live_hits historical_hits=$historical_hits expected_live=$EXPECTED_VISIBLE_BEFORE_RESTORE" >&2
  return 1
}

STATE_JSON="$(generator_metadata)"
ensure_demo_state_dir
STATE_FILE="$(demo_state_file "$INDEX")"
printf '%s\n' "$STATE_JSON" > "$STATE_FILE"

HISTORICAL_FROM="$(jq -r '.historical.from' <<<"$STATE_JSON")"
HISTORICAL_TO="$(jq -r '.historical.to' <<<"$STATE_JSON")"
LIVE_FROM="$(jq -r '.live.from' <<<"$STATE_JSON")"
LIVE_TO="$(jq -r '.live.to' <<<"$STATE_JSON")"
EXPECTED_VISIBLE_BEFORE_RESTORE="$(jq -r '.expectedVisibleBeforeRestore' <<<"$STATE_JSON")"
EXPECTED_VISIBLE_AFTER_RESTORE="$(jq -r '.expectedVisibleAfterRestore' <<<"$STATE_JSON")"
TOTAL_DOCS=$(( HISTORICAL_DOCS + LIVE_DOCS ))
TOTAL_BATCHES=$(( (TOTAL_DOCS + BATCH_SIZE - 1) / BATCH_SIZE ))

if [[ "$DRY_RUN" == "true" ]]; then
  echo "--- dry run: first 20 generated lines ---"
  { stream_bulk_fixture | head -20; } || true
  echo
  echo "State file would be written to: $STATE_FILE"
  exit 0
fi

echo "Ensuring dataset '$INDEX' exists ..."
dataset_state="$(get_dataset_state)"
if [[ -z "$dataset_state" ]]; then
  manager_post_json "$MANAGER_URL" "slack.proto.astra.ManagerApiService/CreateDatasetMetadata" "{
    \"name\": \"$INDEX\",
    \"owner\": \"enterprise-demo\",
    \"serviceNamePattern\": \"$INDEX\"
  }" >/dev/null

  manager_post_json "$MANAGER_URL" "slack.proto.astra.ManagerApiService/UpdatePartitionAssignment" "{
    \"name\": \"$INDEX\",
    \"throughputBytes\": \"4000000\",
    \"partitionIds\": [\"0\"]
  }" >/dev/null

  echo "  created new dataset metadata."
  echo "  waiting briefly for preprocessor rate limits to pick up the new dataset ..."
  sleep 3
else
  dataset_partitions="${dataset_state#*|}"
  if [[ "$dataset_partitions" != "0" ]]; then
    echo "ERROR: dataset '$INDEX' already exists with incompatible active partition metadata:" >&2
    echo "  partitions=$dataset_partitions" >&2
    echo "Use a fresh INDEX value before re-running." >&2
    exit 1
  fi

  existing_hits="$(query_total_hits "$QUERY_URL" "$INDEX" 2>/dev/null || echo 0)"
  if (( existing_hits > 0 )); then
    echo "ERROR: dataset '$INDEX' already contains search-visible data ($existing_hits docs)." >&2
    echo "Use a fresh INDEX value before re-running." >&2
    exit 1
  fi
  echo "  dataset already exists with partition 0 and no visible docs; reusing it."
fi
echo "  done."

echo
echo "Index:                  $INDEX"
echo "Historical window:      $HISTORICAL_FROM .. $HISTORICAL_TO  ($HISTORICAL_DOCS docs)"
echo "Live window:            $LIVE_FROM .. $LIVE_TO  ($LIVE_DOCS docs)"
echo "Visible before restore: $EXPECTED_VISIBLE_BEFORE_RESTORE docs"
echo "Visible after restore:  $EXPECTED_VISIBLE_AFTER_RESTORE docs"
echo "State file:             $STATE_FILE"
echo
echo "Ingesting synthetic fraud data ..."

start_time=$SECONDS
historical_snapshot_count_before="$(docker logs astra_index 2>&1 | grep -c "Finished RW chunk snapshot to S3" || true)"
ingest_window "historical" "$HISTORICAL_DOCS" "historical"

echo "Waiting for the historical chunk snapshot to finish ..."
wait_for_historical_snapshot "$historical_snapshot_count_before" "$HISTORICAL_SNAPSHOT_WAIT_ATTEMPTS"
echo "Snapshot finished. Waiting ${HISTORICAL_SNAPSHOT_SETTLE_SECS}s for metadata to settle ..."
sleep "$HISTORICAL_SNAPSHOT_SETTLE_SECS"
echo "Waiting ${KAFKA_COMMIT_SETTLE_SECS}s for Kafka consumer commits to settle before the indexer restart ..."
sleep "$KAFKA_COMMIT_SETTLE_SECS"

echo "Restarting astra_index so the historical chunk is no longer searchable locally ..."
(cd "$REPO_DIR" && docker compose up -d --force-recreate astra_index >/dev/null)
wait_for_health "indexer" "$INDEXER_HEALTH_URL"
wait_for_historical_to_go_cold
echo "  historical window is now cold and requires restore."

ingest_window "live" "$LIVE_DOCS" "live"

elapsed=$(( SECONDS - start_time ))
echo
echo "Done in ${elapsed}s — $TOTAL_DOCS docs ingested, 0 failed."

echo "Waiting for live-only query shape before restore ..."
wait_for_pre_restore_shape "$QUERY_READY_ATTEMPTS"
echo "  ready: live data visible, historical window cold."

cat <<EOF

Next steps:
  Dashboard:  http://localhost:5601/app/dashboards#/view/enterprise-fraud-investigation
  Restore:    bash scripts/restore-enterprise-fraud-window.sh --index $INDEX
  Fault drill:bash scripts/fail-enterprise-demo-component.sh --index $INDEX --down
EOF
