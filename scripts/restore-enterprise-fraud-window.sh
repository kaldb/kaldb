#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/enterprise-fraud-demo-common.sh"

INDEX="${INDEX:-enterprise_fraud_demo}"
MANAGER_URL="${MANAGER_URL:-http://localhost:8083}"
QUERY_URL="${QUERY_URL:-http://localhost:8081}"
WAIT_ATTEMPTS="${WAIT_ATTEMPTS:-90}"

while [[ $# -gt 0 ]]; do
  case $1 in
    --index) INDEX="$2"; shift 2 ;;
    --index=*) INDEX="${1#*=}"; shift ;;
    --help)
      cat <<'EOF'
Restore the historical fraud window for the enterprise demo dataset.

Usage:
  bash scripts/restore-enterprise-fraud-window.sh [OPTIONS]

Options:
  --index NAME   Target dataset name
  --help         Show this help message
EOF
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

STATE_FILE="$(require_demo_state_file "$INDEX")"
HISTORICAL_FROM="$(jq -r '.historical.from' "$STATE_FILE")"
HISTORICAL_TO="$(jq -r '.historical.to' "$STATE_FILE")"
HISTORICAL_DOCS="$(jq -r '.historical.docs' "$STATE_FILE")"
EXPECTED_VISIBLE_AFTER_RESTORE="$(jq -r '.expectedVisibleAfterRestore' "$STATE_FILE")"

to_epoch_ms() {
  python3 -c 'from datetime import datetime, timezone; import sys; print(int(datetime.fromisoformat(sys.argv[1].replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000))' "$1"
}

wait_for_restore() {
  local attempts="${1:-60}"
  local all_hits historical_hits

  for _ in $(seq 1 "$attempts"); do
    all_hits="$(query_total_hits "$QUERY_URL" "$INDEX" 2>/dev/null || echo 0)"
    historical_hits="$(query_event_window_hits "$QUERY_URL" "$INDEX" "historical" 2>/dev/null || echo 0)"
    if [[ "$all_hits" == "$EXPECTED_VISIBLE_AFTER_RESTORE" && "$historical_hits" == "$HISTORICAL_DOCS" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: historical window never became visible after restore request." >&2
  echo "  all_hits=$all_hits historical_hits=$historical_hits expected_all=$EXPECTED_VISIBLE_AFTER_RESTORE expected_historical=$HISTORICAL_DOCS" >&2
  return 1
}

current_historical_hits="$(query_event_window_hits "$QUERY_URL" "$INDEX" "historical" 2>/dev/null || echo 0)"
if [[ "$current_historical_hits" == "$HISTORICAL_DOCS" ]]; then
  echo "Historical window is already restored for '$INDEX'."
  exit 0
fi

echo "Requesting restore for '$INDEX' historical window ..."
HISTORICAL_FROM_EPOCH_MS="$(to_epoch_ms "$HISTORICAL_FROM")"
HISTORICAL_TO_EPOCH_MS="$(to_epoch_ms "$HISTORICAL_TO")"
RESTORE_PAYLOAD="$(jq -nc \
  --arg service_name "$INDEX" \
  --argjson start_time_epoch_ms "$HISTORICAL_FROM_EPOCH_MS" \
  --argjson end_time_epoch_ms "$HISTORICAL_TO_EPOCH_MS" \
  '{
    serviceName: $service_name,
    startTimeEpochMs: $start_time_epoch_ms,
    endTimeEpochMs: $end_time_epoch_ms
  }')"
manager_post_json "$MANAGER_URL" "slack.proto.astra.ManagerApiService/RestoreReplica" "$RESTORE_PAYLOAD" >/dev/null

echo "Waiting for restored replicas to become queryable ..."
wait_for_restore "$WAIT_ATTEMPTS"

echo "Restore complete."
echo "  Dashboard: http://localhost:5601/app/dashboards#/view/enterprise-fraud-investigation"
