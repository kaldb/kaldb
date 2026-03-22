#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/enterprise-fraud-demo-common.sh"

INDEX="${INDEX:-enterprise_fraud_demo}"
COMPONENT="${COMPONENT:-astra_cache}"
QUERY_URL="${QUERY_URL:-http://localhost:8081}"
ACTION=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --index) INDEX="$2"; shift 2 ;;
    --index=*) INDEX="${1#*=}"; shift ;;
    --component) COMPONENT="$2"; shift 2 ;;
    --component=*) COMPONENT="${1#*=}"; shift ;;
    --down) ACTION="down"; shift ;;
    --up) ACTION="up"; shift ;;
    --help)
      cat <<'EOF'
Run the enterprise demo fault-isolation drill.

Usage:
  bash scripts/fail-enterprise-demo-component.sh [--index NAME] [--component astra_cache] --down|--up

Options:
  --index NAME        Target dataset name
  --component NAME    Docker compose service to stop/start (default: astra_cache)
  --down              Stop the selected component
  --up                Start the selected component again
  --help              Show this help message
EOF
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$ACTION" ]]; then
  echo "ERROR: one of --down or --up is required." >&2
  exit 1
fi

STATE_FILE="$(require_demo_state_file "$INDEX")"
EXPECTED_LIVE_DOCS="$(jq -r '.live.docs' "$STATE_FILE")"

verify_live_query() {
  local attempts="${1:-45}"
  local live_hits

  for _ in $(seq 1 "$attempts"); do
    live_hits="$(query_event_window_hits "$QUERY_URL" "$INDEX" "live" 2>/dev/null || echo 0)"
    if [[ "$live_hits" == "$EXPECTED_LIVE_DOCS" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: live dashboard query stopped matching the expected count during the drill." >&2
  echo "  live_hits=$live_hits expected=$EXPECTED_LIVE_DOCS" >&2
  return 1
}

if [[ "$ACTION" == "down" ]]; then
  echo "Stopping component '$COMPONENT' ..."
  (cd "$REPO_DIR" && docker compose stop "$COMPONENT")
  echo "Verifying hot-path query remains healthy ..."
  verify_live_query
  echo "Component stopped; live query still healthy."
else
  echo "Starting component '$COMPONENT' ..."
  (cd "$REPO_DIR" && docker compose up -d "$COMPONENT")
  if [[ "$COMPONENT" == "astra_cache" ]]; then
    wait_for_health "cache" "http://localhost:8082/health" 60
  fi
  echo "Verifying hot-path query remains healthy ..."
  verify_live_query
  echo "Component started; live query healthy."
fi
