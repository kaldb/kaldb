#!/usr/bin/env bash
set -euo pipefail

CLEAN_STACK=0

for arg in "$@"; do
  case "$arg" in
    --clean)
      CLEAN_STACK=1
      ;;
    --help)
      cat <<'EOF'
Usage:
  ./scripts/start-dashboards-stack.sh [--clean]

Options:
  --clean   Remove volumes and restart from a fully clean local stack.

What this script does:
  1. Runs quick_start.sh from this branch
  2. Waits for the Dashboards gateway and OpenSearch Dashboards to become ready

Why this exists:
  - it gives one stable entrypoint for the UI smoke test
  - it waits for the UI services that quick_start.sh does not explicitly wait for
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

log() {
  printf '%s\n' "$*"
}

wait_for_http() {
  local name="$1"
  local url="$2"
  local attempts="${3:-60}"
  local sleep_secs="${4:-2}"

  log "Waiting for $name at $url ..."
  for _ in $(seq 1 "$attempts"); do
    if curl -sSf "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_secs"
  done

  log "ERROR: $name did not become ready"
  exit 1
}

wait_for_gateway() {
  local attempts="${1:-60}"
  local sleep_secs="${2:-2}"

  log "Waiting for Dashboards gateway ..."
  for _ in $(seq 1 "$attempts"); do
    if docker exec dep_astra_dashboards_gateway curl -fsS \
      "http://localhost:9200/_astra/gateway/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_secs"
  done

  log "ERROR: Dashboards gateway did not become ready"
  exit 1
}

if (( CLEAN_STACK == 1 )); then
  ./quick_start.sh --clean
else
  ./quick_start.sh
fi

wait_for_http "OpenSearch Dashboards" "http://localhost:5601/api/status" 60 2
wait_for_gateway 60 2

log ""
log "Stack ready."
log "Next step:"
log "  .venv/bin/python scripts/verify-dashboards-full-ui-e2e.py"
