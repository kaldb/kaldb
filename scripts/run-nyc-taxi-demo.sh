#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Bring up the local demo stack, ingest the NYC taxi dataset into a fresh
# index by default, and create the Dashboards saved objects for that index.
#
# Usage:
#   ./scripts/run-nyc-taxi-demo.sh [OPTIONS]
#
# Options:
#   --index NAME   Target index name
#   --skip-build   Skip local Docker image builds
#   --skip-up      Skip docker compose startup/recreate steps
#   --help         Show this help message
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

INDEX="${INDEX:-nyc_taxis_demo_$(date -u +%Y%m%d_%H%M%S)}"
SKIP_BUILD=false
SKIP_UP=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --index)   INDEX="$2"; shift 2 ;;
    --index=*) INDEX="${1#*=}"; shift ;;
    --skip-build) SKIP_BUILD=true; shift ;;
    --skip-up) SKIP_UP=true; shift ;;
    --help)
      cat <<'EOF'
Bring up the local NYC taxi demo stack, ingest the dataset, and create the
Dashboards saved objects.

Usage:
  bash scripts/run-nyc-taxi-demo.sh [OPTIONS]

Options:
  --index NAME   Target index name
  --skip-build   Skip local Docker image builds
  --skip-up      Skip docker compose startup/recreate steps
  --help         Show this help message
EOF
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

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

if [[ "$SKIP_UP" != "true" ]]; then
  if [[ "$SKIP_BUILD" != "true" ]]; then
    echo "Building local Astra image ..."
    (cd "$REPO_DIR" && docker build -t slackhq/astra .)

    echo "Building local dashboards gateway image ..."
    (cd "$REPO_DIR" && docker build -t kaldb/dashboards-gateway:local -f contrib/dashboards-gateway.Dockerfile .)
  fi

  echo "Starting local demo stack ..."
  (cd "$REPO_DIR" && docker compose up -d)

  echo "Recreating astra_index so the one-chunk setting is applied ..."
  (cd "$REPO_DIR" && docker compose up -d --force-recreate astra_index)
fi

echo "Waiting for services ..."
wait_for_http "preprocessor" "http://localhost:8086/"
wait_for_http "manager" "http://localhost:8083/"
wait_for_health "indexer" "http://localhost:8080/health"
wait_for_health "query" "http://localhost:8081/health"
wait_for_http "dashboards" "http://localhost:5601/"

echo "Running taxi ingest into '$INDEX' ..."
bash "$SCRIPT_DIR/ingest-nyc-taxis.sh" --index "$INDEX"

echo "Creating Dashboards saved objects for '$INDEX' ..."
INDEX="$INDEX" bash "$SCRIPT_DIR/create-nyc-taxi-dashboards.sh"

cat <<EOF

Demo ready.
  Index:      $INDEX
  Dashboard:  http://localhost:5601/app/dashboards#/view/nyc-taxi-demo
  Discover:   http://localhost:5601/app/discover#/view/nyc-raw-trips

If you rerun this script, it will use a new timestamped index name unless you
pass --index explicitly.
EOF
