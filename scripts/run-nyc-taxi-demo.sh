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
STACK_STABILITY_CHECKS="${STACK_STABILITY_CHECKS:-5}"
STACK_STABILITY_SLEEP_SECS="${STACK_STABILITY_SLEEP_SECS:-2}"

REQUIRED_RUNNING_SERVICES=(
  zookeeper
  kafka
  s3
  openzipkin
  astra_preprocessor
  astra_index
  astra_manager
  astra_query
  astra_cache
  astra_recovery
  opensearch
  astra_dashboards_gateway
  opensearch_dashboards
)

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
    if curl -fsS -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: $name did not become reachable at $url" >&2
  return 1
}

compose_service_container_id() {
  (cd "$REPO_DIR" && docker compose ps -q "$1")
}

compose_service_state() {
  local service="$1"
  local container_id

  container_id="$(compose_service_container_id "$service")"
  if [[ -z "$container_id" ]]; then
    echo "missing|none"
    return 1
  fi

  docker inspect -f '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
    "$container_id" 2>/dev/null
}

service_ready_once() {
  local service="$1"
  local state status health

  state="$(compose_service_state "$service" 2>/dev/null || true)"
  status="${state%%|*}"
  health="${state#*|}"

  [[ "$status" == "running" ]] || return 1
  [[ "$health" == "none" || "$health" == "healthy" ]] || return 1
}

wait_for_service_ready() {
  local service="$1"
  local attempts="${2:-90}"
  local state=""

  for _ in $(seq 1 "$attempts"); do
    if service_ready_once "$service"; then
      return 0
    fi
    sleep 2
  done

  state="$(compose_service_state "$service" 2>/dev/null || echo 'missing|none')"
  echo "ERROR: compose service '$service' did not become ready (state=$state)" >&2
  return 1
}

health_ready_once() {
  local url="$1"
  local response=""

  response="$(curl -fsS "$url" 2>/dev/null || true)"
  [[ "$response" == *'"healthy":true'* ]]
}

http_ready_once() {
  local url="$1"
  curl -fsS -o /dev/null "$url" 2>/dev/null
}

require_stable_stack() {
  local checks="${1:-$STACK_STABILITY_CHECKS}"
  local sleep_secs="${2:-$STACK_STABILITY_SLEEP_SECS}"
  local check_num service state

  for check_num in $(seq 1 "$checks"); do
    for service in "${REQUIRED_RUNNING_SERVICES[@]}"; do
      if ! service_ready_once "$service"; then
        state="$(compose_service_state "$service" 2>/dev/null || echo 'missing|none')"
        echo "ERROR: compose service '$service' is not ready during stability check $check_num/$checks (state=$state)" >&2
        return 1
      fi
    done

    health_ready_once "http://localhost:8086/health" || {
      echo "ERROR: preprocessor health check failed during stability check $check_num/$checks" >&2
      return 1
    }
    health_ready_once "http://localhost:8083/health" || {
      echo "ERROR: manager health check failed during stability check $check_num/$checks" >&2
      return 1
    }
    health_ready_once "http://localhost:8080/health" || {
      echo "ERROR: indexer health check failed during stability check $check_num/$checks" >&2
      return 1
    }
    health_ready_once "http://localhost:8081/health" || {
      echo "ERROR: query health check failed during stability check $check_num/$checks" >&2
      return 1
    }
    http_ready_once "http://localhost:5601/api/status" || {
      echo "ERROR: dashboards status check failed during stability check $check_num/$checks" >&2
      return 1
    }

    if (( check_num < checks )); then
      sleep "$sleep_secs"
    fi
  done
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
for service in "${REQUIRED_RUNNING_SERVICES[@]}"; do
  wait_for_service_ready "$service"
done
wait_for_health "preprocessor" "http://localhost:8086/health"
wait_for_health "manager" "http://localhost:8083/health"
wait_for_health "indexer" "http://localhost:8080/health"
wait_for_health "query" "http://localhost:8081/health"
wait_for_http "dashboards" "http://localhost:5601/api/status"
echo "Verifying the full stack is healthy and stable before ingest ..."
require_stable_stack

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
