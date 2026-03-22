#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/enterprise-fraud-demo-common.sh"

INDEX="${INDEX:-enterprise_fraud_demo_$(date -u +%Y%m%d_%H%M%S)}"
SKIP_BUILD=false
SKIP_UP=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --index) INDEX="$2"; shift 2 ;;
    --index=*) INDEX="${1#*=}"; shift ;;
    --skip-build) SKIP_BUILD=true; shift ;;
    --skip-up) SKIP_UP=true; shift ;;
    --help)
      cat <<'EOF'
Bring up the local enterprise fraud demo stack, ingest the dataset, and create
the Dashboards saved objects.

Usage:
  bash scripts/run-enterprise-fraud-demo.sh [OPTIONS]

Options:
  --index NAME   Target dataset name
  --skip-build   Skip local Docker image builds
  --skip-up      Skip docker compose startup/recreate steps
  --help         Show this help message
EOF
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

DEMO_SCOPE="$(printf '%s' "$INDEX" | tr -c '[:alnum:]' '_')"

export HISTORICAL_DOCS="${HISTORICAL_DOCS:-750}"
export LIVE_DOCS="${LIVE_DOCS:-400}"
DEMO_RECORDS_PER_DOC="${DEMO_RECORDS_PER_DOC:-1}"
export LOG_LEVEL="${LOG_LEVEL:-info}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-$INDEX}"
export KAFKA_CLIENT_GROUP="${KAFKA_CLIENT_GROUP:-ASTRA-$DEMO_SCOPE}"
export KAFKA_AUTO_COMMIT="${KAFKA_AUTO_COMMIT:-true}"
export KAFKA_AUTO_COMMIT_INTERVAL="${KAFKA_AUTO_COMMIT_INTERVAL:-1000}"
export INDEXER_MAX_MESSAGES_PER_CHUNK="${INDEXER_MAX_MESSAGES_PER_CHUNK:-$(( HISTORICAL_DOCS * DEMO_RECORDS_PER_DOC ))}"
export INDEXER_MAX_BYTES_PER_CHUNK="${INDEXER_MAX_BYTES_PER_CHUNK:-1000000000}"
export INDEXER_MAX_TIME_PER_CHUNK_SECONDS="${INDEXER_MAX_TIME_PER_CHUNK_SECONDS:-86400}"
export INDEXER_MAX_CHUNKS_ON_DISK="${INDEXER_MAX_CHUNKS_ON_DISK:-2}"
export INDEXER_CREATE_RECOVERY_TASKS_ON_START="${INDEXER_CREATE_RECOVERY_TASKS_ON_START:-false}"
export ASTRA_MANAGER_AGGREGATION_SECS="${ASTRA_MANAGER_AGGREGATION_SECS:-2}"
export ASTRA_MANAGER_INITIAL_DELAY_MINS="${ASTRA_MANAGER_INITIAL_DELAY_MINS:-0}"
export ASTRA_MANAGER_REPLICA_DELETE_PERIOD_MINS="${ASTRA_MANAGER_REPLICA_DELETE_PERIOD_MINS:-1}"
export ASTRA_MANAGER_REPLICA_EVICT_PERIOD_MINS="${ASTRA_MANAGER_REPLICA_EVICT_PERIOD_MINS:-1}"
export ASTRA_MANAGER_ASSIGNMENT_PERIOD_MINS="${ASTRA_MANAGER_ASSIGNMENT_PERIOD_MINS:-1}"
export ASTRA_MANAGER_SEARCHABILITY_PERIOD_MINS="${ASTRA_MANAGER_SEARCHABILITY_PERIOD_MINS:-1}"
export ASTRA_MANAGER_REPLICA_RESTORE_PERIOD_MINS="${ASTRA_MANAGER_REPLICA_RESTORE_PERIOD_MINS:-1}"
export ASTRA_MANAGER_REPLICA_RESTORE_LIFESPAN_MINS="${ASTRA_MANAGER_REPLICA_RESTORE_LIFESPAN_MINS:-30}"
export ASTRA_MANAGER_SNAPSHOT_LIFESPAN_MINS="${ASTRA_MANAGER_SNAPSHOT_LIFESPAN_MINS:-4320}"
export ASTRA_MANAGER_CREATION_REPLICA_SETS="${ASTRA_MANAGER_CREATION_REPLICA_SETS:-autoroll}"
export ASTRA_MANAGER_ASSIGNMENT_REPLICA_SETS="${ASTRA_MANAGER_ASSIGNMENT_REPLICA_SETS:-rep1}"
export ASTRA_MANAGER_RESTORE_REPLICA_SETS="${ASTRA_MANAGER_RESTORE_REPLICA_SETS:-rep1}"
export ASTRA_CACHE_SLOTS_PER_INSTANCE="${ASTRA_CACHE_SLOTS_PER_INSTANCE:-8}"
export ASTRA_CACHE_REPLICA_SET="${ASTRA_CACHE_REPLICA_SET:-rep1}"
export ASTRA_CACHE_JAVA_TOOL_OPTIONS="${ASTRA_CACHE_JAVA_TOOL_OPTIONS:--Dastra.ng.dynamicChunkSizes=false}"
export ASTRA_ZK_PATH_PREFIX="${ASTRA_ZK_PATH_PREFIX:-ASTRA_${DEMO_SCOPE}}"
export PREPROCESSOR_SCHEMA_FILE="${PREPROCESSOR_SCHEMA_FILE:-}"
export DEMO_TIME_FROM="${DEMO_TIME_FROM:-now-90d}"
export DEMO_TIME_TO="${DEMO_TIME_TO:-now+15m}"

DISCOVER_TIME_TO="${DEMO_TIME_TO//+/%2B}"
DISCOVER_TIME_RANGE="_g=(time:(from:'$DEMO_TIME_FROM',to:'$DISCOVER_TIME_TO'))"

required_messages_per_chunk="$(( HISTORICAL_DOCS * DEMO_RECORDS_PER_DOC ))"
if [[ "$INDEXER_MAX_MESSAGES_PER_CHUNK" != "$required_messages_per_chunk" ]]; then
  echo "ERROR: INDEXER_MAX_MESSAGES_PER_CHUNK must match the Kafka record count for the historical window." >&2
  echo "  INDEXER_MAX_MESSAGES_PER_CHUNK=$INDEXER_MAX_MESSAGES_PER_CHUNK" >&2
  echo "  HISTORICAL_DOCS=$HISTORICAL_DOCS" >&2
  echo "  DEMO_RECORDS_PER_DOC=$DEMO_RECORDS_PER_DOC" >&2
  echo "  required_messages_per_chunk=$required_messages_per_chunk" >&2
  exit 1
fi

required_live_records="$(( LIVE_DOCS * DEMO_RECORDS_PER_DOC ))"
if (( required_live_records >= INDEXER_MAX_MESSAGES_PER_CHUNK )); then
  echo "ERROR: LIVE_DOCS must stay below the rollover threshold seen by the indexer so the live window remains hot." >&2
  echo "  LIVE_DOCS=$LIVE_DOCS" >&2
  echo "  DEMO_RECORDS_PER_DOC=$DEMO_RECORDS_PER_DOC" >&2
  echo "  required_live_records=$required_live_records" >&2
  echo "  INDEXER_MAX_MESSAGES_PER_CHUNK=$INDEXER_MAX_MESSAGES_PER_CHUNK" >&2
  exit 1
fi

if [[ "$SKIP_UP" != "true" ]]; then
  if [[ "$SKIP_BUILD" != "true" ]]; then
    echo "Building local Astra image ..."
    (cd "$REPO_DIR" && docker build -t slackhq/astra .)

    echo "Building local dashboards gateway image ..."
    (cd "$REPO_DIR" && docker build -t kaldb/dashboards-gateway:local -f contrib/dashboards-gateway.Dockerfile .)
  fi

  echo "Starting local demo stack ..."
  (cd "$REPO_DIR" && docker compose up -d)

  echo "Recreating Astra services with enterprise demo settings ..."
  (cd "$REPO_DIR" && docker compose up -d --force-recreate astra_preprocessor astra_index astra_manager astra_query astra_cache astra_recovery)
fi

echo "Waiting for services ..."
wait_for_http "preprocessor" "http://localhost:8086/"
wait_for_http "manager" "http://localhost:8083/"
wait_for_health "indexer" "http://localhost:8080/health"
wait_for_health "query" "http://localhost:8081/health"
wait_for_health "cache" "http://localhost:8082/health"
wait_for_http "dashboards" "http://localhost:5601/"

echo "Running enterprise fraud ingest into '$INDEX' ..."
bash "$SCRIPT_DIR/ingest-enterprise-fraud-demo.sh" --index "$INDEX"

echo "Creating Dashboards saved objects for '$INDEX' ..."
INDEX="$INDEX" bash "$SCRIPT_DIR/create-enterprise-fraud-dashboards.sh"

cat <<EOF

Demo ready.
  Index:      $INDEX
  Dashboard:  http://localhost:5601/app/dashboards#/view/enterprise-fraud-investigation
  Conflicts:  http://localhost:5601/app/discover#/view/enterprise-fraud-conflict-examples?$DISCOVER_TIME_RANGE
  Historical: http://localhost:5601/app/discover#/view/enterprise-fraud-historical-window?$DISCOVER_TIME_RANGE

Suggested live drill:
  1. Open the dashboard and note only the live window is visible.
  2. Show the "Field conflict examples" panel in Discover.
  3. Run: bash scripts/fail-enterprise-demo-component.sh --index $INDEX --down
  4. Refresh the dashboard to show live queries still work, then run:
     bash scripts/fail-enterprise-demo-component.sh --index $INDEX --up
  5. Run: bash scripts/restore-enterprise-fraud-window.sh --index $INDEX
  6. Refresh the dashboard and show the historical window panel fill in.
EOF
