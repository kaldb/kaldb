#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Astra Quick Start Script
# Usage:
#   ./quick-start.sh [OPTIONS]
#
# Options:
#   --clean     Remove old Astra containers, volumes, and images, then rebuild fresh
#   --help      Show this help message
#
# Examples:
#   ./quick-start.sh           # Start Astra using existing containers/images
#   ./quick-start.sh --clean   # Full rebuild from scratch
#
# After you're done, you can clean everything up with:
#   ./clean-astra.sh
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------------------
CLEAN_BUILD=false

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
wait_for_http() {
  local name=$1 url=$2 max_attempts=${3:-60} sleep_secs=${4:-2}
  echo "⏳ Waiting for $name at $url ..."
  for i in $(seq 1 "$max_attempts"); do
    if curl -sSf "$url" >/dev/null 2>&1; then
      echo "✅ $name is up (attempt $i/$max_attempts)"
      return 0
    fi
    sleep "$sleep_secs"
  done
  echo "❌ $name did not become ready in time." >&2
  exit 1
}

wait_for_kafka() {
  local max_attempts=${1:-60} sleep_secs=${2:-2}
  echo "⏳ Waiting for Kafka broker ..."
  for i in $(seq 1 "$max_attempts"); do
    if docker exec dep_kafka kafka-topics.sh --list --bootstrap-server localhost:9092 >/dev/null 2>&1; then
      echo "✅ Kafka is up (attempt $i/$max_attempts)"
      return 0
    fi
    sleep "$sleep_secs"
  done
  echo "❌ Kafka did not become ready in time." >&2
  exit 1
}

for arg in "$@"; do
  case $arg in
    --clean)
      CLEAN_BUILD=true
      shift
      ;;
    --help)
      echo "Astra Quick Start Script"
      echo ""
      echo "Usage:"
      echo "  ./quick-start.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --clean     Remove old Astra containers, volumes, and images, then rebuild fresh"
      echo "  --help      Show this help message"
      echo ""
      echo "Examples:"
      echo "  ./quick-start.sh           # Start Astra using existing containers/images"
      echo "  ./quick-start.sh --clean   # Full rebuild from scratch"
      echo ""
      echo "To clean up everything afterwards:"
      echo "  ./clean-astra.sh"
      echo ""
      exit 0
      ;;
  esac
done

echo "🚀 Starting Astra demo environment..."

# ------------------------------------------------------------------------------
# Step 1. Stop existing Astra containers (only those defined in docker-compose.yml)
# ------------------------------------------------------------------------------
echo "🧹 Stopping Astra containers from docker-compose.yml..."
if docker compose ps -q | grep . >/dev/null 2>&1; then
  docker compose down --remove-orphans
else
  echo "No Astra containers to stop."
fi

# ------------------------------------------------------------------------------
# Step 2. Handle clean build option
# ------------------------------------------------------------------------------
if [ "$CLEAN_BUILD" = true ]; then
  echo "🔥 Performing full clean build..."
  docker compose down -v --remove-orphans
  docker image prune -af --filter "label=astra-demo=true" || true
else
  echo "⚡ Skipping clean build (use --clean for a fresh start)."
fi

# ------------------------------------------------------------------------------
# Step 3. Build local images if necessary
# ------------------------------------------------------------------------------
if [ "$CLEAN_BUILD" = true ]; then
  echo "🔨 Building Astra Docker image..."
  docker build -t slackhq/astra --label astra-demo=true .
  echo "🔨 Rebuilding Dashboards gateway image..."
  docker compose build astra_dashboards_gateway
else
  echo "⚡ Using existing Astra Docker image (run with --clean to rebuild)."
  echo "⚡ Using existing Dashboards gateway image (run with --clean to rebuild)."
fi

# ------------------------------------------------------------------------------
# Step 4. Start Astra stack
# ------------------------------------------------------------------------------
echo "📦 Starting Astra stack via Docker Compose..."
docker compose up -d

# ------------------------------------------------------------------------------
# Step 5. Wait for services to initialize
# ------------------------------------------------------------------------------
wait_for_kafka 60 2
wait_for_http "Manager API" "http://localhost:8083/health" 60 2
wait_for_http "Preprocessor" "http://localhost:8086/health" 60 2

# ------------------------------------------------------------------------------
# Step 6. Configure Kafka topic and Astra dataset
# ------------------------------------------------------------------------------
echo "📡 Creating Kafka topic (if not exists)..."
docker exec dep_kafka kafka-topics.sh \
  --create \
  --topic test-topic \
  --if-not-exists \
  --bootstrap-server localhost:9092 || true

# CreateDatasetMetadata
echo "🧩 Creating dataset metadata via Manager API..."
curl -sS -XPOST \
  -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
  'http://localhost:8083/slack.proto.astra.ManagerApiService/CreateDatasetMetadata' \
  -d '{
    "name": "test",
    "owner": "test@email.com",
    "serviceNamePattern": "_all"
  }' || echo "CreateDatasetMetadata may have already been applied."

# UpdatePartitionAssignment
echo "📦 Applying partition assignment via Manager API..."
curl -sS -XPOST \
  -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
  'http://localhost:8083/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment' \
  -d '{
    "name": "test",
    "throughputBytes": "4000000",
    "partitionIds": ["0"]
  }' || echo "UpdatePartitionAssignment may have already been applied."

# ------------------------------------------------------------------------------
# Step 7. Summary
# ------------------------------------------------------------------------------
echo ""
echo "✅ Astra demo environment is ready!"
echo "   - Manager UI:   http://localhost:8083"
echo "   - Query API:    http://localhost:8081"
echo "   - Grafana:      http://localhost:3000"
echo "   - OpenSearch:   http://localhost:9200"
echo "   - Dashboards:   http://localhost:5601"
echo ""
echo "To ingest sample data, use the _bulk example in docs/topics/Getting-started.md"
echo "or run tools/loadgen directly."
echo "To stop and remove everything, run: ./clean-astra.sh"
echo ""
