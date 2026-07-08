#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# KalDB Quick Start Script
# Usage:
#   ./quick_start.sh [OPTIONS]
#
# Options:
#   --clean     Remove old KalDB containers, volumes, and images, then rebuild fresh
#   --help      Show this help message
#
# Examples:
#   ./quick_start.sh           # Start KalDB using existing containers/images
#   ./quick_start.sh --clean   # Full rebuild from scratch
#
# After you're done, you can clean everything up with:
#   ./clean.sh
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------------------
CLEAN_BUILD=false
readonly SCRIPT_IMAGE_LABEL_KEY="kaldb.quick_start.managed"
readonly SCRIPT_IMAGE_LABEL_VALUE="true"

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
require_command() {
  local command_name=$1 install_hint=${2:-}

  if command -v "$command_name" >/dev/null 2>&1; then
    return
  fi

  echo "❌ Missing required command: $command_name" >&2
  if [ -n "$install_hint" ]; then
    echo "   $install_hint" >&2
  fi
  echo "   On Debian/Ubuntu, run: scripts/setup-linux-deps.sh" >&2
  exit 1
}

check_dependencies() {
  require_command docker "Docker is required to build and run the KalDB demo stack."
  require_command curl "curl is required to configure and check local KalDB services."

  if ! docker compose version >/dev/null 2>&1; then
    echo "❌ Missing required command: docker compose" >&2
    echo "   Docker Compose v2 is required to run the KalDB demo stack." >&2
    echo "   On Debian/Ubuntu, run: scripts/setup-linux-deps.sh" >&2
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    echo "❌ Cannot connect to the Docker daemon." >&2
    echo "   Start Docker and make sure your user can access /var/run/docker.sock." >&2
    echo "   If scripts/setup-linux-deps.sh just added you to the docker group, log out and back in." >&2
    echo "   To apply the group in the current shell, you can run: newgrp docker" >&2
    exit 1
  fi
}

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
      ;;
    --help)
      echo "KalDB Quick Start Script"
      echo ""
      echo "Usage:"
      echo "  ./quick_start.sh [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --clean     Remove old KalDB containers, volumes, and images, then rebuild fresh"
      echo "  --help      Show this help message"
      echo ""
      echo "Examples:"
      echo "  ./quick_start.sh           # Start KalDB using existing containers/images"
      echo "  ./quick_start.sh --clean   # Full rebuild from scratch"
      echo ""
      echo "To clean up everything afterwards:"
      echo "  ./clean.sh"
      echo ""
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      echo "Run './quick_start.sh --help' for usage." >&2
      exit 1
      ;;
  esac
done

check_dependencies

echo "🚀 Starting KalDB demo environment..."

# ------------------------------------------------------------------------------
# Step 1. Stop existing KalDB containers (only those defined in docker-compose.yml)
# ------------------------------------------------------------------------------
echo "🧹 Stopping KalDB containers from docker-compose.yml..."
if docker compose ps -q | grep . >/dev/null 2>&1; then
  docker compose down --remove-orphans
else
  echo "No KalDB containers to stop."
fi

# ------------------------------------------------------------------------------
# Step 2. Handle clean build option
# ------------------------------------------------------------------------------
if [ "$CLEAN_BUILD" = true ]; then
  echo "🔥 Performing full clean build..."
  docker compose down -v --remove-orphans
  docker image prune -af --filter "label=$SCRIPT_IMAGE_LABEL_KEY=$SCRIPT_IMAGE_LABEL_VALUE" || true
else
  echo "⚡ Skipping clean build (use --clean for a fresh start)."
fi

# ------------------------------------------------------------------------------
# Step 3. Build local images if necessary
# ------------------------------------------------------------------------------
IMAGE_LABEL_VALUE=$(docker image inspect --format "{{ index .Config.Labels \"$SCRIPT_IMAGE_LABEL_KEY\" }}" slackhq/astra 2>/dev/null || true)

if [ "$CLEAN_BUILD" = true ] || [ "$IMAGE_LABEL_VALUE" != "$SCRIPT_IMAGE_LABEL_VALUE" ]; then
  echo "🔨 Building KalDB Docker image..."
  docker build \
    -t slackhq/astra \
    -t astra:latest \
    --label "$SCRIPT_IMAGE_LABEL_KEY=$SCRIPT_IMAGE_LABEL_VALUE" \
    .
  echo "🔨 Rebuilding Dashboards gateway image..."
  docker compose build astra_dashboards_gateway
else
  echo "⚡ Using existing KalDB Docker image (run with --clean to rebuild)."
  echo "⚡ Using existing Dashboards gateway image (run with --clean to rebuild)."
fi

# ------------------------------------------------------------------------------
# Step 4. Start KalDB stack
# ------------------------------------------------------------------------------
echo "📦 Starting KalDB stack via Docker Compose..."
docker compose up -d

# ------------------------------------------------------------------------------
# Step 5. Wait for services to initialize
# ------------------------------------------------------------------------------
wait_for_kafka 60 2
wait_for_http "Manager API" "http://localhost:8083/health" 60 2
wait_for_http "Preprocessor" "http://localhost:8086/health" 60 2

# ------------------------------------------------------------------------------
# Step 6. Configure Kafka topic and KalDB dataset
# ------------------------------------------------------------------------------
echo "📡 Creating Kafka topic (if not exists)..."
docker exec dep_kafka kafka-topics.sh \
  --create \
  --topic test-topic \
  --if-not-exists \
  --bootstrap-server localhost:9092 || true

# CreateDatasetMetadata
echo "🧩 Creating exact-match dataset metadata via Manager API..."
curl -sS -XPOST \
  -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
  'http://localhost:8083/slack.proto.astra.ManagerApiService/CreateDatasetMetadata' \
  -d '{
    "name": "test",
    "owner": "test@email.com",
    "serviceNamePattern": "test"
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
echo "✅ KalDB demo environment is ready!"
echo "   - Admin UI:     http://localhost:8083/admin/"
echo "   - Manager API:  http://localhost:8083"
echo "   - Query API:    http://localhost:8081"
echo "   - Grafana:      http://localhost:3000"
echo "   - Zipkin UI:    http://localhost:9411"
echo "   - OpenSearch:   http://localhost:9200"
echo "   - Dashboards:   http://localhost:5601"
echo ""
echo "For manual API examples, see docs/topics/Getting-started.md."
echo "To stop and remove everything, run: ./clean.sh"
echo ""
