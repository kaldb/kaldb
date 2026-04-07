#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NAMESPACE=""
DATASET="logs"
INGEST_SERVICE="ingest"
QUERY_SERVICE="query"
INGEST_LOCAL_PORT="18086"
QUERY_LOCAL_PORT="18081"
BUCKET_COUNT="6"
DOCS_PER_BUCKET="50"
MESSAGE_BYTES="256"
BATCH_SIZE="50"
POLL_TIMEOUT_SECONDS="300"
POLL_INTERVAL_SECONDS="5"
MAVEN_EXTRA_ARGS=()

usage() {
  cat <<EOF
Usage: tools/run_deployed_cluster_e2e.sh [options] [-- extra-maven-args...]

Starts local kubectl port-forwards for the deployed-cluster E2E test, runs the
targeted Maven test, and cleans up the port-forwards on exit.

Options:
  --namespace NAME              Kubernetes namespace. Default: current namespace from kubeconfig.
  --dataset NAME                KalDB dataset. Default: logs
  --ingest-service NAME         Kubernetes ingest service. Default: ingest
  --query-service NAME          Kubernetes query service. Default: query
  --ingest-port PORT            Local ingest port. Default: 18086
  --query-port PORT             Local query port. Default: 18081
  --bucket-count N              Test bucket count. Default: 6
  --docs-per-bucket N           Test docs per bucket. Default: 50
  --message-bytes N             Test message size. Default: 256
  --batch-size N                Test bulk batch size. Default: 50
  --poll-timeout-seconds N      Query polling timeout. Default: 300
  --poll-interval-seconds N     Query polling interval. Default: 5
  -h, --help                    Show this help.

Example:
  tools/run_deployed_cluster_e2e.sh --namespace <namespace>
EOF
}

require_value() {
  local option=$1
  local value=${2:-}
  if [[ -z "$value" ]]; then
    echo "Missing value for $option" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      require_value "$1" "${2:-}"
      NAMESPACE=$2
      shift 2
      ;;
    --dataset)
      require_value "$1" "${2:-}"
      DATASET=$2
      shift 2
      ;;
    --ingest-service)
      require_value "$1" "${2:-}"
      INGEST_SERVICE=$2
      shift 2
      ;;
    --query-service)
      require_value "$1" "${2:-}"
      QUERY_SERVICE=$2
      shift 2
      ;;
    --ingest-port)
      require_value "$1" "${2:-}"
      INGEST_LOCAL_PORT=$2
      shift 2
      ;;
    --query-port)
      require_value "$1" "${2:-}"
      QUERY_LOCAL_PORT=$2
      shift 2
      ;;
    --bucket-count)
      require_value "$1" "${2:-}"
      BUCKET_COUNT=$2
      shift 2
      ;;
    --docs-per-bucket)
      require_value "$1" "${2:-}"
      DOCS_PER_BUCKET=$2
      shift 2
      ;;
    --message-bytes)
      require_value "$1" "${2:-}"
      MESSAGE_BYTES=$2
      shift 2
      ;;
    --batch-size)
      require_value "$1" "${2:-}"
      BATCH_SIZE=$2
      shift 2
      ;;
    --poll-timeout-seconds)
      require_value "$1" "${2:-}"
      POLL_TIMEOUT_SECONDS=$2
      shift 2
      ;;
    --poll-interval-seconds)
      require_value "$1" "${2:-}"
      POLL_INTERVAL_SECONDS=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      MAVEN_EXTRA_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

PORT_FORWARD_LOG_DIR="$(mktemp -d)"
PORT_FORWARD_PIDS=()
KUBECTL_NAMESPACE_ARGS=()

cleanup_port_forwards() {
  for pid in "${PORT_FORWARD_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${PORT_FORWARD_PIDS[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  rm -rf "$PORT_FORWARD_LOG_DIR"
}

on_exit() {
  local status=$?
  cleanup_port_forwards
  exit "$status"
}

on_interrupt() {
  cleanup_port_forwards
  exit 130
}

trap on_exit EXIT
trap on_interrupt INT TERM

wait_for_port_forward() {
  local name=$1
  local local_port=$2
  local pid=$3
  local log_file=$4

  for _ in $(seq 1 60); do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "Port-forward for $name exited early. Log:" >&2
      sed -n '1,120p' "$log_file" >&2
      exit 1
    fi
    if curl -sS --max-time 1 "http://127.0.0.1:${local_port}/health" >/dev/null 2>&1 ||
      curl -sS --max-time 1 "http://127.0.0.1:${local_port}/" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for $name on localhost:$local_port. Log:" >&2
  sed -n '1,120p' "$log_file" >&2
  exit 1
}

start_port_forward() {
  local name=$1
  local service=$2
  local local_port=$3
  local remote_port=$4
  local log_file="$PORT_FORWARD_LOG_DIR/${name}.log"

  echo "Starting port-forward: $name localhost:$local_port -> svc/$service:$remote_port"
  kubectl "${KUBECTL_NAMESPACE_ARGS[@]}" port-forward "svc/${service}" "${local_port}:${remote_port}" \
    >"$log_file" 2>&1 &
  local pid=$!
  PORT_FORWARD_PIDS+=("$pid")
  wait_for_port_forward "$name" "$local_port" "$pid" "$log_file"
}

if [[ -z "$NAMESPACE" ]]; then
  NAMESPACE="$(kubectl config view --minify -o 'jsonpath={.contexts[0].context.namespace}')"
fi
if [[ -n "$NAMESPACE" ]]; then
  KUBECTL_NAMESPACE_ARGS=(-n "$NAMESPACE")
fi

echo "Kubernetes context: $(kubectl config current-context)"
echo "Kubernetes namespace: ${NAMESPACE:-default}"
echo "Dataset: $DATASET"

start_port_forward "ingest" "$INGEST_SERVICE" "$INGEST_LOCAL_PORT" "8086"
start_port_forward "query" "$QUERY_SERVICE" "$QUERY_LOCAL_PORT" "8081"

cd "$REPO_ROOT"
mvn -pl astra -Dtest=DeployedClusterE2ETest test \
  -Dkaldb.e2e.enabled=true \
  -Dkaldb.e2e.ingestBaseUrl="http://localhost:${INGEST_LOCAL_PORT}" \
  -Dkaldb.e2e.queryBaseUrl="http://localhost:${QUERY_LOCAL_PORT}" \
  -Dkaldb.e2e.dataset="$DATASET" \
  -Dkaldb.e2e.bucketCount="$BUCKET_COUNT" \
  -Dkaldb.e2e.docsPerBucket="$DOCS_PER_BUCKET" \
  -Dkaldb.e2e.messageBytes="$MESSAGE_BYTES" \
  -Dkaldb.e2e.batchSize="$BATCH_SIZE" \
  -Dkaldb.e2e.pollTimeoutSeconds="$POLL_TIMEOUT_SECONDS" \
  -Dkaldb.e2e.pollIntervalSeconds="$POLL_INTERVAL_SECONDS" \
  "${MAVEN_EXTRA_ARGS[@]}"
