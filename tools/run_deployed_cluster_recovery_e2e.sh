#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

NAMESPACE=""
DATASET="logs"
INGEST_SERVICE="ingest"
QUERY_SERVICE="query"
INDEX_STATEFULSET="index"
INDEX_CONFIGMAP="index-config"
INDEX_REPLICAS=""
MAX_OFFSET_DELAY_MESSAGES="100"
PATCH_MAX_OFFSET_DELAY="true"
INGEST_LOCAL_PORT="18086"
QUERY_LOCAL_PORT="18081"
RUN_ID="run$(date -u +%Y%m%d%H%M%S)"
BASELINE_DOCS="20000"
BACKLOG_DOCS="2000"
MESSAGE_BYTES="2048"
BATCH_SIZE="500"
BUCKET_COUNT="20"
BASELINE_SETTLE_SECONDS="30"
POLL_TIMEOUT_SECONDS="600"
POLL_INTERVAL_SECONDS="5"
INDEX_WAIT_SECONDS="300"
REQUIRE_RECOVERY_BACKLOG="false"
MAVEN_EXTRA_ARGS=()

usage() {
  cat <<EOF
Usage: tools/run_deployed_cluster_recovery_e2e.sh [options] [-- extra-maven-args...]

Runs DeployedClusterRecoveryE2ETest as a three-phase E2E harness:
  1. BASELINE with indexers up.
  2. Scale the index StatefulSet to 0.
  3. BACKLOG while ingest continues and indexers are down.
  4. Restore the index StatefulSet replica count.
  5. VERIFY that backlog docs become queryable.

Options:
  --namespace NAME              Kubernetes namespace. Default: current namespace from kubeconfig.
  --dataset NAME                KalDB dataset. Default: logs
  --ingest-service NAME         Kubernetes ingest service. Default: ingest
  --query-service NAME          Kubernetes query service. Default: query
  --index-statefulset NAME      Index StatefulSet to scale down/up. Default: index
  --index-configmap NAME        ConfigMap containing indexer config. Default: index-config
  --index-replicas N            Replica count to restore. Default: current StatefulSet spec.replicas
  --max-offset-delay-messages N Temporarily set indexer maxOffsetDelayMessages. Default: 100
  --skip-max-offset-delay-patch Use the deployed maxOffsetDelayMessages as-is.
  --ingest-port PORT            Local ingest port. Default: 18086
  --query-port PORT             Local query port. Default: 18081
  --run-id ID                   Recovery test run id. Default: run<UTC timestamp>
  --baseline-docs N             Baseline docs. Default: 20000
  --backlog-docs N              Backlog docs ingested while index is down. Default: 2000
  --message-bytes N             Test message size. Default: 2048
  --batch-size N                Test bulk batch size. Default: 500
  --bucket-count N              Test bucket count. Default: 20
  --baseline-settle-seconds N   Sleep after baseline verification. Default: 30
  --poll-timeout-seconds N      Verification polling timeout. Default: 600
  --poll-interval-seconds N     Verification polling interval. Default: 5
  --index-wait-seconds N        Timeout for StatefulSet scale operations. Default: 300
  --require-recovery-backlog    Fail if backlog docs are definitely below maxOffsetDelayMessages.
  -h, --help                    Show this help.

Example:
  tools/run_deployed_cluster_recovery_e2e.sh \\
    --namespace <namespace>
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
    --index-statefulset)
      require_value "$1" "${2:-}"
      INDEX_STATEFULSET=$2
      shift 2
      ;;
    --index-configmap)
      require_value "$1" "${2:-}"
      INDEX_CONFIGMAP=$2
      shift 2
      ;;
    --index-replicas)
      require_value "$1" "${2:-}"
      INDEX_REPLICAS=$2
      shift 2
      ;;
    --max-offset-delay-messages)
      require_value "$1" "${2:-}"
      MAX_OFFSET_DELAY_MESSAGES=$2
      PATCH_MAX_OFFSET_DELAY="true"
      shift 2
      ;;
    --skip-max-offset-delay-patch)
      PATCH_MAX_OFFSET_DELAY="false"
      shift
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
    --run-id)
      require_value "$1" "${2:-}"
      RUN_ID=$2
      shift 2
      ;;
    --baseline-docs)
      require_value "$1" "${2:-}"
      BASELINE_DOCS=$2
      shift 2
      ;;
    --backlog-docs)
      require_value "$1" "${2:-}"
      BACKLOG_DOCS=$2
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
    --bucket-count)
      require_value "$1" "${2:-}"
      BUCKET_COUNT=$2
      shift 2
      ;;
    --baseline-settle-seconds)
      require_value "$1" "${2:-}"
      BASELINE_SETTLE_SECONDS=$2
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
    --index-wait-seconds)
      require_value "$1" "${2:-}"
      INDEX_WAIT_SECONDS=$2
      shift 2
      ;;
    --require-recovery-backlog)
      REQUIRE_RECOVERY_BACKLOG="true"
      shift
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
RESTORE_INDEX_REPLICAS=""
INDEX_RESTORE_NEEDED="false"
ORIGINAL_INDEX_CONFIG_FILE="$PORT_FORWARD_LOG_DIR/original-index-config.yaml"
CONFIG_RESTORE_NEEDED="false"
CONFIG_ROLLOUT_NEEDED="false"

kubectl_ns_cmd() {
  kubectl "${KUBECTL_NAMESPACE_ARGS[@]}" "$@"
}

cleanup() {
  local status=$?
  set +e

  if [[ "$CONFIG_RESTORE_NEEDED" == "true" ]]; then
    restore_index_config
    CONFIG_ROLLOUT_NEEDED="true"
  fi

  if [[ "$INDEX_RESTORE_NEEDED" == "true" && -n "$RESTORE_INDEX_REPLICAS" ]]; then
    echo "Restoring $INDEX_STATEFULSET StatefulSet to $RESTORE_INDEX_REPLICAS replicas"
    kubectl_ns_cmd scale "statefulset/${INDEX_STATEFULSET}" --replicas="$RESTORE_INDEX_REPLICAS" >/dev/null 2>&1
    wait_for_statefulset_replicas "$RESTORE_INDEX_REPLICAS" >/dev/null 2>&1
    CONFIG_ROLLOUT_NEEDED="false"
  elif [[ "$CONFIG_ROLLOUT_NEEDED" == "true" && -n "$RESTORE_INDEX_REPLICAS" && "$RESTORE_INDEX_REPLICAS" != "0" ]]; then
    rollout_restart_index >/dev/null 2>&1
  fi

  for pid in "${PORT_FORWARD_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${PORT_FORWARD_PIDS[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  rm -rf "$PORT_FORWARD_LOG_DIR"

  return "$status"
}

on_exit() {
  local status=$?
  cleanup
  exit "$status"
}

on_interrupt() {
  trap - EXIT
  cleanup
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
  kubectl_ns_cmd port-forward "svc/${service}" "${local_port}:${remote_port}" \
    >"$log_file" 2>&1 &
  local pid=$!
  PORT_FORWARD_PIDS+=("$pid")
  wait_for_port_forward "$name" "$local_port" "$pid" "$log_file"
}

statefulset_replicas() {
  local value
  value="$(kubectl_ns_cmd get "statefulset/${INDEX_STATEFULSET}" -o jsonpath='{.spec.replicas}')"
  if [[ -z "$value" ]]; then
    value="1"
  fi
  printf '%s\n' "$value"
}

wait_for_statefulset_replicas() {
  local expected=$1

  for _ in $(seq 1 "$INDEX_WAIT_SECONDS"); do
    local desired current ready
    desired="$(kubectl_ns_cmd get "statefulset/${INDEX_STATEFULSET}" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
    current="$(kubectl_ns_cmd get "statefulset/${INDEX_STATEFULSET}" -o jsonpath='{.status.replicas}' 2>/dev/null || true)"
    ready="$(kubectl_ns_cmd get "statefulset/${INDEX_STATEFULSET}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    desired="${desired:-0}"
    current="${current:-0}"
    ready="${ready:-0}"

    if [[ "$desired" == "$expected" && "$current" == "$expected" && "$ready" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for statefulset/$INDEX_STATEFULSET to reach $expected replicas" >&2
  kubectl_ns_cmd get "statefulset/${INDEX_STATEFULSET}" >&2 || true
  return 1
}

scale_index() {
  local replicas=$1
  echo "Scaling statefulset/$INDEX_STATEFULSET to $replicas replicas"
  kubectl_ns_cmd scale "statefulset/${INDEX_STATEFULSET}" --replicas="$replicas"
  wait_for_statefulset_replicas "$replicas"
}

rollout_restart_index() {
  echo "Restarting statefulset/$INDEX_STATEFULSET to pick up indexer config"
  kubectl_ns_cmd rollout restart "statefulset/${INDEX_STATEFULSET}"
  kubectl_ns_cmd rollout status "statefulset/${INDEX_STATEFULSET}" --timeout="${INDEX_WAIT_SECONDS}s"
  wait_for_statefulset_replicas "$RESTORE_INDEX_REPLICAS"
}

index_config_yaml() {
  kubectl_ns_cmd get "configmap/${INDEX_CONFIGMAP}" -o jsonpath='{.data.config\.yaml}'
}

patch_index_config_yaml() {
  local config_yaml=$1
  local patch
  patch="$(jq -n --arg config_yaml "$config_yaml" '{data: {"config.yaml": $config_yaml}}')"
  kubectl_ns_cmd patch "configmap/${INDEX_CONFIGMAP}" --type merge --patch "$patch"
}

read_max_offset_delay() {
  index_config_yaml 2>/dev/null |
    sed -n 's/^[[:space:]]*maxOffsetDelayMessages:[[:space:]]*"\{0,1\}\([0-9][0-9]*\).*/\1/p' |
    head -n 1
}

set_index_max_offset_delay() {
  local config_yaml patched_config_yaml

  if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to patch configmap/$INDEX_CONFIGMAP." >&2
    exit 1
  fi

  config_yaml="$(index_config_yaml)"
  printf '%s\n' "$config_yaml" >"$ORIGINAL_INDEX_CONFIG_FILE"
  if ! grep -q '^[[:space:]]*maxOffsetDelayMessages:' "$ORIGINAL_INDEX_CONFIG_FILE"; then
    echo "Could not find maxOffsetDelayMessages in configmap/$INDEX_CONFIGMAP." >&2
    exit 1
  fi

  patched_config_yaml="$(
    sed -E \
      "s/^([[:space:]]*maxOffsetDelayMessages:[[:space:]]*)\"?[0-9]+\"?([[:space:]]*)$/\1${MAX_OFFSET_DELAY_MESSAGES}\2/" \
      "$ORIGINAL_INDEX_CONFIG_FILE"
  )"

  echo "Temporarily setting indexer maxOffsetDelayMessages to $MAX_OFFSET_DELAY_MESSAGES"
  patch_index_config_yaml "$patched_config_yaml"
  CONFIG_RESTORE_NEEDED="true"
}

restore_index_config() {
  if [[ ! -s "$ORIGINAL_INDEX_CONFIG_FILE" ]]; then
    return 0
  fi

  echo "Restoring configmap/$INDEX_CONFIGMAP"
  patch_index_config_yaml "$(<"$ORIGINAL_INDEX_CONFIG_FILE")"
  CONFIG_RESTORE_NEEDED="false"
}

warn_if_backlog_below_recovery_threshold() {
  local max_offset_delay
  max_offset_delay="$(read_max_offset_delay || true)"

  if [[ -z "$max_offset_delay" ]]; then
    echo "Could not read maxOffsetDelayMessages from configmap/$INDEX_CONFIGMAP; skipping recovery threshold check" >&2
    return
  fi

  if (( BACKLOG_DOCS <= max_offset_delay )); then
    echo "Warning: backlog docs ($BACKLOG_DOCS) are below maxOffsetDelayMessages ($max_offset_delay)." >&2
    echo "This can test post-outage catch-up, but it is too small to force recovery task creation." >&2
    if [[ "$REQUIRE_RECOVERY_BACKLOG" == "true" ]]; then
      exit 1
    fi
  elif (( RESTORE_INDEX_REPLICAS > 1 && BACKLOG_DOCS <= max_offset_delay * RESTORE_INDEX_REPLICAS )); then
    echo "Warning: backlog docs ($BACKLOG_DOCS) exceed maxOffsetDelayMessages ($max_offset_delay) only in total." >&2
    echo "If writes are spread across $RESTORE_INDEX_REPLICAS index partitions, recovery task creation may still not be forced." >&2
  fi
}

run_recovery_phase() {
  local phase=$1

  echo "Running recovery E2E phase: $phase"
  cd "$REPO_ROOT"
  mvn -pl astra -Dtest=DeployedClusterRecoveryE2ETest test \
    -Dkaldb.e2e.recovery.enabled=true \
    -Dkaldb.e2e.recovery.phase="$phase" \
    -Dkaldb.e2e.recovery.runId="$RUN_ID" \
    -Dkaldb.e2e.ingestBaseUrl="http://localhost:${INGEST_LOCAL_PORT}" \
    -Dkaldb.e2e.queryBaseUrl="http://localhost:${QUERY_LOCAL_PORT}" \
    -Dkaldb.e2e.dataset="$DATASET" \
    -Dkaldb.e2e.recovery.baselineDocs="$BASELINE_DOCS" \
    -Dkaldb.e2e.recovery.backlogDocs="$BACKLOG_DOCS" \
    -Dkaldb.e2e.recovery.messageBytes="$MESSAGE_BYTES" \
    -Dkaldb.e2e.recovery.batchSize="$BATCH_SIZE" \
    -Dkaldb.e2e.recovery.bucketCount="$BUCKET_COUNT" \
    -Dkaldb.e2e.recovery.baselineSettleSeconds="$BASELINE_SETTLE_SECONDS" \
    -Dkaldb.e2e.recovery.pollTimeoutSeconds="$POLL_TIMEOUT_SECONDS" \
    -Dkaldb.e2e.recovery.pollIntervalSeconds="$POLL_INTERVAL_SECONDS" \
    "${MAVEN_EXTRA_ARGS[@]}"
}

if [[ -z "$NAMESPACE" ]]; then
  NAMESPACE="$(kubectl config view --minify -o 'jsonpath={.contexts[0].context.namespace}')"
fi
if [[ -n "$NAMESPACE" ]]; then
  KUBECTL_NAMESPACE_ARGS=(-n "$NAMESPACE")
fi

if [[ -z "$INDEX_REPLICAS" ]]; then
  RESTORE_INDEX_REPLICAS="$(statefulset_replicas)"
else
  RESTORE_INDEX_REPLICAS="$INDEX_REPLICAS"
fi
if (( RESTORE_INDEX_REPLICAS <= 0 )); then
  echo "Index replica count must be greater than zero. Set --index-replicas if the StatefulSet is currently scaled down." >&2
  exit 1
fi

echo "Kubernetes context: $(kubectl config current-context)"
echo "Kubernetes namespace: ${NAMESPACE:-default}"
echo "Dataset: $DATASET"
echo "Run id: $RUN_ID"
echo "Index StatefulSet: $INDEX_STATEFULSET"
echo "Index restore replicas: $RESTORE_INDEX_REPLICAS"

if [[ "$PATCH_MAX_OFFSET_DELAY" == "true" ]]; then
  set_index_max_offset_delay
  scale_index "$RESTORE_INDEX_REPLICAS"
  rollout_restart_index
else
  scale_index "$RESTORE_INDEX_REPLICAS"
fi
warn_if_backlog_below_recovery_threshold
start_port_forward "ingest" "$INGEST_SERVICE" "$INGEST_LOCAL_PORT" "8086"
start_port_forward "query" "$QUERY_SERVICE" "$QUERY_LOCAL_PORT" "8081"

run_recovery_phase "BASELINE"

INDEX_RESTORE_NEEDED="true"
scale_index "0"
run_recovery_phase "BACKLOG"

scale_index "$RESTORE_INDEX_REPLICAS"
INDEX_RESTORE_NEEDED="false"
run_recovery_phase "VERIFY"

if [[ "$CONFIG_RESTORE_NEEDED" == "true" ]]; then
  restore_index_config
  CONFIG_ROLLOUT_NEEDED="true"
  rollout_restart_index
  CONFIG_ROLLOUT_NEEDED="false"
fi
