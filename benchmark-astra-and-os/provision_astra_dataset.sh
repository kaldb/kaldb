#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source common.sh

PROTO_DIR="$(cd "$SCRIPT_DIR/../astra/src/main/proto" && pwd)"
MANAGER_SERVICE="slack.proto.astra.ManagerApiService"

manager_grpc() {
  local method="$1"
  local payload="$2"

  if command -v grpcurl >/dev/null 2>&1; then
    grpcurl \
      -plaintext \
      -import-path "$PROTO_DIR" \
      -proto "$PROTO_DIR/manager_api.proto" \
      -d "$payload" \
      "${ASTRA_MANAGER_HOST}:${ASTRA_MANAGER_PORT}" \
      "${MANAGER_SERVICE}/${method}"
  else
    docker run --rm --network host --user 0:0 \
      -v "$PROTO_DIR:/protos:ro" \
      "$GRPCURL_IMAGE" \
      -plaintext \
      -import-path /protos \
      -proto /protos/manager_api.proto \
      -d "$payload" \
      "${ASTRA_MANAGER_HOST}:${ASTRA_MANAGER_PORT}" \
      "${MANAGER_SERVICE}/${method}"
  fi
}

json_number() {
  jq -en --arg value "$1" '$value | tonumber'
}

ensure_partition() {
  local response
  response="$(manager_grpc ListPartitionMetadata '{}')"
  if jq -e --arg id "$ASTRA_PARTITION_ID" '.partitionMetadata[]? | select(.partitionId == $id)' >/dev/null <<<"$response"; then
    echo "KalDB partition $ASTRA_PARTITION_ID already exists."
    return 0
  fi

  echo "Creating KalDB partition $ASTRA_PARTITION_ID."
  manager_grpc CreatePartition "$(
    jq -nc \
      --arg partition_id "$ASTRA_PARTITION_ID" \
      --argjson max_capacity "$(json_number "$ASTRA_PARTITION_MAX_CAPACITY_BYTES")" \
      '{partition_id: $partition_id, max_capacity: $max_capacity}'
  )" >/dev/null
}

ensure_dataset() {
  if manager_grpc GetDatasetMetadata "$(jq -nc --arg name "$INDEX_NAME" '{name: $name}')" >/dev/null 2>&1; then
    echo "KalDB dataset $INDEX_NAME already exists."
    return 0
  fi

  echo "Creating KalDB dataset $INDEX_NAME."
  manager_grpc CreateDatasetMetadata "$(
    jq -nc \
      --arg name "$INDEX_NAME" \
      --arg owner "$ASTRA_DATASET_OWNER" \
      '{name: $name, owner: $owner, service_name_pattern: $name}'
  )" >/dev/null
}

assign_dataset() {
  echo "Assigning KalDB dataset $INDEX_NAME to partition $ASTRA_PARTITION_ID with throughput $ASTRA_DATASET_THROUGHPUT_BYTES bytes/s."
  manager_grpc UpdatePartitionAssignment "$(
    jq -nc \
      --arg name "$INDEX_NAME" \
      --arg partition_id "$ASTRA_PARTITION_ID" \
      --argjson throughput_bytes "$(json_number "$ASTRA_DATASET_THROUGHPUT_BYTES")" \
      '{name: $name, throughput_bytes: $throughput_bytes, partition_ids: [$partition_id]}'
  )" >/dev/null
}

ensure_partition
ensure_dataset
assign_dataset
