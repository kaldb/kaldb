#!/usr/bin/env bash
set -Eeuo pipefail

INDEX_NAME="${INDEX_NAME:-hits}"
OS_URL="${OS_URL:-http://localhost:9200}"
ZK_CONTAINER="${ZK_CONTAINER:-dep_zookeeper}"
ZK_SERVER="${ZK_SERVER:-localhost:2181}"

usage() {
  cat <<EOF
Usage: $0 [--physical]

Print OpenSearch and KalDB storage sizes for the ClickBench benchmark.

Default mode uses metadata only:
  - OpenSearch _cat/indices primary store size
  - KalDB persisted snapshot sizeInBytes from ZooKeeper

--physical also prints docker mount paths and runs du -sh on them. This walks
the filesystem and can add disk I/O, so use it after timed benchmark runs.

Environment:
  INDEX_NAME     OpenSearch index name, default: hits
  OS_URL         OpenSearch URL, default: http://localhost:9200
  ZK_CONTAINER  ZooKeeper container name, default: dep_zookeeper
  ZK_SERVER     ZooKeeper address inside container, default: localhost:2181
EOF
}

physical=false
if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage
  exit 0
elif [ "${1:-}" = "--physical" ]; then
  physical=true
elif [ "$#" -gt 0 ]; then
  usage >&2
  exit 1
fi

zk_children() {
  local path="$1"
  docker exec "$ZK_CONTAINER" zkCli.sh -server "$ZK_SERVER" ls "$path" 2>/dev/null \
    | awk '/^\[/{gsub(/^\[/,""); gsub(/\]$/,""); gsub(/,/,""); print}'
}

zk_json() {
  local path="$1"
  docker exec "$ZK_CONTAINER" zkCli.sh -server "$ZK_SERVER" get "$path" 2>/dev/null \
    | awk 'seen || /^\{/ {seen=1; print; if (/^}/) exit}'
}

echo "OpenSearch metadata size"
curl -fsS "$OS_URL/_cat/indices/$INDEX_NAME?h=index,pri,rep,docs.count,pri.store.size,store.size&bytes=b"
echo

os_primary_bytes="$(
  curl -fsS "$OS_URL/_cat/indices/$INDEX_NAME?h=pri.store.size&bytes=b" | tr -d '[:space:]'
)"

echo "KalDB persisted snapshot metadata sizes"
kaldb_snapshot_rows="$(
  for partition in $(zk_children /ASTRA/partitioned_snapshot); do
    [ "$partition" = "LIVE" ] && continue

    for snapshot in $(zk_children "/ASTRA/partitioned_snapshot/$partition"); do
      zk_json "/ASTRA/partitioned_snapshot/$partition/$snapshot" \
        | jq -c --arg partition "$partition" '{
            partition: $partition,
            snapshotId: (.snapshotId // .name),
            sizeInBytes: (.sizeInBytes | tonumber?),
            maxOffset: (.maxOffset | tonumber?)
          }'
    done
  done
)"

if [ -n "$kaldb_snapshot_rows" ]; then
  printf '%s\n' "$kaldb_snapshot_rows"
else
  echo "No non-live KalDB persisted snapshot metadata found."
fi
echo

kaldb_bytes="$(printf '%s\n' "$kaldb_snapshot_rows" | jq -r '.sizeInBytes // 0' | awk '{sum += $1} END {print sum + 0}')"

awk -v os="$os_primary_bytes" -v kaldb="$kaldb_bytes" 'BEGIN {
  printf "OpenSearch primary store bytes: %d\n", os
  printf "KalDB persisted snapshot bytes: %d\n", kaldb
  if (os > 0) {
    printf "KalDB / OpenSearch ratio: %.3fx\n", kaldb / os
  }
}'

if [ "$physical" != true ]; then
  exit 0
fi

echo
echo "Physical docker mount paths"
docker inspect dep_opensearch astra_single dep_s3 dep_kafka \
  --format '{{.Name}} {{range .Mounts}}{{.Source}} {{end}}'
echo

echo "Physical disk usage by mounted path"
docker inspect dep_opensearch astra_single dep_s3 dep_kafka \
  --format '{{range .Mounts}}{{.Source}}{{"\n"}}{{end}}' \
  | sed '/^$/d' \
  | sort -u \
  | while IFS= read -r path; do
      sudo du -sh "$path"
    done
