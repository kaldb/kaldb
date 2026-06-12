#!/usr/bin/env bash
set -euo pipefail

# Compares only Lucene index-directory files, excluding OpenSearch translog/state
# directories and KalDB cache-slot wrapper files.
#
# Run this on the benchmark host after timed query phases, not during a timed
# query. It walks filesystem metadata for all segment files.

ASTRA_CONTAINER="${ASTRA_CONTAINER:-astra_single}"
OS_CONTAINER="${OS_CONTAINER:-dep_opensearch}"
INDEX_NAME="${INDEX_NAME:-hits}"
OS_VOLUME_NAME="${OS_VOLUME_NAME:-benchmark-astra-and-os_opensearch-data1}"
OS_DATA_PATH="${OS_DATA_PATH:-}"

if [[ -z "$OS_DATA_PATH" ]]; then
  if docker inspect "$OS_CONTAINER" >/dev/null 2>&1; then
    OS_DATA_PATH="$(
      docker inspect "$OS_CONTAINER" \
        | jq -r '.[0].Mounts[] | select(.Destination == "/usr/share/opensearch/data") | .Source' \
        | head -n 1
    )"
  fi
fi

if [[ -z "$OS_DATA_PATH" || "$OS_DATA_PATH" == "null" ]]; then
  OS_DATA_PATH="$(docker volume inspect "$OS_VOLUME_NAME" | jq -r '.[0].Mountpoint')"
fi

sum_host_files() {
  sudo find "$@" -type f ! -name write.lock -printf '%s %b\n' 2>/dev/null \
    | awk '
        { files += 1; apparent += $1; allocated += ($2 * 512) }
        END { printf "%d,%d,%d", files, apparent, allocated }
      '
}

sum_container_files() {
  local container="$1"
  docker exec "$container" sh -lc '
    set -eu
    find /tmp/astra-slot-* -maxdepth 1 -type f ! -name write.lock ! -name schema.json -printf "%s %b\n" 2>/dev/null \
      | awk '"'"'
          { files += 1; apparent += $1; allocated += ($2 * 512) }
          END { printf "%d,%d,%d", files, apparent, allocated }
        '"'"'
  '
}

container_lucene_dirs() {
  local container="$1"
  docker exec "$container" sh -lc '
    set -eu
    for d in /tmp/astra-slot-*; do
      [ -d "$d" ] && printf "%s\n" "$d"
    done
  ' 2>/dev/null
}

sum_container_dir() {
  local container="$1"
  local dir="$2"
  docker exec "$container" sh -lc '
    set -eu
    dir="$1"
    find "$dir" -maxdepth 1 -type f ! -name write.lock ! -name schema.json -printf "%s %b\n" 2>/dev/null \
      | awk '"'"'
          { files += 1; apparent += $1; allocated += ($2 * 512) }
          END { printf "%d,%d,%d", files, apparent, allocated }
        '"'"'
  ' sh "$dir"
}

echo "system,scope,path,file_count,apparent_bytes,allocated_bytes"

kaldb_total="$(sum_container_files "$ASTRA_CONTAINER")"
echo "kaldb,lucene_index_dirs,/tmp/astra-slot-*,$kaldb_total"

while IFS= read -r dir; do
  [[ -z "$dir" ]] && continue
  dir_sum="$(sum_container_dir "$ASTRA_CONTAINER" "$dir")"
  echo "kaldb,lucene_index_dir,$dir,$dir_sum"
done < <(container_lucene_dirs "$ASTRA_CONTAINER")

os_indices_root="$OS_DATA_PATH/nodes/0/indices"
if [[ -d "$os_indices_root" ]]; then
  os_index_uuid="$(
    curl -fsS "http://localhost:9200/$INDEX_NAME/_settings" 2>/dev/null \
      | jq -r --arg index "$INDEX_NAME" '.[$index].settings.index.uuid // empty' \
      || true
  )"

  if [[ -n "$os_index_uuid" && -d "$os_indices_root/$os_index_uuid" ]]; then
    mapfile -t os_index_dirs < <(
      sudo find "$os_indices_root/$os_index_uuid" -mindepth 2 -maxdepth 2 -type d -name index -print 2>/dev/null \
        | sort
    )
  else
    mapfile -t os_index_dirs < <(
      sudo find "$os_indices_root" -mindepth 3 -maxdepth 3 -type d -name index -print 2>/dev/null \
        | sort
    )
  fi

  if [[ "${#os_index_dirs[@]}" -gt 0 ]]; then
    os_total="$(sum_host_files "${os_index_dirs[@]}")"
    echo "opensearch,lucene_index_dirs,$os_indices_root/${os_index_uuid:-*}/*/index,$os_total"

    for dir in "${os_index_dirs[@]}"; do
        dir_sum="$(sum_host_files "$dir")"
        echo "opensearch,lucene_index_dir,$dir,$dir_sum"
    done
  fi
fi
