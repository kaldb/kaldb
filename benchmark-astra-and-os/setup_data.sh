#!/usr/bin/env bash
set -Eeuo pipefail

# Generate files to load into the bulk ingest APIs.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source common.sh

clickbench_curl() {
  local data_url="$1"
  local retries="${DATA_DOWNLOAD_RETRIES:-8}"
  local retry_delay_seconds="${DATA_DOWNLOAD_RETRY_DELAY_SECONDS:-10}"
  local connect_timeout_seconds="${DATA_DOWNLOAD_CONNECT_TIMEOUT_SECONDS:-30}"

  curl \
    --http1.1 \
    --fail \
    --show-error \
    --location \
    --retry "$retries" \
    --retry-delay "$retry_delay_seconds" \
    --retry-all-errors \
    --connect-timeout "$connect_timeout_seconds" \
    "$data_url" \
    2> >(grep -v "Failure writing output to destination" >&2)
}

stream_clickbench_hits() {
  local data_url="$1"
  if [ -n "${SOURCE_FILE:-}" ]; then
    gzip -dc "$SOURCE_FILE"
  else
    clickbench_curl "$data_url" | gzip -dc
  fi
}

normalize_clickbench_hits() {
  jq -cr --arg index "$INDEX_NAME" '
    def normalize_date:
      if . == null or . == "" then .
      elif test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$") then . + "T00:00:00Z"
      elif test("^[0-9]{4}-[0-9]{2}-[0-9]{2} ") then sub(" "; "T") + "Z"
      else . end;
    def to_bool: (. == true or . == 1 or . == "1" or . == "true");
    .EventDate |= normalize_date
    | .EventTime |= normalize_date
    | .ClientEventTime |= normalize_date
    | .LocalEventTime |= normalize_date
    | .["@timestamp"] = .EventTime
    | .ConstantOne = 1
    | .IsRefresh = (.IsRefresh | to_bool)
    | .DontCountHits = (.DontCountHits | to_bool)
    | ({"index": {"_index": $index}}), .
  '
}

verify_clickbench_ready_files() {
  local ready_dir="$1"
  local split_size="$2"
  local max_records="$3"

  if [ "$max_records" = "all" ]; then
    local ready_count
    ready_count=$(find "$ready_dir" -type f -name '*.ndjson' | wc -l | tr -d ' ')
    if [ "$ready_count" -eq 0 ]; then
      echo "ClickBench data generation produced no files in $ready_dir" >&2
      return 1
    fi
    return 0
  fi

  local expected_files
  expected_files=$(((max_records + split_size - 1) / split_size))
  local ready_count
  ready_count=$(find "$ready_dir" -type f -name '*.ndjson' | wc -l | tr -d ' ')
  if [ "$ready_count" -ne "$expected_files" ]; then
    echo "ClickBench data generation produced $ready_count files, expected $expected_files for $max_records records." >&2
    return 1
  fi

  local last_file
  last_file=$(find "$ready_dir" -type f -name '*.ndjson' | sort | tail -n 1)
  local remainder="$((max_records % split_size))"
  local expected_last_lines
  if [ "$remainder" -eq 0 ]; then
    expected_last_lines=$((split_size * 2))
  else
    expected_last_lines=$((remainder * 2))
  fi

  local actual_last_lines
  actual_last_lines=$(wc -l < "$last_file" | tr -d ' ')
  if [ "$actual_last_lines" -ne "$expected_last_lines" ]; then
    echo "Last ClickBench split has $actual_last_lines lines, expected $expected_last_lines: $last_file" >&2
    return 1
  fi
}

setup_clickbench_hits() {
  local data_url="${DATA_URL:-https://datasets.clickhouse.com/hits_compatible/hits.json.gz}"
  local split_size="${SPLIT_SIZE:-1000}"
  local splits_count="${SPLITS_COUNT:-1000}"
  local max_records="${MAX_RECORDS:-$((split_size * splits_count))}"
  local ready_dir="$DATA_DIR"
  local expected_files="$splits_count"
  if [ "$max_records" != "all" ]; then
    expected_files=$(((max_records + split_size - 1) / split_size))
  fi

  mkdir -p "$ready_dir"
  local ready_count
  ready_count=$(find "$ready_dir" -type f -name '*.ndjson' | wc -l | tr -d ' ')
  if [ "$max_records" != "all" ] && [ "$ready_count" -eq "$expected_files" ]; then
    if verify_clickbench_ready_files "$ready_dir" "$split_size" "$max_records"; then
      echo "$ready_dir already has $ready_count verified files; leaving existing data in place."
      return 0
    fi
    echo "$ready_dir has $ready_count files but failed validation; regenerating ClickBench data."
  fi

  if [ "$ready_count" -gt 0 ]; then
    echo "$ready_dir has existing files; regenerating ClickBench data."
    rm -f "$ready_dir"/*.ndjson
  fi

  local tmp_dir
  tmp_dir="$(dirname "$ready_dir")/tmp"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"

  echo "Preparing ClickBench hits data into $ready_dir"
  if [ "$max_records" = "all" ]; then
    stream_clickbench_hits "$data_url" \
      | normalize_clickbench_hits \
      | split -l "$((split_size * 2))" -d -a 5 - "$tmp_dir/clickbench_hits_"
  else
    set +o pipefail
    stream_clickbench_hits "$data_url" \
      | head -n "$max_records" \
      | normalize_clickbench_hits \
      | split -l "$((split_size * 2))" -d -a 5 - "$tmp_dir/clickbench_hits_"
    local pipeline_status=("${PIPESTATUS[@]}")
    set -o pipefail
    if [ "${pipeline_status[1]}" -ne 0 ] || [ "${pipeline_status[2]}" -ne 0 ] || [ "${pipeline_status[3]}" -ne 0 ]; then
      echo "ClickBench data generation failed. Pipeline statuses: ${pipeline_status[*]}" >&2
      return 1
    fi
  fi

  for f in "$tmp_dir"/*; do
    [ -f "$f" ] || continue
    mv "$f" "$ready_dir/$(basename "$f").ndjson"
  done
  rm -rf "$tmp_dir"
  verify_clickbench_ready_files "$ready_dir" "$split_size" "$max_records"
}

setup_clickbench_hits
