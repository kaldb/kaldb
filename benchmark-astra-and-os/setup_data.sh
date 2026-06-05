#!/usr/bin/env bash
set -eu

# Generate files to load into the bulk ingest APIs.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
source common.sh

setup_clickbench_hits() {
  local data_url="${DATA_URL:-https://datasets.clickhouse.com/hits_compatible/hits.json.gz}"
  local split_size="${SPLIT_SIZE:-1000}"
  local splits_count="${SPLITS_COUNT:-1000}"
  local max_records="${MAX_RECORDS:-$((split_size * splits_count))}"
  local ready_dir="$DATA_DIR"
  local expected_files="$splits_count"

  mkdir -p "$ready_dir"
  local ready_count
  ready_count=$(find "$ready_dir" -type f -name '*.ndjson' | wc -l | tr -d ' ')
  if [ "$max_records" != "all" ] && [ "$ready_count" -ge "$expected_files" ]; then
    echo "$ready_dir already has $ready_count files; leaving existing data in place."
    return 0
  fi

  if [ "$ready_count" -gt 0 ]; then
    echo "$ready_dir has existing files; regenerating ClickBench data."
    rm -f "$ready_dir"/*.ndjson
  fi

  local tmp_dir
  tmp_dir="$(dirname "$ready_dir")/tmp"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"

  local input_cmd
  if [ -n "${SOURCE_FILE:-}" ]; then
    input_cmd=(gzip -dc "$SOURCE_FILE")
  else
    input_cmd=(
      bash -c
      'curl -fsSL "$1" 2> >(grep -v "Failure writing output to destination" >&2) | gzip -dc'
      _
      "$data_url"
    )
  fi

  echo "Preparing ClickBench hits data into $ready_dir"
  if [ "$max_records" = "all" ]; then
    "${input_cmd[@]}" \
      | jq -cr --arg index "$INDEX_NAME" '
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
        ' \
      | split -l "$((split_size * 2))" -d -a 5 - "$tmp_dir/clickbench_hits_"
  else
    "${input_cmd[@]}" \
      | head -n "$max_records" \
      | jq -cr --arg index "$INDEX_NAME" '
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
        ' \
      | split -l "$((split_size * 2))" -d -a 5 - "$tmp_dir/clickbench_hits_"
  fi

  for f in "$tmp_dir"/*; do
    [ -f "$f" ] || continue
    mv "$f" "$ready_dir/$(basename "$f").ndjson"
  done
  rm -rf "$tmp_dir"
}

setup_nyc_taxis() {
  local data_url="${DATA_URL:-https://dbyiw3u3rf9yr.cloudfront.net/corpora/nyc_taxis/documents.json.bz2}"
  local split_size="${SPLIT_SIZE:-7500}"
  local splits_count="${SPLITS_COUNT:-1280}"
  local ready_dir="$DATA_DIR"
  local normalize_dates_to_iso="${NORMALIZE_DATES_TO_ISO:-true}"

  mkdir -p data
  pushd data || exit

  if [ ! -f documents.json.bz2 ]; then
    curl -L "$data_url" -o documents.json.bz2
  fi

  mkdir -p "${ready_dir#data/}"
  ready_dir="${ready_dir#data/}"
  local ready_count
  ready_count=$(find "$ready_dir" -type f -name '*.ndjson' | wc -l | tr -d ' ')
  if [ "$ready_count" -ge "$splits_count" ]; then
    echo "data/$ready_dir already has $ready_count files; leaving existing data in place."
    popd
    return 0
  fi

  if [ "$ready_count" -gt 0 ]; then
    echo "data/$ready_dir has only $ready_count files; regenerating $splits_count files."
    rm -f "$ready_dir"/*.ndjson
  fi

  rm -rf tmp
  mkdir -p tmp

  local split_lines=$((split_size * splits_count))
  bzcat documents.json.bz2 | head -n "$split_lines" | split -l "$split_size" -d -a 4 - tmp/nyc_taxis_

  for f in tmp/*; do
    if [ "$normalize_dates_to_iso" = "true" ]; then
      ruby -rjson -rtime -e '
        ARGF.each_line do |line|
          doc = JSON.parse(line)
          %w[pickup_datetime dropoff_datetime].each do |field|
            doc[field] = Time.strptime(doc.fetch(field), "%Y-%m-%d %H:%M:%S").utc.iso8601
          end
          puts JSON.generate(doc)
        end
      ' "$f" | sed -e "s/^/{ \"index\" : {\"_index\" : \"$INDEX_NAME\"} }\n/" > "$f".ndjson
    else
      sed -e "s/^/{ \"index\" : {\"_index\" : \"$INDEX_NAME\"} }\n/" "$f" > "$f".ndjson
    fi
  done

  mv tmp/*.ndjson "$ready_dir"/
  rm -r tmp

  popd
}

case "$DATASET" in
  clickbench_hits) setup_clickbench_hits ;;
  nyc_taxis) setup_nyc_taxis ;;
esac
