#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Ingest NYC taxi trip data into Astra via the bulk API.
#
# Usage:
#   ./scripts/ingest-nyc-taxis.sh [OPTIONS]
#
# Options:
#   --batch-size N   Docs per bulk request (default: 7500)
#   --index NAME     Target index name (default: nyc_taxis)
#   --dry-run        Print first 20 lines of transformed output, don't send
#   --help           Show this help message
#
# The script reads data/nyc_taxis.ndjson.gz (375,000 NYC taxi trips from
# Jan 1-9 2015, ~21 MB compressed), rewrites the time fields into a recent
# one-day window, and sends the result to the preprocessor bulk API in batches.
#
# Prerequisites:
#   - Astra stack running (preprocessor on port 8086, manager on 8083)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_FILE="${DATA_FILE:-$REPO_DIR/data/nyc_taxis.ndjson.gz}"

BATCH_SIZE="${BATCH_SIZE:-7500}"
INDEX="${INDEX:-nyc_taxis}"
BULK_URL="${BULK_URL:-http://localhost:8086/_bulk}"
MANAGER_URL="${MANAGER_URL:-http://localhost:8083}"
QUERY_URL="${QUERY_URL:-http://localhost:8081}"
EXPECTED_VISIBLE_DOCS="${EXPECTED_VISIBLE_DOCS:-}"
QUERY_READY_ATTEMPTS="${QUERY_READY_ATTEMPTS:-90}"
DEST_END_BUFFER_MINS="${DEST_END_BUFFER_MINS:-30}"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --batch-size)   BATCH_SIZE="$2"; shift 2 ;;
    --batch-size=*) BATCH_SIZE="${1#*=}"; shift ;;
    --index)        INDEX="$2"; shift 2 ;;
    --index=*)      INDEX="${1#*=}"; shift ;;
    --dry-run)      DRY_RUN=true; shift ;;
    --help)
      sed -n '3,/^# -----/{ /^# -----/d; s/^# \{0,1\}//; p }' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -f "$DATA_FILE" ]]; then
  echo "ERROR: data file not found: $DATA_FILE" >&2
  exit 1
fi

json_lines() {
  gunzip -c "$DATA_FILE" | sed -n '/^[[:space:]]*{/p'
}

dataset_stream_summary() {
  python3 - "$DATA_FILE" <<'PY'
import gzip
import json
import sys

path = sys.argv[1]
json_lines = 0
total_docs = 0
first_ts = None
last_ts = None

with gzip.open(path, "rt", encoding="utf-8") as handle:
    for raw_line in handle:
        if not raw_line.lstrip().startswith("{"):
            continue
        json_lines += 1
        if json_lines % 2 == 0:
            doc = json.loads(raw_line)
            timestamp = doc["@timestamp"]
            if first_ts is None:
                first_ts = timestamp
            last_ts = timestamp
            total_docs += 1

print(f"{json_lines}|{total_docs}|{first_ts}|{last_ts}")
PY
}

build_recent_timeline() {
  DEST_END_BUFFER_MINS="$DEST_END_BUFFER_MINS" python3 - <<'PY'
import os
from datetime import datetime, timedelta, timezone

now = datetime.now(timezone.utc)
start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
if start_of_day >= now:
    start_of_day = now - timedelta(minutes=1)
buffer_mins = int(os.environ["DEST_END_BUFFER_MINS"])
end_of_day = start_of_day + timedelta(days=1) - timedelta(milliseconds=1)
window_end = min(now + timedelta(minutes=buffer_mins), end_of_day)

def isoformat_utc(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")

print(int(start_of_day.timestamp() * 1000))
print(int(window_end.timestamp() * 1000))
print(isoformat_utc(start_of_day))
print(isoformat_utc(window_end))
PY
}

stream_ndjson() {
  SOURCE_START_ISO="$SOURCE_START_ISO" \
  SOURCE_END_ISO="$SOURCE_END_ISO" \
  DEST_START_MS="$DEST_START_MS" \
  DEST_END_MS="$DEST_END_MS" \
  INDEX="$INDEX" \
  python3 - "$DATA_FILE" <<'PY'
import gzip
import json
import os
import signal
import sys
from datetime import datetime, timezone

signal.signal(signal.SIGPIPE, signal.SIG_DFL)


def parse_iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_naive(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def format_iso_utc(epoch_ms: int) -> str:
    value = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def format_naive_utc(epoch_ms: int) -> str:
    value = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M:%S")


source_start_ms = int(parse_iso_utc(os.environ["SOURCE_START_ISO"]).timestamp() * 1000)
source_end_ms = int(parse_iso_utc(os.environ["SOURCE_END_ISO"]).timestamp() * 1000)
dest_start_ms = int(os.environ["DEST_START_MS"])
dest_end_ms = int(os.environ["DEST_END_MS"])
index_name = os.environ["INDEX"]
source_span_ms = max(source_end_ms - source_start_ms, 1)
dest_span_ms = max(dest_end_ms - dest_start_ms, 1)

with gzip.open(sys.argv[1], "rt", encoding="utf-8") as handle:
    for raw_line in handle:
        if not raw_line.lstrip().startswith("{"):
            continue
        payload = json.loads(raw_line)
        if "index" in payload:
            print(json.dumps({"index": {"_index": index_name}}, separators=(",", ":")))
            continue

        doc = payload
        original_dropoff_ms = int(parse_iso_utc(doc["@timestamp"]).timestamp() * 1000)
        ratio = (original_dropoff_ms - source_start_ms) / source_span_ms
        ratio = max(0.0, min(1.0, ratio))
        rewritten_dropoff_ms = dest_start_ms + int(round(ratio * dest_span_ms))

        original_pickup = parse_naive(doc.get("pickup_datetime"))
        original_dropoff = parse_naive(doc.get("dropoff_datetime"))
        if original_pickup is not None and original_dropoff is not None:
            duration_ms = max(int((original_dropoff - original_pickup).total_seconds() * 1000), 0)
        else:
            duration_ms = 0
        rewritten_pickup_ms = max(dest_start_ms, rewritten_dropoff_ms - duration_ms)

        doc["@timestamp"] = format_iso_utc(rewritten_dropoff_ms)
        if "dropoff_datetime" in doc:
            doc["dropoff_datetime"] = format_naive_utc(rewritten_dropoff_ms)
        if "pickup_datetime" in doc:
            doc["pickup_datetime"] = format_naive_utc(rewritten_pickup_ms)

        print(json.dumps(doc, separators=(",", ":")))
PY
}

manager_post_json() {
  local endpoint="$1"
  local payload="$2"
  local attempts="${3:-20}"
  local response=""

  for _ in $(seq 1 "$attempts"); do
    response="$(
      curl -sS -XPOST \
        -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
        "$MANAGER_URL/$endpoint" \
        -d "$payload" 2>/dev/null || true
    )"
    if printf '%s' "$response" | python3 -c 'import json,sys; json.load(sys.stdin)' >/dev/null 2>&1; then
      printf '%s' "$response"
      return 0
    fi
    sleep 1
  done

  echo "ERROR: manager endpoint '$endpoint' did not return valid JSON after $attempts attempt(s)." >&2
  return 1
}

query_match_all_state() {
  curl -sS -H 'Content-Type: application/json' -X POST "$QUERY_URL/$INDEX/_search" --data-binary @- <<'EOF' \
    | python3 -c 'import json,sys; payload=json.load(sys.stdin); buckets=payload.get("aggregations",{}).get("per_day",{}).get("buckets",[]); total=sum(bucket.get("doc_count",0) for bucket in buckets); print(f"{payload.get("_shards",{}).get("total",0)}|{total}")'
{"size":0,"aggs":{"per_day":{"date_histogram":{"field":"@timestamp","calendar_interval":"1d"}}},"query":{"match_all":{}}}
EOF
}

query_time_filtered_state() {
  jq -nc --arg gte "$QUERY_TIME_FROM" --arg lte "$QUERY_TIME_TO" '{
    size: 0,
    aggs: {
      per_hour: {
        date_histogram: {
          field: "@timestamp",
          calendar_interval: "1h"
        }
      }
    },
    query: {
      bool: {
        filter: [
          {
            range: {
              "@timestamp": {
                gte: $gte,
                lte: $lte,
                format: "strict_date_optional_time"
              }
            }
          }
        ]
      }
    }
  }' \
    | curl -sS -H 'Content-Type: application/json' -X POST "$QUERY_URL/$INDEX/_search" --data-binary @- \
    | python3 -c 'import json,sys; payload=json.load(sys.stdin); buckets=payload.get("aggregations",{}).get("per_hour",{}).get("buckets",[]); total=sum(bucket.get("doc_count",0) for bucket in buckets); print(f"{payload.get("_shards",{}).get("total",0)}|{total}")'
}

get_dataset_state() {
  local response
  response="$(manager_post_json "slack.proto.astra.ManagerApiService/ListDatasetMetadata" '{}')"
  python3 -c '
import json
import sys

target_index = sys.argv[1]
payload = json.load(sys.stdin)

for dataset in payload.get("datasetMetadata", []):
    if dataset.get("name") != target_index:
        continue
    for partition_config in dataset.get("partitionConfigs", []):
        if partition_config.get("endTimeEpochMs") == "9223372036854775807":
            partitions = ",".join(partition_config.get("partitions", []))
            start_time = partition_config.get("startTimeEpochMs", "")
            print(f"{start_time}|{partitions}")
            raise SystemExit(0)
    print("|")
    raise SystemExit(0)
' "$INDEX" <<<"$response"
}

count_matches_demo_shape() {
  local count="$1"
  (( count == EXPECTED_VISIBLE_DOCS ))
}

wait_for_time_filtered_query() {
  local attempts="${1:-30}"
  local state shards hits

  for _ in $(seq 1 "$attempts"); do
    state="$(query_time_filtered_state 2>/dev/null || echo '0|0')"
    shards="${state%%|*}"
    hits="${state#*|}"
    if (( shards > 0 )) && count_matches_demo_shape "$hits"; then
      echo "$state"
      return 0
    fi
    sleep 1
  done

  echo "ERROR: time-filtered Astra query never reached the expected demo document count" >&2
  return 1
}

# ---- Inspect the input file ----
raw_total_lines=$(gunzip -c "$DATA_FILE" | wc -l)
IFS='|' read -r total_lines total_docs SOURCE_START_ISO SOURCE_END_ISO <<<"$(dataset_stream_summary)"
readarray -t timeline_parts < <(build_recent_timeline)
DEST_START_MS="${timeline_parts[0]}"
DEST_END_MS="${timeline_parts[1]}"
QUERY_TIME_FROM="${timeline_parts[2]}"
QUERY_TIME_TO="${timeline_parts[3]}"
batch_lines=$(( BATCH_SIZE * 2 ))
total_batches=$(( (total_lines + batch_lines - 1) / batch_lines ))
if [[ -z "$EXPECTED_VISIBLE_DOCS" ]]; then
  EXPECTED_VISIBLE_DOCS=$total_docs
fi

if [[ "$DRY_RUN" == "true" ]]; then
  echo "--- dry run: first 20 transformed lines ---"
  stream_ndjson | head -20
  exit 0
fi

# ---- Ensure the dataset exists ----
echo "Ensuring dataset '$INDEX' exists ..."
dataset_preexisting=false
dataset_state="$(get_dataset_state)"
if [[ -z "$dataset_state" ]]; then
  manager_post_json "slack.proto.astra.ManagerApiService/CreateDatasetMetadata" "{
      \"name\": \"$INDEX\",
      \"owner\": \"demo\",
      \"serviceNamePattern\": \"$INDEX\"
    }" >/dev/null

  manager_post_json "slack.proto.astra.ManagerApiService/UpdatePartitionAssignment" "{
      \"name\": \"$INDEX\",
      \"throughputBytes\": \"4000000\",
      \"partitionIds\": [\"0\"]
    }" >/dev/null
  echo "  created new dataset metadata."
  echo "  waiting briefly for preprocessor rate limits to pick up the new dataset ..."
  sleep 3
else
  dataset_preexisting=true
  dataset_partitions="${dataset_state#*|}"
  if [[ "$dataset_partitions" != "0" ]]; then
    echo "ERROR: dataset '$INDEX' already exists with incompatible active partition metadata:" >&2
    echo "  partitions=$dataset_partitions" >&2
    echo "Use a fresh INDEX value or clean up the existing dataset metadata before re-running." >&2
    exit 1
  fi
  echo "  dataset already exists with partition 0; reusing it."
fi
echo "  done."

# ---- Refuse duplicate or ambiguous existing data ----
if [[ "$dataset_preexisting" == "true" ]]; then
  match_all_state="$(query_match_all_state 2>/dev/null || echo '0|0')"
  time_state="$(query_time_filtered_state 2>/dev/null || echo '0|0')"
  match_all_shards="${match_all_state%%|*}"
  match_all_hits="${match_all_state#*|}"
  time_shards="${time_state%%|*}"
  time_hits="${time_state#*|}"

  if count_matches_demo_shape "$match_all_hits" && count_matches_demo_shape "$time_hits"; then
    echo "Dataset already contains the expected demo docs; skipping ingest."
    exit 0
  fi

  if (( match_all_hits > 0 )) || (( time_hits > 0 )) || (( match_all_shards > 0 )) || (( time_shards > 0 )); then
    echo "ERROR: dataset '$INDEX' already contains existing search-visible data." >&2
    echo "  untimed_hits=$match_all_hits time_hits=$time_hits expected_hits=$EXPECTED_VISIBLE_DOCS" >&2
    echo "Clean up the existing dataset state or use a fresh INDEX value before re-running." >&2
    exit 1
  fi
fi

echo ""
echo "Data file:  $DATA_FILE"
echo "Index:      $INDEX"
echo "Docs:       $total_docs"
echo "Queryable:  $EXPECTED_VISIBLE_DOCS docs"
echo "Batch size: $BATCH_SIZE docs"
echo "Batches:    $total_batches"
echo "Time map:   $SOURCE_START_ISO .. $SOURCE_END_ISO  ->  $QUERY_TIME_FROM .. $QUERY_TIME_TO"
if (( raw_total_lines != total_lines )); then
  echo "Skipping:   $(( raw_total_lines - total_lines )) non-NDJSON line(s)"
fi
echo ""
echo "Ingesting ..."

start_time=$SECONDS
batch=0
ingested=0
failed=0

exec 3< <(stream_ndjson)
while true; do
  # Read one batch worth of lines
  batch_data=""
  lines_read=0
  while (( lines_read < batch_lines )); do
    if ! IFS= read -r line <&3; then
      break
    fi
    batch_data+="$line"$'\n'
    (( lines_read++ )) || true
  done

  if (( lines_read == 0 )); then
    break
  fi

  (( batch++ )) || true
  batch_docs=$(( lines_read / 2 ))

  response="$(printf '%s' "$batch_data" \
    | curl -sS -H 'Content-Type: application/x-ndjson' "$BULK_URL" --data-binary @- 2>&1)"

  batch_ok="$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('totalDocs',0))" 2>/dev/null || echo 0)"
  batch_fail="$(echo "$response" | python3 -c "import sys,json; print(json.load(sys.stdin).get('failedDocs',0))" 2>/dev/null || echo 0)"

  ingested=$(( ingested + batch_ok ))
  failed=$(( failed + batch_fail ))

  printf "\r  batch %d/%d  ingested=%d  failed=%d" "$batch" "$total_batches" "$ingested" "$failed"

  if (( lines_read < batch_lines )); then
    break
  fi
done
exec 3<&-

elapsed=$(( SECONDS - start_time ))
echo ""
echo ""
echo "Done in ${elapsed}s — $ingested docs ingested, $failed failed."

if (( ingested > 0 )); then
  echo "Waiting for the recent-window demo data to become queryable ..."
  ready_state="$(wait_for_time_filtered_query "$QUERY_READY_ATTEMPTS")"
  ready_shards="${ready_state%%|*}"
  ready_hits="${ready_state#*|}"
  if (( ready_shards <= 0 )) || ! count_matches_demo_shape "$ready_hits"; then
    echo "ERROR: dataset became queryable with an unexpected document count." >&2
    echo "  shards=$ready_shards hits=$ready_hits expected=$EXPECTED_VISIBLE_DOCS" >&2
    exit 1
  fi
  echo "  ready with $ready_hits docs."
fi
