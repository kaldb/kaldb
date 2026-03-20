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
# Jan 1-8 2015, ~21 MB compressed) and sends it to the preprocessor bulk
# API in batches.
#
# Prerequisites:
#   - Astra stack running (preprocessor on port 8086, manager on 8083)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_FILE="$REPO_DIR/data/nyc_taxis.ndjson.gz"

BATCH_SIZE="${BATCH_SIZE:-7500}"
INDEX="${INDEX:-nyc_taxis}"
BULK_URL="${BULK_URL:-http://localhost:8086/_bulk}"
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

if [[ "$DRY_RUN" == "true" ]]; then
  echo "--- dry run: first 20 lines ---"
  gunzip -c "$DATA_FILE" | head -20
  exit 0
fi

# ---- Ensure the dataset exists ----
echo "Ensuring dataset '$INDEX' exists ..."
curl -sS -XPOST \
  -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
  'http://localhost:8083/slack.proto.astra.ManagerApiService/CreateDatasetMetadata' \
  -d "{
    \"name\": \"$INDEX\",
    \"owner\": \"demo\",
    \"serviceNamePattern\": \"$INDEX\"
  }" 2>/dev/null || true

curl -sS -XPOST \
  -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
  'http://localhost:8083/slack.proto.astra.ManagerApiService/UpdatePartitionAssignment' \
  -d "{
    \"name\": \"$INDEX\",
    \"throughputBytes\": \"4000000\",
    \"partitionIds\": [\"0\"]
  }" 2>/dev/null || true
echo "  done."

# ---- Ingest in batches ----
total_lines=$(gunzip -c "$DATA_FILE" | wc -l)
total_docs=$(( total_lines / 2 ))
batch_lines=$(( BATCH_SIZE * 2 ))
total_batches=$(( (total_lines + batch_lines - 1) / batch_lines ))

echo ""
echo "Data file:  $DATA_FILE"
echo "Index:      $INDEX"
echo "Docs:       $total_docs"
echo "Batch size: $BATCH_SIZE docs"
echo "Batches:    $total_batches"
echo ""
echo "Ingesting ..."

start_time=$SECONDS
batch=0
ingested=0
failed=0

gunzip -c "$DATA_FILE" | while true; do
  # Read one batch worth of lines
  batch_data=""
  lines_read=0
  while (( lines_read < batch_lines )); do
    if ! IFS= read -r line; then
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

elapsed=$(( SECONDS - start_time ))
echo ""
echo ""
echo "Done in ${elapsed}s — $ingested docs ingested, $failed failed."
