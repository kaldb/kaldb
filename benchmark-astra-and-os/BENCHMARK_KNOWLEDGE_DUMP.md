# Astra vs OpenSearch Benchmark Suite — Complete Technical Context

## Purpose
This is a benchmark suite that compares the query performance of **Astra** (a log search/analytics system built on Lucene, developed at Slack/Airbnb) against **OpenSearch** (the AWS fork of Elasticsearch). The goal is to validate that Astra returns correct results and measure how its performance compares to OpenSearch for various query types.

## Repository Context
- **Repo**: `airbnb/kaldb` (Astra was formerly called KalDB)
- **Branch**: `nhoward/local_bulk_ingest`
- **Location**: `benchmark-astra-and-os/` directory within the repo
- **Status**: Not yet merged to master; was in a broken/hard-to-run state, now being fixed

## Architecture Overview

### Astra's Architecture (multi-service)
Astra runs as multiple cooperating services, each with a distinct role:

| Service | Port | Role |
|---------|------|------|
| `astra_preprocessor` | 8086 | Receives data via `_bulk` API, writes to Kafka |
| `astra_index` | 8080 | Consumes from Kafka, indexes into Lucene, serves `_local_bulk` (direct ingest bypassing Kafka) and `_msearch` |
| `astra_query` | 8081 | Distributed query service, also serves `_msearch` |
| `astra_manager` | 8083 | Cluster management, snapshot lifecycle |
| `astra_cache` | 8082 | Caches snapshots loaded from S3 |
| `astra_recovery` | 8085 | Recovers data from Kafka into snapshots |

Supporting infrastructure:
- **ZooKeeper**: Metadata store coordination
- **Kafka**: Message queue between preprocessor and indexer
- **S3 Mock**: Simulates S3 for snapshot storage (local dev)

### Data Flow
```
[Client] → _bulk → [Preprocessor] → Kafka → [Indexer] → Lucene → S3 snapshot → [Cache] → [Query Node]
                                              ↑
[Client] → _local_bulk → [Indexer] (direct, bypasses Kafka — this is what this branch adds)
```

### OpenSearch
Standard single-node OpenSearch instance. In the benchmark's original setup, it ran with TLS and basic auth. In the tested deployment, it ran with security disabled (plain HTTP).

## The Benchmark Tool (`benchmark.rb`)

### What It Does
A Ruby script that:
1. Sends the same queries to both Astra and OpenSearch via their `_msearch` (multi-search) endpoints
2. Measures response time for each
3. Compares the results for correctness
4. Outputs a color-coded performance ratio table

### Query Types Tested (19 total)
The queries use the **NYC Taxi dataset** (real-world taxi trip records from 2015) with fields like `total_amount`, `tip_amount`, `trip_distance`, `dropoff_datetime`, `pickup_datetime`.

**Categories:**
1. **Simple queries**: `match_all`, `range` (on `total_amount`)
2. **Aggregations with date histograms**:
   - `date_histogram` with calendar intervals (day, month)
   - `date_histogram` with fixed intervals (60 days)
   - `auto_date_histogram` with target bucket counts (5, 100, 1000)
   - All of the above with timezone variants (`America/New_York`)
   - With nested `stats` sub-aggregations on `total_amount`, `tip_amount`, `trip_distance`
3. **Other aggregations**: `histogram` on `trip_distance` with nested stats
4. **Sorting**: `match_all` sorted by `tip_amount` ascending/descending

### How Queries Are Sent
Each query is tested with 15 different `size` parameters (number of hit documents to return):
```
sizes = [0, 50, 100, 250, 500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
```

The request format is Elasticsearch's `_msearch` NDJSON format:
```
{"index":"test"}
{"query": <query>, "size": <count>}
```

Queries are sent via `curl` subprocess calls.

### Result Comparison Logic
The comparison is format-aware because Astra and OpenSearch return slightly different response structures:

**Known differences:**
| Aspect | Astra | OpenSearch |
|--------|-------|-----------|
| `hits.total.value` | Count of **returned** hits (capped by `size`) | Count of **all matching** documents |
| `_score` | String or null | Number |
| `_timesinceepoch` | Custom extra field (Instant) | Not present |
| `sort` in hits | Always `[timestamp_millis]` | Varies by query |
| `_debug` field | Present at top-level and per-response | Not present |
| `_shards` | Only `total` and `failed` | Also `successful` and `skipped` |
| Default hit ordering | By timestamp | By `_score` or `_doc` |

**Comparison strategy:**
- **Aggregation queries**: Compare bucket counts and `doc_count` per bucket
- **Sorted queries**: Compare `_source.total_amount` values (same ordering guarantees same docs)
- **Unsorted queries**: Only compare hit counts, NOT individual documents (different default ordering means different docs returned)
- **`hits.total.value`**: Not compared across systems (means different things)

### Output
1. **Console**: Per-query timing with mismatch annotations, then a color-coded summary table
   - Green background: Astra faster
   - Red background: OpenSearch faster
   - Number shows the ratio (e.g., 2.5 means Astra took 2.5x longer)
2. **CSV file**: `results/benchmark.<timestamp>.csv` with all timing data
3. **JSON files**: `output/<timestamp>/<query>-<size>-<system>.json` — raw responses for debugging

## Setup Scripts

### `setup_data.sh`
Downloads the NYC Taxi dataset (~4.8GB compressed) from CloudFront, decompresses it, splits into files of 7500 lines each, and prepends Elasticsearch bulk index headers.

Output: `data/ready/nyc_taxis_NNNN.ndjson` files

### `setup_clusters.sh`
Starts OpenSearch (standalone Docker container) and Astra (via docker-compose), then waits for both to be healthy (polls with curl, up to 60s).

### `setup_indices.sh`
Loads data files into one or both systems. Supports:
- `./setup_indices.sh [count] [target]` — count=number of files, target=`os`/`astra`/`both`
- Environment variables: `OS_SCHEME`, `OS_PORT`, `ASTRA_BULK_PORT`, `ASTRA_BULK_PATH`

This decoupling means if loading fails for one system, you can retry just that one.

### `teardown.sh`
Stops OpenSearch container and runs `docker compose down` for Astra.

## Bugs Found During Benchmarking

### In Astra:
1. **`auto_date_histogram` ignores bucket count**: Requesting 5 buckets returns 54; requesting 1000 returns 2247. OpenSearch respects the requested count.
2. **`size: 0` with no aggregations returns 500**: Astra validates "Hits or aggregation should be requested" but `size: 0` is valid in ES (returns just metadata).
3. **`hits.total.value` is wrong**: Reports `responseHits.size()` (returned count) instead of total matching documents. Code location: `ElasticsearchApiService.java:187`.
4. **Sorted queries return different results**: Even with explicit sort, tie-breaking differs from OpenSearch.

### In the Benchmark (now fixed):
1. **Zero iterations by default**: `ARGV.first.to_i || 1` — `nil.to_i` is `0`, which is truthy in Ruby
2. **Silent comparison failures**: `rescue [[100],[100]]` hid all parse errors by making both sides equal
3. **Coupled data loading**: Single loop loaded both systems; failure in one meant redo everything
4. **No health checks**: Scripts started containers but didn't wait for readiness
5. **No teardown**: No way to clean up
6. **zsh shebangs**: Not portable

## Configuration

### Docker Compose Environment Variables (for Astra)
Key settings for benchmarking:
- `INDEXER_MAX_TIME_PER_CHUNK_SECONDS`: How often Lucene chunks roll over (default 120s). After rollover, data gets snapshotted to S3. If S3 fails, ingestion blocks. Set to 3600 for benchmarking.
- `INDEXER_MAX_MESSAGES_PER_CHUNK`: Max messages before forced rollover
- `ASTRA_INDEX_REQUEST_TIMEOUT_MS`: HTTP request timeout (70 min for benchmark)
- `ASTRA_INDEX_DEFAULT_QUERY_TIMEOUT_MS`: Query timeout (60 min for benchmark)

### Benchmark Environment Variables
- `ASTRA_QUERY_PORT`: Astra port for `_msearch` (default: 8080)
- `OS_PORT`: OpenSearch port (default: 9200)
- `OS_SCHEME`: `http` or `https` (default: `https`)
- `ASTRA_BULK_PORT`: Port for data loading (default: 8080)
- `ASTRA_BULK_PATH`: Endpoint path for data loading (default: `/_local_bulk`)

## Performance Results (75k documents, single iteration)
- Astra is generally **1.5–8x slower** than OpenSearch, scaling worse with larger `size` values
- Small queries (size 0–100) are close to parity for simple queries
- Aggregation-heavy queries show 2–6x slower performance
- The performance gap widens with result size, suggesting serialization/transport overhead

## File Listing
```
benchmark-astra-and-os/
├── benchmark.rb          # Main benchmark script (Ruby)
├── common.sh             # Shared config (OS password)
├── setup_data.sh         # Download and prepare NYC taxi data
├── setup_clusters.sh     # Start Docker containers with health checks
├── setup_indices.sh      # Load data into OS and/or Astra
├── teardown.sh           # Stop all containers
├── run_benchmark.sh      # (Dead code — entirely commented out, older approach)
├── query_astra.sh        # Helper: send msearch to Astra
├── query_os.sh           # Helper: send msearch to OpenSearch
├── snippets.sh           # Scratch file (not part of workflow)
├── README.md             # Setup instructions
├── .gitignore            # Ignores data/, output/, results/
└── queries/              # Query files (used by commented-out run_benchmark.sh, not by benchmark.rb)
    ├── q1-match_all
    ├── q2-range
    ├── ... (15 total)
```

## How to Run (End-to-End)
```bash
cd benchmark-astra-and-os

# 1. Prepare data
./setup_data.sh

# 2. Start clusters (builds Astra Docker image + starts OS)
./setup_clusters.sh

# 3. Load data (e.g., 10 files = 75k docs)
./setup_indices.sh 10

# 4. Run benchmark (1 iteration)
./benchmark.rb 1
# Or with custom config:
OS_SCHEME=http ASTRA_QUERY_PORT=8080 ./benchmark.rb 3

# 5. Clean up
./teardown.sh
```
