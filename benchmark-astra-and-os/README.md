# KalDB/OpenSearch ClickBench Harness

This directory contains a reproducible benchmark harness for comparing KalDB
and OpenSearch on the ClickBench `hits` dataset.

The harness is intentionally self-contained:

- ClickBench request JSON lives in `clickbench/queries/q00.json` through
  `clickbench/queries/q42.json`.
- KalDB uses `schema/clickbench_hits_schema.yaml`.
- OpenSearch uses `schema/clickbench_hits_mapping.json`.
- Generated data and benchmark output are ignored by git.

## Prerequisites

Install:

- Docker with the Compose plugin
- Ruby
- `curl`
- `jq`

Build and start the local benchmark stack:

```bash
cd benchmark-astra-and-os
ASTRA_DEPLOYMENT=single ./setup_clusters.sh
```

`setup_clusters.sh` builds the local `slackhq/astra` image and starts KalDB,
OpenSearch, Kafka, ZooKeeper, MinIO, and Zipkin using
`benchmark-astra-and-os/docker-compose.yml`.

## Smoke Run

Use a tiny data slice before running a large benchmark:

```bash
cd benchmark-astra-and-os
SPLITS_COUNT=2 SPLIT_SIZE=5 ./setup_data.sh
ASTRA_DEPLOYMENT=single ./setup_indices.sh 2 both
BENCHMARK_QUERIES=q0,q1 ruby tracked_benchmark.rb \
  --label smoke \
  --note "10 ClickBench rows; q0/q1 smoke run" \
  --iterations 1
```

The tracked runner writes a bundle under `history/runs/<timestamp>-smoke/`.

## Normal Benchmark

Prepare enough ClickBench rows once:

```bash
MAX_RECORDS=10000000 SPLIT_SIZE=1000 ./setup_data.sh
```

Load the desired number of split files into both systems:

```bash
ASTRA_DEPLOYMENT=single \
OS_NUMBER_OF_SHARDS=1 \
INDEXER_MAX_MESSAGES_PER_CHUNK=10000000 \
./setup_indices.sh 10000 both
```

Run five measured iterations:

```bash
ASTRA_DEPLOYMENT=single \
OS_REQUEST_CACHE=false \
ruby tracked_benchmark.rb \
  --label clickbench-10m-1shard-1chunk \
  --note "10M rows; single KalDB chunk; single OpenSearch shard" \
  --iterations 5
```

Useful split counts:

| Rows | Split files |
| ---: | ---: |
| 1M | 1000 |
| 5M | 5000 |
| 10M | 10000 |
| 20M | 20000 |
| 25M | 25000 |

## Query Set

The checked-in query files cover all ClickBench slots, Q0 through Q42.

Some queries use large ClickBench offsets or `HAVING` thresholds that are too
large for tiny E2E compatibility datasets. The benchmark JSON keeps the
full-scale values:

- Q27 and Q28 use `min_doc_count=100001`, matching `HAVING COUNT(*) > 100000`.
- Q38 and Q39 use `bucket_sort.from=1000`, `bucket_sort.size=10`, and parent
  bucket size `1010`.
- Q40 uses `bucket_sort.from=100`, `bucket_sort.size=10`, and parent bucket
  size `110`.
- Q41 uses `bucket_sort.from=10000`, `bucket_sort.size=10`, and parent bucket
  size `10010`.
- Q42 uses `bucket_sort.from=1000` and `bucket_sort.size=10`.

## Cold/Hot Protocol

For ClickBench-style cold/hot measurements, use:

```bash
ASTRA_DEPLOYMENT=single \
OS_REQUEST_CACHE=false \
EXPECTED_DOCS=10000000 \
ruby clickbench_protocol_benchmark.rb \
  --label clickbench-10m-protocol \
  --repetitions 2
```

For each query and subject, the protocol runner records:

- `cold`: restart/drop-cache path before timing the query.
- `hot1`: immediate repeat after `cold`.
- `hot2`: immediate repeat after `hot1`.

The ClickBench-style weighted latency for a query is:

```text
0.25 * cold + 0.75 * min(hot1, hot2)
```

Protocol bundles are written under `history/protocol-runs/`.

## Important Configuration

Common environment variables:

- `ASTRA_DEPLOYMENT`: `single` or `split`; default `split`.
- `OS_NUMBER_OF_SHARDS`: OpenSearch primary shard count; default `10`.
- `INDEXER_MAX_MESSAGES_PER_CHUNK`: KalDB chunk row target.
- `INDEXER_MAX_BYTES_PER_CHUNK`: KalDB chunk byte target.
- `OPENSEARCH_JAVA_OPTS`: OpenSearch heap, for example `-Xms16g -Xmx16g`.
- `ASTRA_JAVA_TOOL_OPTIONS`: KalDB JVM options.
- `OS_REQUEST_CACHE`: OpenSearch `_msearch` `request_cache`; default `false`.
- `CLICKBENCH_TRACK_TOTAL_HITS`: whether measured requests ask for exact hit
  totals; default `false`.
- `BENCHMARK_QUERIES`: comma-separated query subset, for example `q0,q6,q19`.
- `ASTRA_REQUIRE_DURABLE_AFTER_LOAD`: set `true` to wait for persisted,
  cache-backed KalDB data after ingest.

Q19 uses a specific `UserID`. The harness patches Q19 to a `UserID` present in
the loaded OpenSearch slice by default. Override with `CB_Q19_USER_ID`, or set
`CB_PATCH_Q19_USER_ID=false` to run the checked-in request unchanged.

## Output Files

Tracked benchmark bundles contain:

- `benchmark.csv`: one aggregate row per query.
- `samples.csv`: one row per measured query iteration.
- `comparison.csv`: compact KalDB/OpenSearch numbers and ratios.
- `output/`: raw JSON responses.
- `run.json`: run metadata, git state, JVM versions, and captured environment.
- `summary.md`: human-readable summary.

Protocol bundles contain:

- `samples.csv`: per-query, per-subject, per-phase timings.
- `summary.csv`: cold/hot summary by query and subject.
- `variation.csv`: variation across repetitions when `--repetitions >= 2`.
- `response_comparison.csv`: response parity notes for each phase.
- `output/`: raw JSON responses.
- `run.json`: run metadata and JVM capture.

In ratio columns, `kaldb/opensearch < 1` means KalDB was faster and
`kaldb/opensearch > 1` means OpenSearch was faster.

## Result Validation

The runners fail rows when either service returns:

- HTTP errors
- `_msearch` `errors=true`
- per-response error objects or error statuses
- missing aggregations for aggregation queries
- missing hits for hit-returning queries

They also compare KalDB and OpenSearch responses. For distributed
terms/multi-terms aggregations, some bucket-ranking differences can be caused by
OpenSearch-compatible `shard_size` behavior. Keep those rows labeled when using
them for performance analysis rather than mixing them into exact-parity rows.

## Cleanup

```bash
./teardown.sh
```
