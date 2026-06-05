Benchmarking KalDB and OpenSearch
=================================

This directory has two ways to run the benchmark:

- `benchmark.rb`: the raw harness. It writes a CSV and per-request JSON output.
- `tracked_benchmark.rb`: the durable workflow. It creates a self-contained run bundle with the benchmark output, git state, version info, and a summary.

If you are trying to understand changes over days or weeks, use `tracked_benchmark.rb`.

Setup
-----

1. Ensure you have Docker installed and running.
2. Assumes you have:
   - `ruby`
   - `docker`
   - KalDB checked out in this repo
3. Prepare the benchmark environment:
   ```bash
   ./setup_data.sh
   ./setup_clusters.sh
   ./setup_indices.sh
   ```

To run KalDB as a single multi-role process instead of split role containers,
set `ASTRA_DEPLOYMENT=single` for cluster setup and loading:

```bash
ASTRA_DEPLOYMENT=single ./setup_clusters.sh
ASTRA_DEPLOYMENT=single ./setup_indices.sh 1000 astra
```

The single-node mode starts one `astra_single` container with
`PREPROCESSOR,INDEX,MANAGER,QUERY,CACHE,RECOVERY` roles and exposes the same
local ports as the split setup.

The default setup uses this repo's `docker-compose.yml`, including the local
OpenSearch service with security disabled. No OpenSearch credentials are
required for the default local benchmark.

The compose file defaults OpenSearch to a 4 GB JVM heap because the ClickBench
aggregation set can trip circuit breakers with the old 256 MB local default.
Override it with `OPENSEARCH_JAVA_OPTS` if the machine needs a different size.

By default `setup_data.sh` prepares a 1M-row slice of the official ClickBench
`hits` dataset: 1000 split files with 1000 documents per file. Override that
for quicker smoke runs:

```bash
SPLITS_COUNT=2 SPLIT_SIZE=5 ./setup_data.sh
./setup_indices.sh 2
```

To prepare the full ClickBench dataset, use:

```bash
MAX_RECORDS=all SPLIT_SIZE=1000 ./setup_data.sh
```

The legacy NYC taxi benchmark path is still available with `DATASET=nyc_taxis`.

The scripts use these endpoint defaults:

- KalDB query: `http://localhost:8081/_msearch`
- KalDB bulk ingest: `http://localhost:8086/_bulk`
- OpenSearch: `http://localhost:9200`

Benchmark queries send OpenSearch `_msearch` requests with `request_cache=false`
in the per-search metadata line by default, so repeated runs measure query
execution instead of cached aggregation responses. Set `OS_REQUEST_CACHE=true`
only when you intentionally want to measure OpenSearch request-cache behavior.

Override them with `ASTRA_QUERY_HOST`, `ASTRA_QUERY_PORT`, `ASTRA_BULK_HOST`,
`ASTRA_BULK_PORT`, `ASTRA_BULK_PATH`, `OS_HOST`, `OS_PORT`, `OS_SCHEME`,
`OS_USER`, `OS_PW`, and `OS_CURL_INSECURE`.

KalDB bulk ingest must also be provisioned through the manager API. `setup_indices.sh`
does that automatically before loading KalDB. The defaults provision dataset
`hits`, service pattern `hits`, partition `0`, and 1GB/s throughput. Override
those with `INDEX_NAME`, `ASTRA_PARTITION_ID`, `ASTRA_DATASET_THROUGHPUT_BYTES`,
and `ASTRA_PARTITION_MAX_CAPACITY_BYTES`.

Quick Start
-----------

Run a tracked benchmark:

```bash
ruby tracked_benchmark.rb \
  --label lucene-bump \
  --note "Baseline after Lucene/OpenSearch version update" \
  --iterations 5
```

For a fast smoke run:

```bash
BENCHMARK_QUERIES=q0,q1 ruby benchmark.rb 1
```

That creates a run bundle under `history/runs/<timestamp>-<label>/` with:

- `benchmark.csv`: the benchmark result table
- `samples.csv`: one row per query per measured iteration
- `comparison.csv`: compact comparison numbers, including median, trimmed average, min, and max latency
- `output/`: raw KalDB/OpenSearch JSON responses for every query and size
- `run.json`: metadata about the run
- `summary.md`: a short summary
- `git-status.txt`
- `git-diff.patch`
- `git-diff-cached.patch`

This is the main workflow to use when you are changing code and want a durable record of what was tested.

Latency Statistics
------------------

For ClickBench runs, `benchmark.csv` contains one aggregate row per query. The
primary `astra_ms` and `os_ms` values are medians across the measured
iterations. The same file also records:

- `*_trimmed_avg_ms`: average after dropping the fastest and slowest samples when there are at least 3 samples
- `*_min_ms`: fastest observed sample
- `*_max_ms`: slowest observed sample
- `*_samples`: number of measured samples

Use at least 5 iterations for normal comparisons:

```bash
ASTRA_DEPLOYMENT=single OS_REQUEST_CACHE=false ruby tracked_benchmark.rb \
  --label one-million-no-request-cache \
  --note "1M ClickBench rows; single-node KalDB; OpenSearch request_cache=false" \
  --iterations 5
```

The raw per-iteration rows stay in `samples.csv`.

ClickBench Q19
--------------

The compatibility test for Q19 uses controlled rows with a hard-coded `UserID`.
That ID is not guaranteed to exist in arbitrary performance slices of the
ClickBench dataset. For performance runs, the benchmark harness patches Q19 to
use a `UserID` found in the loaded OpenSearch index. Override it when needed:

```bash
CB_Q19_USER_ID=12345 ruby tracked_benchmark.rb --iterations 5
```

Set `CB_PATCH_Q19_USER_ID=false` only when you intentionally want to benchmark
the exact compatibility-test request.

ClickBench Query Coverage
-------------------------

The ClickBench benchmark path loads request JSON from
`astra/src/test/java/com/slack/astra/server/ClickBenchCompatibilityTest.java`.
That keeps benchmark requests aligned with the compatibility test assertions,
but it also means only compatibility specs present in that file can run.

At the time this harness was added, the compatibility suite contains Q0-Q26,
Q29-Q38, and Q40-Q42. It is missing Q27, Q28, and Q39. Those queries must be
ported into `ClickBenchCompatibilityTest` before the benchmark can include them.

Q13 is different: it is present in the compatibility suite, but some local
performance runs intentionally excluded it because the high-cardinality
`SearchPhrase` cardinality-order aggregation exhausted the available local
OpenSearch memory. Treat Q13 as a run exclusion, not as a missing query spec.

Dataset Sizes
-------------

To test scaling behavior, prepare enough ClickBench rows once and then load the
desired number of 1000-row split files. These counts correspond to the requested
dataset sizes:

| Rows | Split files |
| ---: | ---: |
| 1M | 1000 |
| 5M | 5000 |
| 10M | 10000 |
| 20M | 20000 |
| 25M | 25000 |

For example, to prepare up to 25M rows:

```bash
MAX_RECORDS=25000000 SPLIT_SIZE=1000 ./setup_data.sh
```

Then run a fresh load and benchmark for each size:

```bash
ASTRA_DEPLOYMENT=single ./setup_clusters.sh
ASTRA_DEPLOYMENT=single ./setup_indices.sh 1000 both
ASTRA_DEPLOYMENT=single OS_REQUEST_CACHE=false ruby tracked_benchmark.rb \
  --label clickbench-1m \
  --note "1M ClickBench rows" \
  --iterations 5
```

Repeat with `5000`, `10000`, `20000`, and `25000` split files for 5M, 10M,
20M, and 25M rows.

Result Quality
--------------

The tracked workflow fails the run when there are unexpected KalDB/OpenSearch
result mismatches or unexpected request failures. Known non-comparable rows are
marked as expected xfail rows in the summary instead of being mixed into the
comparable performance set.

What Gets Recorded
------------------

Each tracked run records:

- benchmark label and note
- start/end time and duration
- git branch and commit SHA
- whether the worktree was dirty
- the exact unstaged and staged patch contents
- Lucene and OpenSearch versions from `astra/pom.xml`
- the benchmark CSV and raw output JSON files
- a small machine-readable and human-readable summary

This matters because many benchmark sessions are not clean commits. If the tree is dirty, the saved patch files tell you exactly what local code was tested.

Comparing Runs
--------------

To compare two tracked runs:

```bash
ruby compare_runs.rb \
  history/runs/2026-03-25-10-00-00-before-change \
  history/runs/2026-03-25-15-30-00-after-change
```

To compare only one size, such as `size=0`:

```bash
ruby compare_runs.rb --count 0 OLD_RUN NEW_RUN
```

The comparison tool prints:

- biggest KalDB improvements
- biggest KalDB regressions
- biggest KalDB/OpenSearch ratio improvements
- biggest KalDB/OpenSearch ratio regressions

Replaying The Heatmap
---------------------

Tracked runs are self-contained, so you can replay the heatmap directly from the bundle:

```bash
ruby replay_heatmap.rb \
  history/runs/<run-id>/output \
  history/runs/<run-id>/benchmark.csv
```

Using The Raw Harness
---------------------

If you only want the legacy behavior:

```bash
ruby benchmark.rb 1
```

`benchmark.rb` now also accepts environment variables so callers can choose where the output goes:

- `BENCHMARK_RUN_ID`
- `BENCHMARK_OUTPUT_ROOT`
- `BENCHMARK_RESULTS_ROOT`
- `BENCHMARK_RESULTS_FILE`
- `BENCHMARK_RESULTS_PREFIX`
- `BENCHMARK_QUERIES`: comma-separated query names, such as `match_all,range`
- `BENCHMARK_SIZES`: comma-separated `size` values, such as `0,50,1000`
- `OS_REQUEST_CACHE`: OpenSearch `_msearch` request cache flag, default `false`

`tracked_benchmark.rb` uses those to write into a single run bundle.

Cleanup
-------

```bash
./teardown.sh
```
