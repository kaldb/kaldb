# Benchmark Suite Fixes: Problems Found and Changes Made

## How to Use This Document
Feed this to ChatGPT and ask it to walk you through it in small chunks. Start wherever you want — each section is self-contained. The document covers every problem we found in the benchmark suite, what was wrong at the code level, and exactly what we changed to fix it.

---

## Context: What This Benchmark Suite Does

The benchmark suite lives in `benchmark-astra-and-os/` and compares Astra (the log search system in this repo) against OpenSearch (an Elasticsearch fork). The workflow is:

1. **`setup_clusters.sh`** — Starts Docker containers for both OpenSearch and Astra
2. **`setup_indices.sh`** — Loads NYC taxi trip data into both systems
3. **`benchmark.rb`** — Runs 19 query types at 15 different result sizes against both, times them, compares results
4. **`teardown.sh`** — Stops everything

The benchmark was broken in several ways. Here's every problem we found, verified, and fixed.

---

## Problem 1: Running the Benchmark With No Arguments Did Nothing

### The Bug

In Ruby, when you run `benchmark.rb` with no command-line arguments:

```ruby
iterations = ARGV.first.to_i || 1
```

`ARGV.first` is `nil` (no arguments). `nil.to_i` returns `0` in Ruby. Then `0 || 1` — here's the trap: in Ruby, `0` is **truthy** (only `nil` and `false` are falsy). So `0 || 1` evaluates to `0`, not `1`.

The result: `iterations = 0`, and `0.times { ... }` runs zero times. The benchmark silently does nothing.

### Why This Was Hard to Notice

The script didn't print any error. It just... exited. If you didn't know to pass an argument like `./benchmark.rb 1`, you'd think it ran and produced no output.

### The Fix (commit `e34f18f1`)

```ruby
# Before:
iterations = ARGV.first.to_i || 1

# After:
iterations = (ARGV.first || 1).to_i
iterations = 1 if iterations <= 0
```

We moved the `|| 1` fallback to happen **before** `.to_i`, so it applies when `ARGV.first` is `nil` (before the integer conversion). The second line is a safety net for someone passing `0` or a negative number explicitly.

### The Ruby Truthiness Lesson

This is a common Ruby gotcha. In most languages, `0` is falsy. In Ruby, it's truthy:

```ruby
0 || 1     # => 0  (Ruby: 0 is truthy, so || doesn't fire)
nil || 1   # => 1  (nil is falsy, so || fires)
false || 1 # => 1  (false is falsy, so || fires)
```

If you're coming from Python, JavaScript, or C, this will bite you.

---

## Problem 2: The Rescue That Hid All Comparison Failures

### The Bug

The benchmark compared Astra vs OpenSearch results after each query. Here's what the original code looked like:

```ruby
_astra, _os = timings.map(&:last).map{|json|
  JSON.parse(json).dig("responses", 0, "hits", "hits").
     map {|hit| hit["_source"]["total_amount"]}.sort} rescue [[100],[100]]
```

That `rescue [[100],[100]]` at the end is a **blanket rescue** — if **anything** goes wrong anywhere in that pipeline (JSON parse fails, `dig` returns nil, a field is missing), it catches the exception and returns `[[100], [100]]`. Since both sides are now `[100]`, the comparison `_astra != _os` is false, so it silently reports "results match!"

This meant:
- If Astra returned a 500 error → "results match!"
- If the JSON structure was different than expected → "results match!"
- If one system returned no hits → "results match!"

Every comparison failure was invisible.

### The Fix (commit `33fe1326`)

We replaced the blanket rescue with per-side parsing that reports failures:

```ruby
_astra, _os = timings.map(&:last).map { |json|
  parsed = JSON.parse(json) rescue nil
  next nil unless parsed
  hits = parsed.dig("responses", 0, "hits", "hits")
  next nil unless hits
  hits.map { |hit| hit["_source"]["total_amount"] }.sort
}
```

Now if parsing fails on either side, that side becomes `nil` and the output says "comparison skipped (parse error)" instead of silently claiming a match.

### Why Blanket Rescue Is Dangerous

Ruby's `rescue` without specifying an exception class catches `StandardError` and all its subclasses. When you put it at the end of a multi-line expression, it catches errors from **any** line. This is the Ruby equivalent of:

```python
try:
    # 10 lines of code with 5 different things that could fail
except:
    return "everything is fine!"
```

You should almost never use bare `rescue` in production code.

---

## Problem 3: Data Loading Was All-or-Nothing

### The Bug

The original `setup_indices.sh` loaded data into both systems in a single loop:

```bash
for f in $(ls data/ready/* | head -n $load_ct_splits); do
  echo -n "$f\t"
  echo -n "OpenSearch: "
  curl ... "https://localhost:9200/_bulk" ... && echo -n " DONE." || echo -n " FAILED."
  echo -n " Astra:"
  curl ... "http://localhost:8080/_local_bulk" ... && echo " DONE." || echo " FAILED."
done
```

Problems:
1. **If Astra isn't running, you can't load OpenSearch** — both loads happen in the same loop, so to load OS you need Astra up and vice versa. If one system is down or misconfigured, the other side's load fails or you have to sit through error messages for every file.
2. **No failure counting** — the `|| echo " FAILED."` continues to the next file. You don't know at the end how many files failed.
3. **Can't re-run one side** — if OpenSearch loaded fine but Astra's load needs to be redone, you have to re-load both.

### The Fix (commit `b0554d1d`)

Split into separate `load_os()` and `load_astra()` functions, with a `target` argument:

```bash
./setup_indices.sh 10 os      # Only load OpenSearch
./setup_indices.sh 10 astra   # Only load Astra
./setup_indices.sh 10 both    # Load both (default)
```

Each function tracks its own failure count:

```bash
load_os() {
  local failed=0
  for f in ...; do
    if curl --fail ...; then
      echo "DONE."
    else
      echo "FAILED."
      failed=$((failed + 1))
    fi
  done
  if [ "$failed" -gt 0 ]; then
    echo "WARNING: $failed files failed to load into OpenSearch"
    return 1
  fi
}
```

This was practically necessary during testing. When the Astra container was crashing due to chunk rollover issues, we could still load and test OpenSearch independently.

---

## Problem 4: No Health Checks Before Loading Data

### The Bug

The original `setup_clusters.sh` started containers and returned immediately:

```bash
docker run -d ... opensearch ...
docker compose up -d
# script ends — hope the services are ready!
```

Docker containers take time to initialize. OpenSearch needs to create its cluster state, allocate shards, etc. Astra needs ZooKeeper → Kafka → preprocessor → indexer to all be ready. If `setup_indices.sh` runs immediately after `setup_clusters.sh`, it hits services that aren't ready yet, and data loading fails with connection errors.

### The Fix (commit `d341df21`)

Added a `wait_for_url()` function that polls a health endpoint:

```bash
wait_for_url() {
  local name="$1" url="$2" curl_opts="${3:-}"
  local max_attempts=30    # 30 attempts × 2 seconds = 60 second timeout
  echo -n "Waiting for $name"
  for i in $(seq 1 $max_attempts); do
    if curl -sf -o /dev/null $curl_opts "$url" 2>/dev/null; then
      echo " ready."
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo " timed out after $((max_attempts * 2))s!"
  return 1
}

# After starting containers:
wait_for_url "OpenSearch" "https://localhost:9200/_cluster/health" "-ku admin:$OS_PW"
wait_for_url "Astra" "http://localhost:8080/_msearch" ""
```

This blocks until both systems are actually ready to receive requests, or times out after 60 seconds.

---

## Problem 5: No Way to Stop the Benchmark Containers

### The Bug

There was no `teardown.sh`. To stop everything you had to remember:
- The OpenSearch container name (or find it with `docker ps`)
- That Astra uses docker-compose (so you need `docker compose down` in the right directory)

### The Fix (commit `f279089c`)

Added `teardown.sh`:

```bash
#!/usr/bin/env bash
set -eu

echo "Stopping OpenSearch..."
docker rm -f benchmark_opensearch 2>/dev/null || true

echo "Stopping Astra..."
pushd .. > /dev/null || exit 1
  docker compose down
popd > /dev/null

echo "Teardown complete."
```

The `benchmark_opensearch` name matches what `setup_clusters.sh` creates via `docker run --name benchmark_opensearch`.

---

## Problem 6: Shell Scripts Used zsh Shebangs

### The Bug

Several scripts had `#!/bin/zsh` shebangs:

```bash
#!/bin/zsh    # setup_indices.sh, query_astra.sh, etc.
```

On machines without zsh installed (most Linux servers, CI environments), these scripts fail with:

```
/bin/zsh: No such file or directory
```

Even on machines with zsh, the scripts didn't use any zsh-specific features.

### The Fix (commit `2717a215`)

Changed all shebangs to use bash:

```bash
#!/usr/bin/env bash
```

Using `env bash` instead of `/bin/bash` is a portable convention — it finds `bash` wherever it's installed on the system, rather than assuming it's at `/bin/bash` (on NixOS, for example, it's not).

---

## Problem 7: The Comparison Logic Didn't Understand Format Differences

### The Bug

The original comparison sorted both sides' `total_amount` values and compared them:

```ruby
map {|hit| hit["_source"]["total_amount"]}.sort
```

This comparison was broken in multiple ways:
1. **Without explicit sort, Astra and OS return hits in different orders.** Astra defaults to timestamp order; OpenSearch defaults to relevance score order. Even after sorting `total_amount` values, if there are ties (e.g., many rides costing $5.00), the sets of documents returned are different — so the sorted values don't match.
2. **Aggregation results were never compared.** The code only compared hits, completely ignoring whether aggregation buckets matched between systems.
3. **The blanket rescue (Problem 2) hid all of this anyway.**

### The Fix (commit `947b3295`)

Replaced the simple comparison with two new functions: `compare_responses` and `compare_aggs` (~90 lines total).

**`compare_responses(astra_json, os_json, query_str, requested_size)`** does three things:

1. **Compares hit counts** — both systems should return the same number of hits:
```ruby
astra_hit_ct = (astra_resp.dig("hits", "hits") || []).size
os_hit_ct = (os_resp.dig("hits", "hits") || []).size
if astra_hit_ct != os_hit_ct
  notes << "hit count: astra=#{astra_hit_ct} os=#{os_hit_ct}"
end
```

2. **Compares hit documents only when there's an explicit sort** — without explicit sort, comparing documents is meaningless because the systems return different documents:
```ruby
if has_sort && requested_size > 0
  astra_vals = (astra_resp.dig("hits", "hits") || []).map { |h| h.dig("_source", "total_amount") }
  os_vals = (os_resp.dig("hits", "hits") || []).map { |h| h.dig("_source", "total_amount") }
  if astra_vals != os_vals
    notes << "sorted hits differ"
  end
end
```

3. **Compares aggregation buckets** — delegates to `compare_aggs`, which checks bucket count and doc_count per bucket:
```ruby
if has_aggs
  astra_aggs = astra_resp["aggregations"]
  os_aggs = os_resp["aggregations"]
  # ... nil checks ...
  agg_diffs = compare_aggs(astra_aggs, os_aggs)
  notes.concat(agg_diffs) if agg_diffs.any?
end
```

**`compare_aggs(astra_aggs, os_aggs)`** walks the aggregation tree:
- Checks that both sides have the same aggregation keys
- For bucket aggregations, checks that the bucket count matches
- For each bucket, checks that `doc_count` is the same
- Reports the first mismatching bucket (to avoid flooding the output)

### Design Decision: What Not to Compare

We deliberately **don't** compare:
- **`hits.total.value`** — Astra reports the number of hits returned (capped by `size`), while OpenSearch reports the total number of matching documents. This is an Astra bug, but it's in the Astra codebase, not something the benchmark should try to work around.
- **Hit documents without explicit sort** — the systems use different default orderings, so the sets of returned documents legitimately differ.
- **Exact aggregation key formats** — Astra and OpenSearch may format date bucket keys slightly differently (timezone handling, precision).

---

## Problem 8: Nothing Was Configurable

### The Bug

Ports, URLs, and authentication were all hardcoded:

```ruby
# benchmark.rb
ports = { "astra" => 8080, "os" => 9200 }
curls = { ... "curl -s --fail -ku admin:$OS_PW 'https://localhost:9200/_msearch'" ... }
```

```bash
# setup_indices.sh
curl -XDELETE "https://localhost:9200/test" -ku admin:$OS_PW
curl ... "http://localhost:8080/_local_bulk" ...
```

This meant:
- You couldn't run against OpenSearch on a different port
- You couldn't run against OpenSearch with TLS disabled (which our test deployment used — `DISABLE_SECURITY_PLUGIN=true` means plain HTTP, not HTTPS)
- You couldn't point at a different Astra endpoint

### The Fix (commit `4033db0b`)

Added environment variable configuration to both `benchmark.rb` and `setup_indices.sh`:

**benchmark.rb:**
```ruby
ports = { "astra" => ENV.fetch("ASTRA_QUERY_PORT", "8080").to_i,
          "os" => ENV.fetch("OS_PORT", "9200").to_i }
os_scheme = ENV.fetch("OS_SCHEME", "https")
os_auth = os_scheme == "https" ? "-ku admin:$OS_PW" : ""
```

**setup_indices.sh:**
```bash
OS_SCHEME="${OS_SCHEME:-https}"
OS_PORT="${OS_PORT:-9200}"
ASTRA_BULK_PORT="${ASTRA_BULK_PORT:-8080}"
ASTRA_BULK_PATH="${ASTRA_BULK_PATH:-/_local_bulk}"
```

The defaults preserve the original behavior (`https`, port `9200`, etc.) so existing usage doesn't break.

### How We Discovered This Was Needed

When we tried to test against the deployed Astra/OpenSearch setup, OpenSearch was running with security disabled (plain HTTP). The hardcoded `https://` and `-ku admin:$OS_PW` caused curl to fail with SSL errors. Rather than hacking the script each time, we made it configurable.

---

## Problem 9: Docker Images Were Stale/Unavailable

### The Bug

`docker-compose.yml` referenced container images that no longer exist on Docker Hub:

```yaml
zookeeper:
  image: 'bitnami/zookeeper:3.6.3'    # Very old, may not pull on all architectures
kafka:
  image: 'bitnami/kafka:3.2.3'         # REMOVED from Docker Hub entirely
```

Bitnami moved their old Kafka images to `bitnamilegacy/kafka`. The original `bitnami/kafka:3.2.3` tag was deleted. Running `docker compose pull` would fail.

### The Fix (commit `0894b6d7`)

Updated to images that actually exist:

```yaml
zookeeper:
  image: 'zookeeper:3.9'               # Official Apache ZooKeeper image
kafka:
  image: 'bitnamilegacy/kafka:3.2.3'   # Where Bitnami moved the old images
```

This same commit also increased the chunk limits:

```yaml
# Before:
INDEXER_MAX_BYTES_PER_CHUNK=800000000        # 800MB
INDEXER_MAX_TIME_PER_CHUNK_SECONDS=120       # 2 minutes

# After:
INDEXER_MAX_BYTES_PER_CHUNK=80000000000      # 80GB (effectively unlimited)
INDEXER_MAX_TIME_PER_CHUNK_SECONDS=36000     # 10 hours (effectively unlimited)
```

### Why the Chunk Limit Change Was Needed

With the default 120-second time limit, the indexer would try to "roll over" its Lucene chunk every 2 minutes. Rollover means: snapshot the chunk to S3, upload it, then start a fresh chunk. In the Docker setup, S3 is mocked (using `adobe/s3mock`), and the upload was failing. Once rollover failed, **all subsequent ingestion was blocked** — the indexer refused to accept new data.

By setting the time limit to 10 hours, rollover never triggers during a benchmark run, so all data stays in the in-memory Lucene index and S3 is never touched.

---

## Problem 10: Heatmap Didn't Show Unreliable Results

### The Bug

After all the above fixes, the benchmark's summary heatmap showed a colored ratio (Astra ms / OpenSearch ms) for each query×size cell. But when the results between systems didn't match (e.g., different hit counts), the timing ratio is meaningless — you're comparing apples to oranges. The heatmap didn't distinguish "Astra is 2x slower and returning correct results" from "Astra is 2x slower and returning wrong results."

### The Fix (commit `a3836b10`)

Added mismatch detection to the heatmap. When `compare_responses` finds differences, the cell is rendered with a gray `!` prefix instead of a color:

```ruby
def color_code ratio, mismatch: false
  if mismatch
    return "\033[97;90m!#{ratio.round(2).to_s.ljust(4)}\033[0m"
  end
  # ... normal color logic ...
end
```

We also created `replay_heatmap.rb` — a standalone tool that re-reads the saved output JSON files and CSV timing data from a previous benchmark run and regenerates the heatmap with comparison logic applied. This means you can fix a comparison bug and re-analyze old results without re-running the benchmark.

---

## Infrastructure Problems Encountered During Testing

These weren't code fixes — they were issues we hit while trying to actually run the benchmark end-to-end.

### Kafka Ephemeral Node Conflict

After restarting the Docker containers, Kafka failed to start:

```
org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists for /brokers/ids/1
```

ZooKeeper still had a stale "ephemeral node" from the previous Kafka instance (ZooKeeper didn't clean it up because the session wasn't properly closed). Fix: restart ZooKeeper first, wait for it, then restart Kafka.

### S3 Upload URI Parsing Bug

The Astra indexer crashed during chunk rollover:

```
java.net.URISyntaxException: Expected scheme-specific part at index 5: http:
```

This was a bug in the S3 client configuration in an older Astra image. Resolved by building from the current branch.

### Astra Returns 500 for size:0 + No Aggregations

When you send `{"query": {"match_all": {}}, "size": 0}` (no hits, no aggregations), Astra returns:

```
Status 500: "Hits or aggregation should be requested"
```

OpenSearch returns an empty hits array. This is an Astra validation bug — `size: 0` is valid in Elasticsearch to get metadata/aggregations only, or just to test if a query parses. The benchmark marks these cells with `*` as errors.

### Container Name Conflicts

Docker containers from a previous compose project had the same names as our benchmark containers. `docker compose up -d` failed because the names were taken. Fixed by `docker rm -f` on conflicting containers before starting.

---

## Summary: The 10 Commits in Order

| # | Commit | What it fixed |
|---|--------|--------------|
| 1 | `e34f18f1` | Zero-iteration default — benchmark did nothing without args |
| 2 | `33fe1326` | Blanket rescue hiding all comparison failures |
| 3 | `b0554d1d` | All-or-nothing data loading — decoupled OS and Astra |
| 4 | `d341df21` | No health checks — scripts ran before services were ready |
| 5 | `f279089c` | No teardown script — had to manually find and kill containers |
| 6 | `2717a215` | zsh shebangs — scripts failed on Linux without zsh |
| 7 | `947b3295` | Comparison logic — didn't understand format differences |
| 8 | `4033db0b` | Hardcoded config — couldn't test different deployments |
| 9 | `0894b6d7` | Stale Docker images + chunk limits blocking ingestion |
| 10 | `a3836b10` | Heatmap didn't flag unreliable results |

### The Theme

Most of these problems share a common trait: **they failed silently**. The zero-iteration bug, the blanket rescue, the lack of health checks, the hardcoded HTTPS — none of them produced clear error messages. The benchmark would appear to run, maybe produce some output, and you'd have to already know what correct output looked like to realize something was wrong.

The fixes prioritize making failures visible: error counts, comparison notes, health check timeouts, and heatmap markers for unreliable data.
