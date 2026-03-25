Benchmarking Astra and OpenSearch
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
   - Astra checked out in this repo
3. Prepare the benchmark environment:
   ```bash
   ./setup_data.sh
   ./setup_clusters.sh
   ./setup_indices.sh
   ```
4. Export any required credentials:
   ```bash
   export OS_PW='...'
   ```

Quick Start
-----------

Run a tracked benchmark:

```bash
ruby tracked_benchmark.rb \
  --label lucene-bump \
  --note "Baseline after Lucene/OpenSearch version update" \
  --iterations 1
```

That creates a run bundle under `history/runs/<timestamp>-<label>/` with:

- `benchmark.csv`: the benchmark result table
- `output/`: raw Astra/OpenSearch JSON responses for every query and size
- `run.json`: metadata about the run
- `summary.md`: a short summary
- `git-status.txt`
- `git-diff.patch`
- `git-diff-cached.patch`

This is the main workflow to use when you are changing code and want a durable record of what was tested.

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

- biggest Astra improvements
- biggest Astra regressions
- biggest Astra/OpenSearch ratio improvements
- biggest Astra/OpenSearch ratio regressions

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

`tracked_benchmark.rb` uses those to write into a single run bundle.
