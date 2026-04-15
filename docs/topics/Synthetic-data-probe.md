# Synthetic Data Probe

The synthetic data probe is a low-rate production canary for KalDB ingestion and
querying. It continuously writes synthetic documents through the normal bulk
ingest path, continuously queries those documents through the normal query path,
and exposes Prometheus metrics that can drive Grafana dashboards and alerts.

This is intentionally not a load generator. Its purpose is to answer a narrow
operational question: "Can this cluster ingest known documents and return the
expected documents from query?"

## Motivation

The probe is designed around the production monitoring requirement:

- ingestion and querying should run continuously, not one after the other
- failures should be observable through Prometheus and Grafana metrics
- alerts should be evaluated by the monitoring system instead of the probe
  process exiting on assertion failures
- the existing load generation tools should remain separate and unchanged
- the probe should run in the same deployed KalDB artifact instead of requiring
  a separate standalone jar

The result is a dedicated runtime probe packaged in `astra.jar` and deployed by
the Helm chart as its own Kubernetes Deployment.

## Data Model

Each ingest cycle writes one batch of documents into a one-minute timestamp
bucket. The probe alternates documents between two hostnames:

- `TARGET_HOSTNAME`, which is included in expected query results
- `DISTRACTOR_HOSTNAME`, which is deliberately excluded by the query filter

The query filters on both the probe run id and the target hostname. This makes
the signal more meaningful than "some synthetic data exists" because each query
must return the target half of the generated documents and ignore the distractor
half.

The important document fields are:

- `@timestamp`
- `test_run_id`
- `hostname`
- `bucket_epoch_ms`
- `doc_id`
- `message`

## Query Window

The probe tracks expected document counts in memory by one-minute bucket. Query
readiness is therefore based on wall-clock time since the current probe process
started. A restarted probe does not assume old data is valid because it no
longer has the exact in-memory expected counts for old buckets.

The deployed defaults are:

```text
WINDOW_BUCKETS=15
NEWEST_BUCKET_AGE_MINUTES=3
```

With those values, the probe writes documents into the current wall-clock
minute bucket and checks buckets 3 through 17 minutes old. This gives a usable
post-redeploy signal after roughly 17 minutes while avoiding the freshest
completed bucket, which can still be catching up in query visibility.

The probe intentionally does not delay ingest timestamps. Query stability should
come from `NEWEST_BUCKET_AGE_MINUTES`, not from writing artificially old event
times. This keeps the synthetic data aligned with current cluster behavior while
still leaving enough time for recently ingested data to become visible to query.

The ingest and query loops run independently. A single Prometheus scrape can
therefore land after ingest has updated the expected count for a bucket but
before the next query cycle has refreshed the observed count. Alerts should
require failures to persist across multiple query intervals rather than firing
on one scrape.

## Query Path Coverage

The indexer rolls active chunks when any of these conditions is true:

- indexed Lucene bytes exceed `maxBytesPerChunk`
- indexed messages exceed `maxMessagesPerChunk`
- chunk wall-clock age exceeds `maxTimePerChunkSeconds`

The current probe does not yet prove which query sub-path served each result.
It verifies end-to-end ingest and query correctness for the configured window.
If we need to alert specifically on "live indexer path and cache path were both
hit", that should be a separate enhancement with explicit path-coverage metrics.

## Scheduling And Catch-up

Ingest cycles are scheduled at a fixed rate. If a cycle is delayed or missed,
the probe does not skip indefinitely. Instead, it catches up missed one-minute
buckets, capped by `MAX_CATCHUP_BUCKETS_PER_CYCLE`.

The cap prevents an unbounded write burst after a stall while still allowing the
probe to repair a small backlog. This keeps the implementation simple and avoids
turning the canary into a load test.

## Metrics

The probe exposes Prometheus metrics on `/metrics`.

Key metrics:

- `kaldb_synthetic_data_probe_up`
- `kaldb_synthetic_data_probe_window_ready`
- `kaldb_synthetic_data_probe_window_buckets_with_expected_docs`
- `kaldb_synthetic_data_probe_active_ingest_bucket_epoch_seconds`
- `kaldb_synthetic_data_probe_ingest_requests_total`
- `kaldb_synthetic_data_probe_ingest_batches_succeeded_total`
- `kaldb_synthetic_data_probe_ingest_batches_failed_total`
- `kaldb_synthetic_data_probe_ingest_docs_accepted_total`
- `kaldb_synthetic_data_probe_ingest_docs_failed_total`
- `kaldb_synthetic_data_probe_ingest_coverage_backlog_buckets`
- `kaldb_synthetic_data_probe_ingest_catchup_batches_total`
- `kaldb_synthetic_data_probe_last_ingest_success_epoch_seconds`
- `kaldb_synthetic_data_probe_last_ingest_status_code`
- `kaldb_synthetic_data_probe_query_requests_total`
- `kaldb_synthetic_data_probe_query_succeeded_total`
- `kaldb_synthetic_data_probe_query_failed_total`
- `kaldb_synthetic_data_probe_last_query_success_epoch_seconds`
- `kaldb_synthetic_data_probe_last_query_status_code`
- `kaldb_synthetic_data_probe_window_min_ratio`
- `kaldb_synthetic_data_probe_window_max_abs_delta_docs`
- `kaldb_synthetic_data_probe_bucket_expected_docs`
- `kaldb_synthetic_data_probe_bucket_observed_docs`
- `kaldb_synthetic_data_probe_bucket_doc_delta`
- `kaldb_synthetic_data_probe_bucket_doc_ratio`
- `kaldb_synthetic_data_probe_bucket_epoch_seconds`

`window_ready` is the guardrail metric. Alerting should not treat query mismatch
metrics as meaningful until `window_ready` is `1`.

## Alerting Shape

A practical alert should check:

- the probe is up
- the window is ready
- ingest failures are not increasing
- query failures are not increasing
- the maximum absolute document delta is zero while the window is ready
- the minimum document ratio is one while the window is ready
- ingest coverage backlog is not growing

The exact alert thresholds should be owned by the production Prometheus/Grafana
configuration, not by the probe process. Alert conditions should also include a
`for` duration or equivalent persistence check that spans multiple query
intervals. The default query interval is 10 seconds.

## Troubleshooting A Ready Window

If `window_ready` is `1` but the window delta or ratio metrics show missing
documents, ingestion and query HTTP plumbing are working but the query result is
not complete. Check cache and replica assignment first:

- cache capacity versus total replica demand
- cache assignment errors
- indexer chunk rollover and stale chunk cleanup timing
- whether rolled chunks are assigned and searchable before the indexer evicts
  them locally

## Current Limitations

- expected counts are in memory, so a probe restart resets readiness
- the probe does not backfill historical buckets at startup
- expected counts update during ingest cycles and observed counts update during
  query cycles, so individual scrapes can briefly see the two out of phase
- rolling deploys that reuse the same `SYNTHETIC_DATA_PROBE_RUN_ID` can briefly
  leave one overlap bucket from the old and new pods; the default generated run
  id avoids this for normal deployments
- the probe verifies end-to-end correctness, not per-query-path coverage
- it assumes the configured dataset and Kafka partition are safe for synthetic
  data in the target cluster
- it is a canary, not a throughput or saturation test
