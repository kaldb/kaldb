Deployed cluster E2E runner
===========================

`run_deployed_cluster_e2e.sh` starts local `kubectl port-forward` processes for
the ingest and query services, runs `DeployedClusterE2ETest`, and cleans up the
port-forwards on exit.

The script assumes any required `KUBECONFIG` and `AWS_PROFILE` are already set in
the calling environment, or that the default `kubectl`/AWS configuration is
correct.

```
tools/run_deployed_cluster_e2e.sh \
  --namespace <namespace>
```

Use `tools/run_deployed_cluster_e2e.sh --help` to see dataset, namespace, port,
and workload-size options.

Deployed cluster recovery E2E runner
====================================

`run_deployed_cluster_recovery_e2e.sh` runs
`DeployedClusterRecoveryE2ETest` as a three-phase harness. It port-forwards
ingest and query, temporarily lowers indexer `maxOffsetDelayMessages`, restarts
indexers onto that config, runs `BASELINE`, scales `statefulset/index` to zero,
runs `BACKLOG`, restores the original index replica count, and runs `VERIFY`.

The script restores the index ConfigMap and index StatefulSet on exit when it has
changed them, including failed or interrupted runs.

The script assumes any required `KUBECONFIG` and `AWS_PROFILE` are already set in
the calling environment, or that the default `kubectl`/AWS configuration is
correct.

```
tools/run_deployed_cluster_recovery_e2e.sh \
  --namespace <namespace>
```

By default, the runner sets `maxOffsetDelayMessages` to `100` for the test run so
the default `--backlog-docs 2000` should force recovery task creation. Use
`--max-offset-delay-messages` to change that value, or
`--skip-max-offset-delay-patch` to use the deployed config as-is.

Use `tools/run_deployed_cluster_recovery_e2e.sh --help` to see service,
StatefulSet, dataset, namespace, port, and workload-size options.

Steady-state monitor
====================

`tools/steady-state-monitor` is a continuous correctness monitor, not a load
test. It keeps ingest and query running in parallel at a low steady rate and
exports Prometheus metrics so Grafana and alert rules can detect drift between
expected and observed docs-per-minute buckets.

Build the shaded JAR:

```
mvn -pl tools/steady-state-monitor package
```

Run the monitor:

```
KALDB_BULK_URL=http://localhost:8086/_bulk \
KALDB_QUERY_URL=http://localhost:8081/_msearch \
INDEX=logs \
METRICS_PORT=9464 \
java -jar tools/steady-state-monitor/target/tools-steady-state-monitor.jar
```

Important metrics:

```
kaldb_steady_state_window_ready
kaldb_steady_state_bucket_expected_docs{bucket_age_minutes="1"}
kaldb_steady_state_bucket_observed_docs{bucket_age_minutes="1"}
kaldb_steady_state_bucket_doc_delta{bucket_age_minutes="1"}
kaldb_steady_state_bucket_doc_ratio{bucket_age_minutes="1"}
kaldb_steady_state_window_min_ratio
kaldb_steady_state_window_max_abs_delta_docs
```

The monitor warms up over `WINDOW_BUCKETS` minutes before
`kaldb_steady_state_window_ready` becomes `1`, because it waits until every
completed bucket in the rolling window has locally expected docs to compare
against query results.

Span generator tool
===================
spangen is a cli tool that can generate spans from the command line. 

```
> ./spangen  -id 100 -trace-id 200 -start-micros 1234 -duration-micros 5 -string-tag test:1234 -int-tag count:123 -dataset test_dataset
{
    "id": "MTAw",
    "trace_id": "MjAw",
    "timestamp": 1234,
    "duration": 5,
    "tags": [
        {
            "key": "test",
            "v_str": "1234"
        },
        {
            "key": "count",
            "v_type": 2,
            "v_int64": 123
        },
        {
            "key": "__dataset",
            "v_str": "test_dataset"
        }
    ]
}
```

Sample run:

```
> ./spangen --help
Usage of ./spangen:
  -bool-tag value
    	tag formatted as key:value, where value is true or false (repeatable)
  -dataset string
    	set a dataset tag
  -duration-micros int
    	duration of event in microseconds (required)
  -float-tag value
    	tag formatted as key:value, where value is a float (repeatable)
  -id string
    	span id (required unless using -one-off)
  -int-tag value
    	tag formatted as key:value, where value is an integer (repeatable)
  -name string
    	name for the event
  -one-off
    	use a random span id and trace id
  -parent-id string
    	parent id (leave empty for root span)
  -reporter string
    	tell the generator how to report spans; allowed reporters: noop console (default "console")
  -start-micros int
    	start of event in microseconds since epoch (required)
  -string-tag value
    	tag formatted as key:value, where value is a string (repeatable)
  -trace-id string
    	trace id (required unless using -one-off)
  -verbose
    	print results as json object
  -version
    	print version and quit
```
