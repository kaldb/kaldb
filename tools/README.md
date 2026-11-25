Synthetic data probe
====================

The synthetic data probe is packaged inside `astra.jar`. It continuously writes
synthetic documents through `_bulk`, queries those documents back through
`_msearch`, and exposes Prometheus metrics on `/metrics`.

See [Synthetic Data Probe](../docs/topics/Synthetic-data-probe.md) for the
design motivation and production monitoring model.

Run it from a built KalDB image or local jar with:

```bash
java --enable-preview -cp /astra.jar com.slack.astra.tools.syntheticdataprobe.SyntheticDataProbeMain
```

Useful environment variables:

```bash
KALDB_BULK_URL=http://ingest:8086/_bulk
KALDB_QUERY_URL=http://query:8081/_msearch
INDEX=logs
SYNTHETIC_DATA_PROBE_RUN_ID=synthetic
TARGET_HOSTNAME=synthetic-data-probe.target
DISTRACTOR_HOSTNAME=synthetic-data-probe.other
METRICS_PORT=9464
WINDOW_BUCKETS=15
NEWEST_BUCKET_AGE_MINUTES=3
MAX_CATCHUP_BUCKETS_PER_CYCLE=3
```

`WINDOW_BUCKETS` controls how many one-minute buckets are checked.
`NEWEST_BUCKET_AGE_MINUTES` shifts that checked range back from the active
ingest bucket. For example, `WINDOW_BUCKETS=15` and
`NEWEST_BUCKET_AGE_MINUTES=3` checks buckets 3-17 minutes old. With the Helm
chart defaults, this favors a faster post-redeploy signal while avoiding the
freshest bucket, which can still be catching up in query visibility. The deployed
chart's current index chunk limits usually roll synthetic probe data by byte
size well before the 90-minute wall-clock chunk limit.

`MAX_CATCHUP_BUCKETS_PER_CYCLE` caps how many missed minute buckets one ingest
cycle can fill after a slow cycle or temporary outage. The probe keeps draining
the backlog over later cycles rather than dropping missed buckets.

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
