# Testing OpenSearch Dashboards On This Branch

This branch now has two different kinds of committed Dashboards UI testing:

1. a committed, self-contained Dashboards UI check for exact-match data-view creation
2. a committed full Dashboards E2E harness that covers data-view creation, Discover, and a dashboard aggregation check

## Current Scope

What is portable and repeatable from a clean checkout:

- exact-match Dashboards data-view creation through the real UI
- deletion of an existing Dashboards `index-pattern` saved object before recreating it
- a precheck against Dashboards status and exact-name index resolution
- full Dashboards E2E coverage for fixture ingest, data-view creation, Discover, and dashboard aggregations

## Native OpenTelemetry trace demo

The optional gateway adapter enables the **Trace analytics → Traces** list and native
trace detail page, including the span timeline, service timing breakdown, payload,
and span inspection. Searches and aggregations run against the existing KalDB
`otel-demo-traces` dataset. OpenSearch continues to hold Dashboards saved objects;
it does not need a copy of the spans or Data Prepper.

First run the OpenTelemetry demo with its KalDB OTLP exporter and load generator.
The KalDB preprocessor must provide `/v1/traces`, and `otel-demo-traces` must already
contain spans. The running demo uses the OTLP-enabled KalDB image configured by
the OpenTelemetry demo project; this gateway change does not add an OTLP receiver
to the base KalDB image on this branch.

Build and start the gateway, keeping any existing runtime/image overrides before
the trace override in your Compose command:

```bash
docker compose -f docker-compose.yml \
  -f config/docker-compose.trace-analytics.yaml build astra_dashboards_gateway
docker compose -f docker-compose.yml \
  -f config/docker-compose.trace-analytics.yaml up -d --no-deps astra_dashboards_gateway
python3 scripts/verify-trace-analytics-demo.py
```

The verifier prints a direct trace URL. It exercises the same Dashboards HTTP
routes as the installed 2.11.1 plugin, checks complete span retrieval and parent
links across at least three services, validates timing units and span lookup,
and checks that the backing OpenSearch cluster has no `otel*` indices. Pass
`--trace-id ID` to inspect a particular trace or `--trace-group NAME` to select
a different transaction. The default group is `user_browse_product`.

Open `http://localhost:5601/app/observability-traces#/traces`, select a trace,
and use **Timeline** or **Span list**. Click a timeline bar to inspect its span.
`TRACE_ANALYTICS_DATASET` in the override enables the mapping from the plugin's
`otel-v1-apm-span-*` index to an exact KalDB dataset. Leaving it unset preserves
the gateway's ordinary routing. It must be an exact dataset name without wildcards.

The adapter reuses `trace_id`, document `_id`, `parent_id`,
`resource.service.name`, `_timesinceepoch`, `duration` (microseconds), and
`status.code`. It returns the plugin's camel-case names, nanosecond durations,
and nanosecond start-time sort values. The plugin sends search size in the URL;
the adapter puts that limit into the KalDB search body so the timeline includes
all spans, up to the plugin's 3,000-span limit.

This is a trace-view demo, not a replacement for Data Prepper's full trace-group
enrichment. Overview values and trace-group filtering use root spans. Filtering
the trace list by a child service is not supported; service filters there match
the root service. Service topology is not generated, so the separate service-map
panel can show no matches. Source timestamps are preserved: clock skew in a
producing service stretches the timeline. In the current astronomy demo the
quote service can have a large clock offset; the verifier rejects skewed traces
and defaults to a browse-product transaction that does not call that service.

Gateway regression tests:

```bash
mvn -q -f contrib/dashboards-gateway/pom.xml test
```

## Gateway base images

`contrib/dashboards-gateway.Dockerfile` pins both Maven/Corretto and the Corretto runtime to
multi-platform image digests. The runtime explicitly selects Amazon Linux 2023. Rebuilding the
same Dockerfile therefore uses the same base images even when upstream tags move. This does not
make the entire build byte-for-byte reproducible; Maven dependencies and the application source
are separate inputs.

The runtime already includes `curl-minimal`, which supports the HTTP healthcheck. Do not install
the full `curl` package alongside it. The build checks that curl is available, and the gateway CI
job verifies startup and the health endpoint using curl both outside and inside the container.

Dependabot checks `/contrib` weekly for base-image updates. Review and merge those PRs to receive
Java and OS fixes; pinned digests do not update automatically. For manual updates, use
`docker buildx imagetools inspect <image>:<tag>` to obtain the multi-platform index digest, update
the corresponding `FROM` line, and run the gateway image build and healthcheck before merging.
Changes to the Java major version or Linux distribution should be deliberate.

## Preconditions

This document assumes you already have the local stack running and reachable at:

- Dashboards: `http://localhost:5601`
- Manager API: `http://localhost:8083`
- Bulk ingest health: `http://localhost:8086/health`

For browser automation, create a local virtualenv and install Selenium:

```bash
python3 -m venv .venv
.venv/bin/pip install selenium
```

## Find An Exact Dataset Name To Use

This branch is documented as exact-match only for Dashboards data-view creation. Use a real Astra
dataset name, not a wildcard pattern.

To list the currently provisioned dataset names:

```bash
curl -sS -XPOST \
  -H 'content-type: application/json; charset=utf-8; protocol=gRPC' \
  http://localhost:8083/slack.proto.astra.ManagerApiService/ListDatasetMetadata \
  -d '{}' \
  | jq -r '.datasetMetadata[].name'
```

Use one exact dataset name from that output as the `--index-pattern` value below. If your local
stack follows the usual examples, `test` is a common exact name.

## Repeatable Dashboards UI Check

For a visible, repeatable browser run of the Dashboards "Create index pattern" flow:

```bash
.venv/bin/python scripts/verify-dashboards-index-pattern-ui.py \
  --index-pattern test \
  --delete-existing \
  --slow-seconds 1 \
  --keep-open-seconds 5
```

What that script does:

1. checks `GET /api/status`
2. checks Dashboards exact-name resolution for the index you passed
3. optionally deletes an existing Dashboards `index-pattern` saved object with that exact title
4. opens `http://localhost:5601/app/management/opensearch-dashboards/indexPatterns/create`
5. enters the exact index name into the UI
6. clicks through the time-field step
7. confirms that Dashboards saved the resulting `index-pattern` object

Notes:

- the script defaults to a visible Chrome window; add `--headless` only if you want a CI-style run
- it only supports exact names, not wildcard patterns
- it expects the exact Astra dataset/index name to already exist
- if Selenium cannot start Chrome automatically, pass `--chrome-binary` and `--chromedriver`

Useful variants:

```bash
.venv/bin/python scripts/verify-dashboards-index-pattern-ui.py \
  --index-pattern test \
  --delete-existing \
  --headless
```

```bash
.venv/bin/python scripts/verify-dashboards-index-pattern-ui.py \
  --precheck-only \
  --index-pattern test
```

## Exact-Match Behavior

The intended behavior on this branch is exact-match only.

What that means in practice:

- `test` is a valid data-view name if there is an exact dataset named `test`
- `logs-*` is not supported
- `ui-e2e-*` is not supported

The narrow Selenium helper above is aligned with that model and will only verify exact names.

## Full Dashboards E2E Harness

If you want one command that covers the Dashboards user path from data-view creation through
Discover and dashboard aggregations:

```bash
.venv/bin/python scripts/verify-dashboards-full-ui-e2e.py
```

If you want the script to start the local stack first:

```bash
.venv/bin/python scripts/verify-dashboards-full-ui-e2e.py --start
```

If you want a clean rebuild first:

```bash
.venv/bin/python scripts/verify-dashboards-full-ui-e2e.py --clean
```

What that script does:

1. ensures the exact-match `test` and `ui-test` Astra datasets exist
2. ingests the committed smoke fixture
3. verifies the Dashboards gateway returns only `test` hits for `POST /test/_search`
4. recreates the `test` Dashboards data view through the UI
5. opens Discover in the browser and verifies:
   - `Test dataset log one`
   - `Test dataset log two`
   - no `UI test dataset ...` leakage
6. creates a dashboard backed by the UI-created data view
7. opens that dashboard in the browser and verifies aggregation output is rendered

This flow is exact-match only. It is the closest thing in the repo to a committed customer-path
acceptance test for Dashboards on this branch.
