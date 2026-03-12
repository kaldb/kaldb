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
