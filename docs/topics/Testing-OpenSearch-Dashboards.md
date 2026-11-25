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

## Native Security demo (OpenSearch 2.11.1)

The paired OTel demo on `codex/security-demo-2.11-review` contains a two-user,
two-tenant security showcase at `local/kaldb/permissions/README.md`:
https://github.com/kaldb/opentelemetry-demo/tree/codex/security-demo-2.11-review/local/kaldb/permissions

Build this checkout's gateway with `mvn -f contrib/dashboards-gateway/pom.xml package`.
Its optional secured mode uses these environment variables:

| Variable | Meaning |
| --- | --- |
| `SECURED_MODE=true` | Authenticate and authorize data requests using native Security. |
| `OPENSEARCH_URL` | Private HTTPS native OpenSearch authority, with Basic authentication enabled. |
| `OPENSEARCH_CA_FILE` | PEM CA file used to verify that authority. |
| `POLICY_READER_CREDENTIALS_FILE` | Private JSON file with `username` and `password`; never expose it to Dashboards users. |
| `REGISTERED_DATASETS` | Comma-separated concrete KalDB dataset names, each mirrored by an empty native schema index (maximum 32). |
| `ASTRA_URL` | Private KalDB query endpoint. It must not be reachable by user-facing clients. |

The policy reader can GET native roles and complete mappings for registered
schema indexes. It cannot search telemetry, change roles, or access other Security
administration endpoints. Native user probes establish index/field access on
each request; there is no authorization cache. Real native Security also governs
saved-object tenants. Saved-object scroll and tenant-template bootstrap require
the authenticated native `kibana_server` role and remain on OpenSearch.

The bounded query grammar supports search/multi-search, selected aggregations,
and filtered mapping/field-capability/index discovery. It rejects whole indexes
with multi-fields or aliases; FLS must contain literal negative leaf exclusions.
DLS, masking, parent/include/wildcard/regex FLS, scripts, runtime fields, data
scroll/PIT, SQL/PPL, and other unsupported operations fail closed. Only the empty
`script_fields` placeholder emitted by Dashboards is accepted and removed.
Metadata and returned documents are projected to visible leaves. The supplied
demo verifies real KalDB fixtures and zero upstream calls for denied requests.

`post_filter` and query-string operator/options are rejected because the bounded
backend path cannot preserve their semantics. Literal query strings support only
`query`, `fields`, and `default_field`. URL `fields` is accepted and authorized
only for field capabilities. Matching uses a work-limited glob, exact field names
use set lookup, and each multi-search shares selector/query-expansion budgets.
The demo limits schemas and field selectors/query nodes to 2,048 and expanded
text clauses to 4,096. Discovery accepts up to 32 index selectors of 128 characters.
Native roles must include complete typed cluster/index/tenant permission arrays,
with index patterns, allowed actions, FLS, and masking arrays on each index entry.

The default mode remains the existing routing demo. Use `SECURED_MODE=true` and
the private network layout for permission enforcement; the legacy demo's
synthetic account responses are not an authorization mechanism.
