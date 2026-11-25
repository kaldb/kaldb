# ADR: Native OTLP/HTTP trace ingestion

## Status

Current state: `Draft`

Discussion thread: https://github.com/kaldb/kaldb/pull/55

Issue: https://github.com/kaldb/kaldb/issues/76

PR: https://github.com/kaldb/kaldb/pull/55

Supersedes: n/a

Superseded by: n/a

## Motivation

OpenTelemetry Collectors should be able to export traces directly to the KalDB preprocessor.
The existing bulk-ingest path already provides dataset rate limiting and Kafka partition routing;
an OTLP receiver can reuse it after converting spans to KalDB's trace format.

## Questions

- Why configure a destination dataset? OTLP export requests do not identify a KalDB dataset and
  may contain many producing services. One configured destination lets an operator provision the
  dataset independently of those service names, without rebuilding KalDB to change its name.
- Does this require OpenSearch 2.11? The receiver does not call an OpenSearch server. It shares
  KalDB's existing ingest components and does not change the project's OpenSearch library version.

## Public Interfaces

- `POST /v1/traces` accepts OTLP protobuf and protobuf JSON requests on the preprocessor.
- `preprocessorConfig.otlpTraceDatasetName` selects the destination dataset. The sample config
  exposes `ASTRA_PREPROCESSOR_OTLP_TRACE_DATASET_NAME`; omitted or blank values use `otel_traces`.
  Leading and trailing whitespace is removed. Protobuf tag `15` identifies this config field.
- The preprocessor records separate OTLP byte/span/error counters and ingest latency.

## Proposed Changes

Parse each request with the OpenTelemetry protobuf types and convert its spans into the existing
`Trace.Span` representation. Preserve trace/span/parent identities as hexadecimal strings and
convert nanosecond timestamps and durations into KalDB's microseconds. Keep resource and scope
attributes under their respective prefixes.

Route every request to the configured dataset key. Store that key as the span's `service_name`
and retain its producing service as `resource.service.name`. Consequently, the dataset's
`serviceNamePattern` can equal the configured key or use `_all`.

Apply dataset rate limiting and submit through `BulkIngestKafkaProducer`. Charge at least one
byte for each empty protobuf span. The handler uses Armeria's blocking executor through
`@Blocking` while waiting for the producer result; it does not create a virtual thread per request.

Keep the native Dashboards adapter, Collector orchestration, and Grafana provisioning in their
separate gateway PR and demo repository. This receiver supports traces over HTTP; other OTLP
signals, gRPC, and per-request dataset selection are outside this change.

## Compatibility, Deprecation, and Migration Plan

Existing bulk ingestion and stored trace formats are unchanged. Provision the configured trace
dataset and its Kafka assignments before exporting spans. The astronomy demo uses
`otel-demo-traces`; operators can choose a different name through config.

## Test Plan

Verify protobuf/JSON conversion, hexadecimal IDs, multiple producing services sharing a dataset,
blank and whitespace-padded config values, malformed/unsupported requests, Kafka delivery,
rate limiting, empty-span charging, and the blocking-executor annotation. Run the existing
preprocessor/config tests and project build checks.

## Documentation Plan

Document the dataset setting and provisioning semantics in `docs/topics/Config-options.md` and
keep sample/test configs aligned with the protobuf field. Keep local execution plans and notes
ignored rather than committing them.

## Rejected Alternatives

- Hardcode one dataset name: operators would need a different build to change routing.
- Derive the destination from `resource.service.name`: one multi-service trace would be spread
  across service-specific datasets and require separate provisioning/rate-limit policies.
- Bridge OTLP through an OpenSearch exporter: the older upstream prototype used this approach,
  but a native receiver can preserve the trace representation and reuse Kafka ingestion directly.

## Consequences

Collectors can use the standard OTLP/HTTP endpoint, and deployment configuration controls one
destination trace dataset. Multi-dataset routing would require a separate API/design decision;
this setting does not provide tenant authorization or Data Prepper trace-group enrichment.
