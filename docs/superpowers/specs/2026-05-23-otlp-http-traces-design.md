# OTLP HTTP Trace Ingestion Design

## Goal

Add OTLP/HTTP trace ingestion to the preprocessor so an OpenTelemetry Collector can export spans to
KalDB with `POST /v1/traces`. This first pass supports traces only. OTLP logs, metrics, profiles,
and OTLP/gRPC are out of scope.

## Architecture

The preprocessor will register a dedicated annotated Armeria service, `OtlpTraceIngestApi`, alongside
the existing OpenSearch bulk ingest API. The new service will:

- Accept `POST /v1/traces`.
- Parse OTLP `ExportTraceServiceRequest` request bodies.
- Convert each OTLP span into the existing `com.slack.service.murron.trace.Trace.Span` proto.
- Reuse `BulkIngestKafkaProducer` for Kafka writes.
- Reuse `DatasetRateLimitingService` for per-dataset rate limiting.

This keeps OTLP parsing and mapping isolated from OpenSearch bulk parsing while preserving the
existing Kafka and indexing path.

The implementation will add a preprocessor config field for the logical OTLP trace dataset name.
The default will be `traces`, and operators can override it when they provision a differently named
trace dataset.

## Request Formats

The endpoint will support these content types:

- `application/x-protobuf`: binary OTLP protobuf.
- `application/json`: OTLP protobuf JSON.

Requests with unsupported content types will return `415 Unsupported Media Type`. Malformed OTLP
payloads will return `400 Bad Request`.

## Dataset Selection

All OTLP trace spans are assigned to the configured logical trace dataset name. The default name is
`traces`, but the name is configurable so deployments can use any dataset name.

The configured logical trace dataset must be provisioned with `serviceNamePattern: _all`. That
lets the existing producer map the logical trace dataset key to the dataset partitions without
routing spans by the OpenTelemetry resource `service.name`.

OpenTelemetry `service.name` remains searchable metadata, but it is not used for dataset selection.
The converter stores it under `resource.service.name`.

OTLP trace ingestion will always submit a single Kafka producer entry keyed by the configured trace
dataset name. This differs from OpenSearch bulk ingestion, where each bulk index key can represent a
different dataset.

## Span Mapping

For each OTLP span:

- `trace_id` maps to `Trace.Span.trace_id` as raw bytes.
- `span_id` maps to `Trace.Span.id` as raw bytes.
- `parent_span_id` maps to `Trace.Span.parent_id` as raw bytes when present.
- `name` maps to `Trace.Span.name`.
- `start_time_unix_nano` maps to `Trace.Span.timestamp` in epoch microseconds.
- Duration is `end_time_unix_nano - start_time_unix_nano`, converted to microseconds. If the end
  time is missing or earlier than the start time, duration is set to `0`.

The converter will add reserved tags so existing query and Zipkin compatibility code can find the
same fields it already expects:

- `service_name`
- `trace_id`
- `id`
- `parent_id`
- `name`
- `duration`

Resource attributes, scope attributes, and span attributes will also become KalDB tags. Attribute
names are preserved, except resource attributes are prefixed with `resource.` and scope attributes
are prefixed with `scope.` to avoid collisions with span attributes.

The reserved `service_name` tag is set to the configured logical trace dataset name. KalDB currently
uses `service_name` as the logical index field for scoped OpenSearch-compatible searches, so this
keeps searches against the trace dataset name working. The original OpenTelemetry service name is
preserved as `resource.service.name`.

OTLP attribute values map to KalDB schema field types as follows:

- string values become `KEYWORD`.
- boolean values become `BOOLEAN`.
- integer values become `LONG`.
- double values become `DOUBLE`.
- byte values become `BINARY`.
- arrays and key-value lists become stringified `KEYWORD` values for this first pass.

When a tag key is present in the configured ingest schema, existing schema conversion logic will be
used so field type and multi-field behavior stays consistent with OpenSearch bulk ingest.

## Response Behavior

On success, the endpoint will return `200 OK` with an empty OTLP
`ExportTraceServiceResponse` body encoded using the same response format as the request.

If the configured trace dataset exceeds its rate limit, the endpoint will return the configured
preprocessor rate-limit status code and will not write the request.

If the Kafka producer reports a write failure, the endpoint will return `500 Internal Server Error`.

## Metrics

The endpoint will expose trace-specific counters and timers:

- incoming OTLP trace bytes.
- incoming OTLP spans.
- OTLP trace ingest errors.
- OTLP trace ingest latency.

These metrics will be separate from the existing OpenSearch bulk ingest metrics.

## Tests

Unit tests will cover:

- binary protobuf request parsing.
- JSON protobuf request parsing.
- configured trace dataset selection.
- resource `service.name` preservation as `resource.service.name`.
- trace id, span id, parent id, timestamp, duration, and name mapping.
- resource, scope, and span attribute tag mapping.
- malformed payload and unsupported content type errors.

API-level tests will cover:

- successful `/v1/traces` ingestion through the preprocessor service.
- rate-limit rejection behavior.
- multi-service requests written under one configured trace dataset entry.

## Operational Follow-Up

The OpenTelemetry demo collector can be pointed at KalDB by adding an OTLP/HTTP exporter with an
endpoint such as `http://astra_preprocessor:8086` and adding that exporter to the traces pipeline.
The collector will send trace export requests to `/v1/traces`.

The KalDB deployment must also provision a trace dataset using the configured trace dataset name
and `serviceNamePattern: _all`.
