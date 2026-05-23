# OTLP HTTP Traces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `POST /v1/traces` to the preprocessor so OTLP/HTTP trace exports are converted into existing KalDB trace spans and written to Kafka.

**Architecture:** Add a small OTLP trace parser/converter and a dedicated annotated Armeria service. The service parses binary or JSON OTLP requests, maps all spans to the configurable logical dataset name defaulting to `otel_traces`, reuses the existing dataset rate limiter and Kafka producer, and returns OTLP response payloads.

**Tech Stack:** Java 21, Armeria annotated services, Micrometer, Protocol Buffers, OpenTelemetry proto Java classes, existing KalDB `Trace.Span` ingestion path.

---

### Task 1: Add OTLP Proto Dependency And Config Field

**Files:**
- Modify: `astra/pom.xml`
- Modify: `astra/src/main/proto/astra_configs.proto`
- Modify: `config/config.yaml`
- Modify: `docs/topics/Config-options.md`
- Modify: `astra/src/test/resources/test_config.yaml`
- Modify: `astra/src/test/resources/test_config.json`

- [ ] **Step 1: Write the failing config test**

Add assertions to `astra/src/test/java/com/slack/astra/server/AstraConfigTest.java` that expect:

```java
assertThat(config.getPreprocessorConfig().getOtlpTraceDatasetName()).isEqualTo("otel_traces");
```

and, for parsed fixture configs:

```java
assertThat(config.getPreprocessorConfig().getOtlpTraceDatasetName()).isEqualTo("custom_otel_traces");
```

- [ ] **Step 2: Run the config test to verify RED**

Run: `mvn -pl astra -Dtest=AstraConfigTest test`

Expected: compilation failure because `getOtlpTraceDatasetName()` is not defined.

- [ ] **Step 3: Add the config field**

Add this field to `PreprocessorConfig` in `astra/src/main/proto/astra_configs.proto`:

```proto
  // Logical dataset name used by the OTLP/HTTP /v1/traces endpoint.
  // All spans received through OTLP traces are written using this dataset key.
  // Provision this dataset with serviceNamePattern: _all so it can accept spans from every service.
  string otlp_trace_dataset_name = 15;
```

Add this to `config/config.yaml` under `preprocessorConfig`:

```yaml
  otlpTraceDatasetName: ${ASTRA_PREPROCESSOR_OTLP_TRACE_DATASET_NAME:-otel_traces}
```

Add fixture values to `astra/src/test/resources/test_config.yaml` and `astra/src/test/resources/test_config.json`:

```yaml
  otlpTraceDatasetName: custom_otel_traces
```

```json
"otlpTraceDatasetName": "custom_otel_traces"
```

Document the option in `docs/topics/Config-options.md`:

````markdown
### otlpTraceDatasetName

```yaml
preprocessorConfig:
  otlpTraceDatasetName: otel_traces
```

Logical dataset name used by the OTLP/HTTP `/v1/traces` endpoint. The dataset must be provisioned with `serviceNamePattern: _all`.
````

- [ ] **Step 4: Add the OpenTelemetry proto Java dependency**

Add a property and dependency to `astra/pom.xml`:

```xml
<opentelemetry.proto.version>1.10.0</opentelemetry.proto.version>
```

```xml
<dependency>
    <groupId>io.opentelemetry.proto</groupId>
    <artifactId>opentelemetry-proto</artifactId>
    <version>${opentelemetry.proto.version}</version>
</dependency>
```

- [ ] **Step 5: Run the config test to verify GREEN**

Run: `mvn -pl astra -Dtest=AstraConfigTest test`

Expected: pass.

### Task 2: Add OTLP Trace Converter

**Files:**
- Create: `astra/src/main/java/com/slack/astra/otlp/OtlpTraceRequestParser.java`
- Test: `astra/src/test/java/com/slack/astra/otlp/OtlpTraceRequestParserTest.java`

- [ ] **Step 1: Write failing parser tests**

Create tests that build `ExportTraceServiceRequest` objects with:

- one resource span containing `service.name=frontend`;
- one scope attribute;
- one span with trace id, span id, parent span id, start/end timestamps, name, and primitive attributes;
- two resource spans with different `service.name` values.

Assert that `parseProtobuf(...)` and `parseJson(...)` return `Map.of("otel_traces", spans)` and that:

```java
span.getTraceId().toByteArray()
span.getId().toByteArray()
span.getParentId().toByteArray()
span.getName()
span.getTimestamp()
span.getDuration()
```

match the OTLP input. Assert that the tags include:

```java
service_name = otel_traces
resource.service.name = frontend
scope.scope-key = scope-value
http.method = GET
```

- [ ] **Step 2: Run parser tests to verify RED**

Run: `mvn -pl astra -Dtest=OtlpTraceRequestParserTest test`

Expected: compilation failure because `OtlpTraceRequestParser` does not exist.

- [ ] **Step 3: Implement minimal parser**

Implement `OtlpTraceRequestParser` with:

```java
public static final String DEFAULT_TRACE_DATASET_NAME = "otel_traces";

public static Map<String, List<Trace.Span>> parseProtobuf(
    byte[] body, String traceDatasetName, Schema.IngestSchema schema)

public static Map<String, List<Trace.Span>> parseJson(
    String body, String traceDatasetName, Schema.IngestSchema schema)
```

Use `ExportTraceServiceRequest.parseFrom(body)` for protobuf and `JsonFormat.parser().merge(...)` for JSON.

For each span, set:

- `Trace.Span.trace_id` from OTLP `trace_id`;
- `Trace.Span.id` from OTLP `span_id`;
- `Trace.Span.parent_id` when non-empty;
- `Trace.Span.name` from OTLP span name;
- `Trace.Span.timestamp` as `start_time_unix_nano / 1_000`;
- `Trace.Span.duration` as `max(0, end-start) / 1_000`.

Convert attributes using existing `SpanFormatter.convertKVtoProto(...)` after translating OTLP `AnyValue` into Java primitive/string values.

- [ ] **Step 4: Run parser tests to verify GREEN**

Run: `mvn -pl astra -Dtest=OtlpTraceRequestParserTest test`

Expected: pass.

### Task 3: Add `/v1/traces` API Service

**Files:**
- Create: `astra/src/main/java/com/slack/astra/otlp/OtlpTraceIngestApi.java`
- Test: `astra/src/test/java/com/slack/astra/otlp/OtlpTraceIngestApiTest.java`

- [ ] **Step 1: Write failing API tests**

Create tests that instantiate `OtlpTraceIngestApi` with a real `BulkIngestKafkaProducer`, `DatasetRateLimitingService`, and test Kafka server. Provision dataset name `otel_traces` with `serviceNamePattern: _all`.

Assert:

- `application/x-protobuf` request returns `200 OK` and writes one Kafka span keyed by `otel_traces`;
- `application/json` request returns `200 OK`;
- unsupported content type returns `415 Unsupported Media Type`;
- malformed protobuf returns `400 Bad Request`;
- low throughput returns the configured rate limit status code.

- [ ] **Step 2: Run API tests to verify RED**

Run: `mvn -pl astra -Dtest=OtlpTraceIngestApiTest test`

Expected: compilation failure because `OtlpTraceIngestApi` does not exist.

- [ ] **Step 3: Implement API service**

Implement `OtlpTraceIngestApi` with:

```java
@Post("/v1/traces")
public HttpResponse ingestTraces(AggregatedHttpRequest request)
```

Behavior:

- increment incoming byte counter from request body length;
- parse protobuf for `application/x-protobuf`;
- parse JSON for `application/json`;
- reject unsupported content type with `415`;
- reject parse errors with `400`;
- call `datasetRateLimitingService.tryAcquire(traceDatasetName, spans)`;
- call `bulkIngestKafkaProducer.submitRequest(docs).getResponse()` on a virtual thread;
- return empty `ExportTraceServiceResponse` in protobuf or JSON using the request content type;
- return `500` for producer errors.

- [ ] **Step 4: Run API tests to verify GREEN**

Run: `mvn -pl astra -Dtest=OtlpTraceIngestApiTest test`

Expected: pass.

### Task 4: Wire API Into Preprocessor Startup

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/server/Astra.java`
- Test: `astra/src/test/java/com/slack/astra/server/AstraConfigTest.java`

- [ ] **Step 1: Write failing wiring assertion**

Extend config tests to assert blank `otlpTraceDatasetName` falls back to `otel_traces` at service construction time if the field is omitted.

- [ ] **Step 2: Run targeted tests to verify RED or existing gap**

Run: `mvn -pl astra -Dtest=AstraConfigTest,OtlpTraceIngestApiTest test`

Expected: fail until the fallback/wiring is implemented.

- [ ] **Step 3: Wire service into `Astra.java`**

After constructing `BulkIngestApi`, add:

```java
armeriaServiceBuilder.withAnnotatedService(
    new OtlpTraceIngestApi(
        bulkIngestKafkaProducer,
        datasetRateLimitingService,
        meterRegistry,
        preprocessorConfig.getRateLimitExceededErrorCode(),
        schema,
        preprocessorConfig.getOtlpTraceDatasetName()));
```

Normalize blank dataset names inside `OtlpTraceIngestApi` to `OtlpTraceRequestParser.DEFAULT_TRACE_DATASET_NAME`.

- [ ] **Step 4: Run targeted tests to verify GREEN**

Run: `mvn -pl astra -Dtest=AstraConfigTest,OtlpTraceRequestParserTest,OtlpTraceIngestApiTest test`

Expected: pass.

### Task 5: Final Verification And Cleanup

**Files:**
- All changed files

- [ ] **Step 1: Format and compile**

Run: `mvn -pl astra -DskipTests compile`

Expected: build success.

- [ ] **Step 2: Run focused test suite**

Run: `mvn -pl astra -Dtest=AstraConfigTest,OtlpTraceRequestParserTest,OtlpTraceIngestApiTest,BulkIngestApiTest test`

Expected: pass.

- [ ] **Step 3: Review diff**

Run: `git diff --check` and `git diff --stat`.

Expected: no whitespace errors; changes scoped to OTLP traces, config, docs, and tests.
