package com.slack.astra.otlp;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.schema.Schema;
import com.slack.service.murron.trace.Trace;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpTraceRequestParserTest {
  private static final String TRACE_DATASET = "otel_traces";
  private static final byte[] TRACE_ID =
      new byte[] {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
      };
  private static final byte[] SPAN_ID = new byte[] {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};
  private static final byte[] PARENT_SPAN_ID =
      new byte[] {0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27};
  private static final long START_NANOS = 1_700_000_000_123_456_789L;
  private static final long END_NANOS = START_NANOS + 432_100_000L;

  private static ExportTraceServiceRequest requestWithSpan(String serviceName, String spanName) {
    return ExportTraceServiceRequest.newBuilder()
        .addResourceSpans(
            ResourceSpans.newBuilder()
                .setResource(
                    Resource.newBuilder()
                        .addAttributes(stringAttribute("service.name", serviceName))
                        .addAttributes(stringAttribute("deployment.environment", "demo")))
                .addScopeSpans(
                    ScopeSpans.newBuilder()
                        .setScope(
                            InstrumentationScope.newBuilder()
                                .setName("io.opentelemetry.test")
                                .setVersion("1.2.3")
                                .addAttributes(stringAttribute("scope-key", "scope-value")))
                        .addSpans(span(spanName))))
        .build();
  }

  private static Span span(String spanName) {
    return Span.newBuilder()
        .setTraceId(ByteString.copyFrom(TRACE_ID))
        .setSpanId(ByteString.copyFrom(SPAN_ID))
        .setParentSpanId(ByteString.copyFrom(PARENT_SPAN_ID))
        .setName(spanName)
        .setKind(Span.SpanKind.SPAN_KIND_SERVER)
        .setStartTimeUnixNano(START_NANOS)
        .setEndTimeUnixNano(END_NANOS)
        .addAttributes(stringAttribute("http.method", "GET"))
        .addAttributes(booleanAttribute("success", true))
        .addAttributes(intAttribute("retries", 7))
        .addAttributes(doubleAttribute("duration.double", 12.5))
        .build();
  }

  private static KeyValue stringAttribute(String key, String value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setStringValue(value))
        .build();
  }

  private static KeyValue booleanAttribute(String key, boolean value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setBoolValue(value))
        .build();
  }

  private static KeyValue intAttribute(String key, long value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setIntValue(value))
        .build();
  }

  private static KeyValue doubleAttribute(String key, double value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setDoubleValue(value))
        .build();
  }

  private static Trace.KeyValue tag(Trace.Span span, String key) {
    return span.getTagsList().stream()
        .filter(keyValue -> keyValue.getKey().equals(key))
        .findFirst()
        .orElseThrow();
  }

  @Test
  void parsesBinaryOtlpTraceRequest() throws Exception {
    Map<String, List<Trace.Span>> spansByDataset =
        OtlpTraceRequestParser.parseProtobuf(
            requestWithSpan("frontend", "GET /api").toByteArray(),
            TRACE_DATASET,
            Schema.IngestSchema.getDefaultInstance());

    assertThat(spansByDataset.keySet()).containsExactly(TRACE_DATASET);
    assertThat(spansByDataset.get(TRACE_DATASET)).hasSize(1);

    Trace.Span span = spansByDataset.get(TRACE_DATASET).get(0);
    assertThat(span.getTraceId().toStringUtf8()).isEqualTo("000102030405060708090a0b0c0d0e0f");
    assertThat(span.getId().toStringUtf8()).isEqualTo("1011121314151617");
    assertThat(span.getParentId().toStringUtf8()).isEqualTo("2021222324252627");
    assertThat(span.getName()).isEqualTo("GET /api");
    assertThat(span.getTimestamp()).isEqualTo(START_NANOS / 1_000);
    assertThat(span.getDuration()).isEqualTo((END_NANOS - START_NANOS) / 1_000);

    assertThat(tag(span, LogMessage.ReservedField.SERVICE_NAME.fieldName).getVStr())
        .isEqualTo(TRACE_DATASET);
    assertThat(tag(span, "resource.service.name").getVStr()).isEqualTo("frontend");
    assertThat(tag(span, "resource.deployment.environment").getVStr()).isEqualTo("demo");
    assertThat(tag(span, "scope.name").getVStr()).isEqualTo("io.opentelemetry.test");
    assertThat(tag(span, "scope.version").getVStr()).isEqualTo("1.2.3");
    assertThat(tag(span, "scope.scope-key").getVStr()).isEqualTo("scope-value");
    assertThat(tag(span, "http.method").getVStr()).isEqualTo("GET");
    assertThat(tag(span, "success").getVBool()).isTrue();
    assertThat(tag(span, "retries").getVInt64()).isEqualTo(7);
    assertThat(tag(span, "duration.double").getVFloat64()).isEqualTo(12.5);
  }

  @Test
  void parsesJsonOtlpTraceRequest() throws Exception {
    String json = JsonFormat.printer().print(requestWithSpan("frontend", "GET /api"));

    Map<String, List<Trace.Span>> spansByDataset =
        OtlpTraceRequestParser.parseJson(
            json, TRACE_DATASET, Schema.IngestSchema.getDefaultInstance());

    assertThat(spansByDataset.keySet()).containsExactly(TRACE_DATASET);
    assertThat(spansByDataset.get(TRACE_DATASET)).hasSize(1);
    assertThat(spansByDataset.get(TRACE_DATASET).get(0).getTraceId().toStringUtf8())
        .isEqualTo("000102030405060708090a0b0c0d0e0f");
  }

  @Test
  void routesAllServicesToConfiguredTraceDataset() throws Exception {
    ExportTraceServiceRequest request =
        ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(requestWithSpan("frontend", "GET /api").getResourceSpans(0))
            .addResourceSpans(requestWithSpan("checkout", "POST /checkout").getResourceSpans(0))
            .build();

    Map<String, List<Trace.Span>> spansByDataset =
        OtlpTraceRequestParser.parseProtobuf(
            request.toByteArray(), TRACE_DATASET, Schema.IngestSchema.getDefaultInstance());

    assertThat(spansByDataset.keySet()).containsExactly(TRACE_DATASET);
    assertThat(spansByDataset.get(TRACE_DATASET))
        .extracting(span -> tag(span, "resource.service.name").getVStr())
        .containsExactly("frontend", "checkout");
    assertThat(spansByDataset.get(TRACE_DATASET))
        .allSatisfy(
            span ->
                assertThat(tag(span, LogMessage.ReservedField.SERVICE_NAME.fieldName).getVStr())
                    .isEqualTo(TRACE_DATASET));
  }

  @Test
  void usesDefaultDatasetNameWhenConfiguredNameIsBlank() throws Exception {
    Map<String, List<Trace.Span>> spansByDataset =
        OtlpTraceRequestParser.parseProtobuf(
            requestWithSpan("frontend", "GET /api").toByteArray(),
            " ",
            Schema.IngestSchema.getDefaultInstance());

    assertThat(spansByDataset.keySet())
        .containsExactly(OtlpTraceRequestParser.DEFAULT_TRACE_DATASET_NAME);
    assertThat(
            tag(
                    spansByDataset.get(OtlpTraceRequestParser.DEFAULT_TRACE_DATASET_NAME).get(0),
                    LogMessage.ReservedField.SERVICE_NAME.fieldName)
                .getVStr())
        .isEqualTo(OtlpTraceRequestParser.DEFAULT_TRACE_DATASET_NAME);
  }
}
