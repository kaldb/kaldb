package com.slack.astra.otlp;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.writer.SpanFormatter;
import com.slack.service.murron.trace.Trace;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;

/** Converts OTLP trace export requests into KalDB trace spans. */
public final class OtlpTraceRequestParser {
  public static final String DEFAULT_TRACE_DATASET_NAME = "otel_traces";

  private static final String RESOURCE_PREFIX = "resource.";
  private static final String SCOPE_PREFIX = "scope.";
  private static final String SERVICE_NAME = LogMessage.ReservedField.SERVICE_NAME.fieldName;

  private OtlpTraceRequestParser() {}

  /**
   * Parses an OTLP protobuf trace export request into spans keyed by the configured trace dataset.
   */
  public static Map<String, List<Trace.Span>> parseProtobuf(
      byte[] postBody, String traceDatasetName, Schema.IngestSchema schema)
      throws InvalidProtocolBufferException {
    return convert(ExportTraceServiceRequest.parseFrom(postBody), traceDatasetName, schema);
  }

  /** Parses an OTLP JSON trace export request into spans keyed by the configured trace dataset. */
  public static Map<String, List<Trace.Span>> parseJson(
      String postBody, String traceDatasetName, Schema.IngestSchema schema)
      throws InvalidProtocolBufferException {
    ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(postBody, builder);
    return convert(builder.build(), traceDatasetName, schema);
  }

  /** Returns the configured trace dataset name, falling back to the default when unset. */
  public static String normalizeTraceDatasetName(String traceDatasetName) {
    if (traceDatasetName == null || traceDatasetName.isBlank()) {
      return DEFAULT_TRACE_DATASET_NAME;
    }
    return traceDatasetName;
  }

  private static Map<String, List<Trace.Span>> convert(
      ExportTraceServiceRequest request, String traceDatasetName, Schema.IngestSchema schema) {
    String normalizedDatasetName = normalizeTraceDatasetName(traceDatasetName);
    List<Trace.Span> spans = new ArrayList<>();

    for (ResourceSpans resourceSpans : request.getResourceSpansList()) {
      List<Trace.KeyValue> resourceTags = resourceTags(resourceSpans.getResource(), schema);

      for (ScopeSpans scopeSpans : resourceSpans.getScopeSpansList()) {
        List<Trace.KeyValue> scopeTags = scopeTags(scopeSpans.getScope(), schema);

        for (Span span : scopeSpans.getSpansList()) {
          spans.add(convertSpan(span, normalizedDatasetName, resourceTags, scopeTags, schema));
        }
      }
    }

    return Map.of(normalizedDatasetName, spans);
  }

  private static Trace.Span convertSpan(
      Span otlpSpan,
      String traceDatasetName,
      List<Trace.KeyValue> resourceTags,
      List<Trace.KeyValue> scopeTags,
      Schema.IngestSchema schema) {
    Trace.Span.Builder spanBuilder =
        Trace.Span.newBuilder()
            .setTraceId(
                ByteString.copyFromUtf8(Hex.encodeHexString(otlpSpan.getTraceId().toByteArray())))
            .setId(ByteString.copyFromUtf8(Hex.encodeHexString(otlpSpan.getSpanId().toByteArray())))
            .setName(otlpSpan.getName())
            .setTimestamp(nanosToMicros(otlpSpan.getStartTimeUnixNano()))
            .setDuration(durationMicros(otlpSpan));

    if (!otlpSpan.getParentSpanId().isEmpty()) {
      spanBuilder.setParentId(
          ByteString.copyFromUtf8(Hex.encodeHexString(otlpSpan.getParentSpanId().toByteArray())));
    }

    spanBuilder.addTags(keywordTag(SERVICE_NAME, traceDatasetName));
    spanBuilder.addAllTags(resourceTags);
    spanBuilder.addAllTags(scopeTags);
    addSpanMetadataTags(spanBuilder, otlpSpan, schema);
    spanBuilder.addAllTags(attributesToTags(otlpSpan.getAttributesList(), "", schema));

    return spanBuilder.build();
  }

  private static void addSpanMetadataTags(
      Trace.Span.Builder spanBuilder, Span otlpSpan, Schema.IngestSchema schema) {
    if (!otlpSpan.getTraceState().isBlank()) {
      addConvertedTags(spanBuilder, "trace_state", otlpSpan.getTraceState(), schema);
    }
    if (otlpSpan.getKind() != Span.SpanKind.SPAN_KIND_UNSPECIFIED) {
      addConvertedTags(spanBuilder, "span.kind", spanKind(otlpSpan.getKind()), schema);
    }
    if (otlpSpan.hasStatus()) {
      addConvertedTags(spanBuilder, "status.code", otlpSpan.getStatus().getCode().name(), schema);
      addConvertedTags(spanBuilder, "status.message", otlpSpan.getStatus().getMessage(), schema);
    }
    if (otlpSpan.getDroppedAttributesCount() > 0) {
      addConvertedTags(
          spanBuilder,
          "otel.dropped_attributes_count",
          otlpSpan.getDroppedAttributesCount(),
          schema);
    }
    if (otlpSpan.getDroppedEventsCount() > 0) {
      addConvertedTags(
          spanBuilder, "otel.dropped_events_count", otlpSpan.getDroppedEventsCount(), schema);
    }
    if (otlpSpan.getDroppedLinksCount() > 0) {
      addConvertedTags(
          spanBuilder, "otel.dropped_links_count", otlpSpan.getDroppedLinksCount(), schema);
    }
  }

  private static List<Trace.KeyValue> resourceTags(Resource resource, Schema.IngestSchema schema) {
    return attributesToTags(resource.getAttributesList(), RESOURCE_PREFIX, schema);
  }

  private static List<Trace.KeyValue> scopeTags(
      InstrumentationScope scope, Schema.IngestSchema schema) {
    List<Trace.KeyValue> tags = new ArrayList<>();

    if (!scope.getName().isBlank()) {
      addConvertedTags(tags, "scope.name", scope.getName(), schema);
    }
    if (!scope.getVersion().isBlank()) {
      addConvertedTags(tags, "scope.version", scope.getVersion(), schema);
    }
    tags.addAll(attributesToTags(scope.getAttributesList(), SCOPE_PREFIX, schema));

    return tags;
  }

  private static List<Trace.KeyValue> attributesToTags(
      List<KeyValue> attributes, String prefix, Schema.IngestSchema schema) {
    List<Trace.KeyValue> tags = new ArrayList<>();
    for (KeyValue attribute : attributes) {
      if (attribute.getKey().isBlank()) {
        continue;
      }
      Object value = anyValueToObject(attribute.getValue());
      addConvertedTags(tags, prefix + attribute.getKey(), value, schema);
    }
    return tags;
  }

  private static Object anyValueToObject(AnyValue value) {
    return switch (value.getValueCase()) {
      case STRING_VALUE -> value.getStringValue();
      case BOOL_VALUE -> value.getBoolValue();
      case INT_VALUE -> value.getIntValue();
      case DOUBLE_VALUE -> value.getDoubleValue();
      case BYTES_VALUE -> Hex.encodeHexString(value.getBytesValue().toByteArray());
      case ARRAY_VALUE ->
          value.getArrayValue().getValuesList().stream()
              .map(OtlpTraceRequestParser::anyValueToObject)
              .toList();
      case KVLIST_VALUE -> keyValueListToMap(value.getKvlistValue());
      case VALUE_NOT_SET -> null;
    };
  }

  private static Map<String, Object> keyValueListToMap(KeyValueList keyValueList) {
    Map<String, Object> values = new LinkedHashMap<>();
    for (KeyValue keyValue : keyValueList.getValuesList()) {
      if (!keyValue.getKey().isBlank()) {
        values.put(keyValue.getKey(), anyValueToObject(keyValue.getValue()));
      }
    }
    return values;
  }

  private static String spanKind(Span.SpanKind kind) {
    return kind.name().replace("SPAN_KIND_", "").toLowerCase(Locale.ROOT);
  }

  private static long nanosToMicros(long nanos) {
    return nanos / 1_000;
  }

  private static long durationMicros(Span span) {
    return Math.max(0, span.getEndTimeUnixNano() - span.getStartTimeUnixNano()) / 1_000;
  }

  private static Trace.KeyValue keywordTag(String key, String value) {
    return Trace.KeyValue.newBuilder()
        .setKey(key)
        .setFieldType(Schema.SchemaFieldType.KEYWORD)
        .setVStr(value)
        .build();
  }

  private static void addConvertedTags(
      Trace.Span.Builder spanBuilder, String key, Object value, Schema.IngestSchema schema) {
    List<Trace.KeyValue> tags = SpanFormatter.convertKVtoProto(key, value, schema);
    if (tags != null) {
      spanBuilder.addAllTags(tags);
    }
  }

  private static void addConvertedTags(
      List<Trace.KeyValue> target, String key, Object value, Schema.IngestSchema schema) {
    List<Trace.KeyValue> tags = SpanFormatter.convertKVtoProto(key, value, schema);
    if (tags != null) {
      target.addAll(tags);
    }
  }
}
