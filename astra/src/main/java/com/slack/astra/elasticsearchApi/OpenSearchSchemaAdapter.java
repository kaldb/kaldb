package com.slack.astra.elasticsearchApi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchResultUtils;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

final class OpenSearchSchemaAdapter {
  private final AstraQueryServiceBase searcher;

  OpenSearchSchemaAdapter(AstraQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  Map<String, Map<String, String>> buildPropertiesMap(
      String indexName, long startTimeEpochMs, long endTimeEpochMs) {
    Map<String, FieldType> schema =
        SearchResultUtils.fromSchemaResultProto(
            searcher.getSchema(
                AstraSearch.SchemaRequest.newBuilder()
                    .setDataset(indexName)
                    .setStartTimeEpochMs(startTimeEpochMs)
                    .setEndTimeEpochMs(endTimeEpochMs)
                    .build()));

    TreeMap<String, Map<String, String>> propertiesMap = new TreeMap<>();
    schema.forEach(
        (key, fieldType) ->
            propertiesMap.put(key, Map.of("type", fieldType.toOpenSearchTypeName())));

    Map<String, String> dateType = Map.of("type", FieldType.DATE.toOpenSearchTypeName());
    propertiesMap.put(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, dateType);
    propertiesMap.put(LogMessage.ReservedField.TIMESTAMP.fieldName, dateType);
    return propertiesMap;
  }

  Map<String, Map<String, FieldTypeCapabilities>> buildFieldCaps(
      String indexName,
      long startTimeEpochMs,
      long endTimeEpochMs,
      RequestedFields requestedFields) {
    TreeMap<String, Map<String, FieldTypeCapabilities>> fieldCaps = new TreeMap<>();
    buildPropertiesMap(indexName, startTimeEpochMs, endTimeEpochMs).entrySet().stream()
        .filter(e -> requestedFields.matches(e.getKey()))
        .forEach(
            e -> {
              String fieldType = e.getValue().get("type");
              fieldCaps.put(
                  e.getKey(),
                  Map.of(
                      fieldType,
                      new FieldTypeCapabilities(
                          fieldType, true, isAggregatableFieldType(fieldType))));
            });
    return fieldCaps;
  }

  record FieldTypeCapabilities(
      @JsonProperty("type") String type,
      @JsonProperty("searchable") boolean searchable,
      @JsonProperty("aggregatable") boolean aggregatable) {}

  private static boolean isAggregatableFieldType(String fieldType) {
    return !"text".equals(fieldType);
  }
}

record RequestedFields(boolean matchAll, List<Pattern> patterns) {
  static RequestedFields from(String fields) {
    if (fields == null || fields.isBlank()) {
      return new RequestedFields(true, List.of());
    }

    List<String> requestedFields =
        Arrays.stream(fields.split(",")).map(String::strip).filter(s -> !s.isEmpty()).toList();
    if (requestedFields.contains("*")) {
      return new RequestedFields(true, List.of());
    }
    return new RequestedFields(
        false, requestedFields.stream().map(RequestedFields::globPattern).toList());
  }

  boolean matches(String fieldName) {
    return matchAll || patterns.stream().anyMatch(pattern -> pattern.matcher(fieldName).matches());
  }

  private static Pattern globPattern(String fieldPattern) {
    StringBuilder regex = new StringBuilder("^");
    StringBuilder literal = new StringBuilder();
    for (int i = 0; i < fieldPattern.length(); i++) {
      char c = fieldPattern.charAt(i);
      switch (c) {
        case '*' -> {
          appendQuotedLiteral(regex, literal);
          regex.append(".*");
        }
        case '?' -> {
          appendQuotedLiteral(regex, literal);
          regex.append(".");
        }
        default -> literal.append(c);
      }
    }
    appendQuotedLiteral(regex, literal);
    regex.append("$");
    return Pattern.compile(regex.toString());
  }

  private static void appendQuotedLiteral(StringBuilder regex, StringBuilder literal) {
    if (literal.length() == 0) {
      return;
    }
    regex.append(Pattern.quote(literal.toString()));
    literal.setLength(0);
  }
}
