package com.slack.astra.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregatorFactories;

public class SearchResultUtils {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
  private static final NamedXContentRegistry namedXContentRegistry =
      new NamedXContentRegistry(searchModule.getNamedXContents());

  public static Map<String, Object> fromValueStruct(AstraSearch.Struct struct) {
    Map<String, Object> returnMap = new HashMap<>();
    struct.getFieldsMap().forEach((key, value) -> returnMap.put(key, fromValueProto(value)));
    return returnMap;
  }

  public static AstraSearch.Struct toStructProto(Map<String, Object> map) {
    Map<String, AstraSearch.Value> valueMap = new HashMap<>();
    map.forEach((key, value) -> valueMap.put(key, toValueProto(value)));
    return AstraSearch.Struct.newBuilder().putAllFields(valueMap).build();
  }

  public static Object fromValueProto(AstraSearch.Value value) {
    if (value.hasNullValue()) {
      return null;
    } else if (value.hasIntValue()) {
      return value.getIntValue();
    } else if (value.hasLongValue()) {
      return value.getLongValue();
    } else if (value.hasDoubleValue()) {
      return value.getDoubleValue();
    } else if (value.hasStringValue()) {
      return value.getStringValue();
    } else if (value.hasBoolValue()) {
      return value.getBoolValue();
    } else if (value.hasBytesValue()) {
      return value.getBytesValue();
    } else if (value.hasStructValue()) {
      return fromValueStruct(value.getStructValue());
    } else if (value.hasListValue()) {
      return value.getListValue().getValuesList().stream()
          .map(SearchResultUtils::fromValueProto)
          .collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static AstraSearch.Value toValueProto(Object object) {
    AstraSearch.Value.Builder valueBuilder = AstraSearch.Value.newBuilder();

    if (object == null) {
      valueBuilder.setNullValue(AstraSearch.NullValue.NULL_VALUE);
    } else if (object instanceof Integer) {
      valueBuilder.setIntValue((Integer) object);
    } else if (object instanceof Long) {
      valueBuilder.setLongValue((Long) object);
    } else if (object instanceof Float) {
      valueBuilder.setDoubleValue(((Float) object).doubleValue());
    } else if (object instanceof Double) {
      valueBuilder.setDoubleValue((Double) object);
    } else if (object instanceof String) {
      valueBuilder.setStringValue((String) object);
    } else if (object instanceof Boolean) {
      valueBuilder.setBoolValue((Boolean) object);
    } else if (object instanceof ByteString) {
      valueBuilder.setBytesValue((ByteString) object);
    } else if (object instanceof Map) {
      valueBuilder.setStructValue(toStructProto((Map<String, Object>) object));
    } else if (object instanceof List) {
      valueBuilder.setListValue(
          AstraSearch.ListValue.newBuilder()
              .addAllValues(
                  ((List<?>) object)
                      .stream().map(SearchResultUtils::toValueProto).collect(Collectors.toList()))
              .build());
    } else {
      throw new IllegalArgumentException();
    }

    return valueBuilder.build();
  }

  public static SearchQuery fromSearchRequest(AstraSearch.SearchRequest searchRequest) {
    QueryBuilder queryBuilder = null;

    if (!searchRequest.getQuery().isEmpty()) {
      try {
        JsonXContentParser jsonXContentParser =
            new JsonXContentParser(
                namedXContentRegistry,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                objectMapper.createParser(searchRequest.getQuery()));
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(jsonXContentParser);
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    }

    AggregatorFactories.Builder aggregatorFactoriesBuilder = null;
    if (!searchRequest.getAggregationJson().isEmpty()) {
      try {
        JsonXContentParser jsonXContentParser =
            new JsonXContentParser(
                namedXContentRegistry,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                objectMapper.createParser(searchRequest.getAggregationJson()));

        jsonXContentParser.nextToken();
        aggregatorFactoriesBuilder = AggregatorFactories.parseAggregators(jsonXContentParser);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    return new SearchQuery(
        searchRequest.getDataset(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        searchRequest.getStartFrom(),
        parseSortFieldSpecs(searchRequest.getSortJson()),
        searchRequest.getChunkIdsList(),
        queryBuilder,
        SourceFieldFilter.fromProto(searchRequest.getSourceFieldFilter()),
        aggregatorFactoriesBuilder);
  }

  /** Returns the number of user-requested sort values to render in each response hit. */
  public static int responseSortValueCount(String sortJson) {
    return parseSortFieldSpecs(sortJson).size();
  }

  /** Parses OpenSearch hit sort clauses into the internal sort representation. */
  static List<SearchQuery.SortFieldSpec> parseSortFieldSpecs(String sortJson) {
    if (sortJson == null || sortJson.isBlank()) {
      return List.of();
    }

    try {
      JsonNode sortNode = objectMapper.readTree(sortJson);
      List<SearchQuery.SortFieldSpec> sortFieldSpecs = new ArrayList<>();

      if (sortNode.isArray()) {
        for (JsonNode sortClause : sortNode) {
          addSortFieldSpec(sortFieldSpecs, sortClause);
        }
      } else {
        addSortFieldSpec(sortFieldSpecs, sortNode);
      }

      return sortFieldSpecs;
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static void addSortFieldSpec(
      List<SearchQuery.SortFieldSpec> sortFieldSpecs, JsonNode sortClause) {
    if (sortClause == null || sortClause.isNull()) {
      return;
    }
    if (sortClause.isTextual()) {
      addSortFieldSpec(sortFieldSpecs, sortClause.asText(), SearchQuery.SortDirection.ASC);
      return;
    }
    if (!sortClause.isObject()) {
      throw new IllegalArgumentException("Unsupported sort clause: " + sortClause);
    }

    Iterator<Map.Entry<String, JsonNode>> fields = sortClause.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      addSortFieldSpec(
          sortFieldSpecs, entry.getKey(), parseSortDirection(entry.getKey(), entry.getValue()));
    }
  }

  private static SearchQuery.SortDirection parseSortDirection(String fieldName, JsonNode value) {
    if (value != null && value.isTextual()) {
      return parseSortOrder(fieldName, value.asText());
    }
    if (value != null && value.isObject()) {
      JsonNode order = value.get("order");
      if (order == null) {
        return SearchQuery.SortDirection.ASC;
      }
      if (order.isTextual()) {
        return parseSortOrder(fieldName, order.asText());
      }
    }
    throw new IllegalArgumentException(
        "Unsupported sort direction for field " + fieldName + ": " + value);
  }

  private static SearchQuery.SortDirection parseSortOrder(String fieldName, String order) {
    if ("asc".equalsIgnoreCase(order)) {
      return SearchQuery.SortDirection.ASC;
    }
    if ("desc".equalsIgnoreCase(order)) {
      return SearchQuery.SortDirection.DESC;
    }
    throw new IllegalArgumentException(
        "Unsupported sort direction for field " + fieldName + ": " + order);
  }

  private static void addSortFieldSpec(
      List<SearchQuery.SortFieldSpec> sortFieldSpecs,
      String fieldName,
      SearchQuery.SortDirection direction) {
    if ("_doc".equals(fieldName)) {
      return;
    }
    if (LogMessage.SystemField.ID.fieldName.equals(fieldName)) {
      throw new IllegalArgumentException("Sorting by _id is not supported.");
    }
    sortFieldSpecs.add(new SearchQuery.SortFieldSpec(fieldName, direction));
  }

  public static SearchResult<LogMessage> fromSearchResultProtoOrEmpty(
      AstraSearch.SearchResult protoSearchResult) {
    try {
      return fromSearchResultProto(protoSearchResult);
    } catch (IOException e) {
      return SearchResult.empty();
    }
  }

  public static SearchResult<LogMessage> fromSearchResultProto(
      AstraSearch.SearchResult protoSearchResult) throws IOException {
    List<SearchResultHit<LogMessage>> hits = new ArrayList<>(protoSearchResult.getHitsCount());

    for (AstraSearch.SearchResult.Hit protoHit : protoSearchResult.getHitsList()) {
      LogWireMessage hit = JsonUtil.read(protoHit.getMessage(), LogWireMessage.class);
      LogMessage message = LogMessage.fromWireMessage(hit);
      List<HitSortValue> sortValues =
          protoHit.getSortValuesList().stream()
              .map(SearchResultUtils::fromHitSortValueProto)
              .toList();
      hits.add(new SearchResultHit<>(message, sortValues));
    }

    return new SearchResult<>(
        hits,
        protoSearchResult.getTookMicros(),
        protoSearchResult.getFailedNodes(),
        protoSearchResult.getTotalNodes(),
        protoSearchResult.getRequestedSnapshots(),
        protoSearchResult.getFulfilledSnapshots(),
        OpenSearchInternalAggregation.fromByteArray(
            protoSearchResult.getInternalAggregations().toByteArray()));
  }

  /** Converts a protobuf hit sort value into the domain value used by search reduction. */
  public static HitSortValue fromHitSortValueProto(AstraSearch.HitSortValue value) {
    if (value.hasNullValue()) {
      return HitSortValue.nullValue();
    } else if (value.hasIntValue()) {
      return HitSortValue.intValue(value.getIntValue());
    } else if (value.hasLongValue()) {
      return HitSortValue.longValue(value.getLongValue());
    } else if (value.hasFloatValue()) {
      return HitSortValue.floatValue(value.getFloatValue());
    } else if (value.hasDoubleValue()) {
      return HitSortValue.doubleValue(value.getDoubleValue());
    } else if (value.hasStringValue()) {
      return HitSortValue.stringValue(value.getStringValue());
    } else if (value.hasBoolValue()) {
      return HitSortValue.booleanValue(value.getBoolValue());
    } else if (value.hasBytesValue()) {
      return HitSortValue.bytes(value.getBytesValue());
    } else if (value.hasIpValue()) {
      return HitSortValue.ipAddress(value.getIpValue());
    } else {
      return HitSortValue.nullValue();
    }
  }

  /** Converts a domain hit sort value into its protobuf representation. */
  public static AstraSearch.HitSortValue toHitSortValueProto(HitSortValue sortValue) {
    AstraSearch.HitSortValue.Builder valueBuilder = AstraSearch.HitSortValue.newBuilder();
    switch (sortValue.kind()) {
      case NULL -> valueBuilder.setNullValue(AstraSearch.NullValue.NULL_VALUE);
      case INT -> valueBuilder.setIntValue((Integer) sortValue.value());
      case LONG -> valueBuilder.setLongValue((Long) sortValue.value());
      case FLOAT -> valueBuilder.setFloatValue((Float) sortValue.value());
      case DOUBLE -> valueBuilder.setDoubleValue((Double) sortValue.value());
      case STRING -> valueBuilder.setStringValue((String) sortValue.value());
      case BOOLEAN -> valueBuilder.setBoolValue((Boolean) sortValue.value());
      case BYTES -> valueBuilder.setBytesValue((ByteString) sortValue.value());
      case IP_ADDRESS -> valueBuilder.setIpValue((ByteString) sortValue.value());
    }
    return valueBuilder.build();
  }

  public static FieldType fromSchemaDefinitionProto(
      AstraSearch.SchemaDefinition protoSchemaDefinition) {
    return FieldType.fromSchemaFieldType(protoSchemaDefinition.getType());
  }

  public static AstraSearch.SchemaDefinition toSchemaDefinitionProto(FieldType fieldType) {
    AstraSearch.SchemaDefinition.Builder schemaBuilder = AstraSearch.SchemaDefinition.newBuilder();
    schemaBuilder.setType(fieldType.toSchemaFieldType());
    return schemaBuilder.build();
  }

  public static Map<String, FieldType> fromSchemaResultProto(
      AstraSearch.SchemaResult protoSchemaResult) {
    Map<String, FieldType> schemaMap = new HashMap<>();
    protoSchemaResult
        .getFieldDefinitionMap()
        .forEach(
            (key, value) -> {
              schemaMap.put(key, fromSchemaDefinitionProto(value));
            });
    return schemaMap;
  }

  public static AstraSearch.SchemaResult toSchemaResultProto(Map<String, FieldType> schema) {
    AstraSearch.SchemaResult.Builder schemaBuilder = AstraSearch.SchemaResult.newBuilder();
    schema.forEach(
        (key, value) -> schemaBuilder.putFieldDefinition(key, toSchemaDefinitionProto(value)));
    return schemaBuilder.build();
  }

  public static <T> AstraSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultUtils.toSearchResultProto");
    span.tag("tookMicros", String.valueOf(searchResult.tookMicros));
    span.tag("failedNodes", String.valueOf(searchResult.failedNodes));
    span.tag("totalNodes", String.valueOf(searchResult.totalNodes));
    span.tag("requestedSnapshots", String.valueOf(searchResult.requestedSnapshots));
    span.tag("fulfilledSnapshots", String.valueOf(searchResult.fulfilledSnapshots));
    span.tag("hits", String.valueOf(searchResult.hits.size()));

    AstraSearch.SearchResult.Builder searchResultBuilder = AstraSearch.SearchResult.newBuilder();
    searchResultBuilder.setTookMicros(searchResult.tookMicros);
    searchResultBuilder.setFailedNodes(searchResult.failedNodes);
    searchResultBuilder.setTotalNodes(searchResult.totalNodes);
    searchResultBuilder.setRequestedSnapshots(searchResult.requestedSnapshots);
    searchResultBuilder.setFulfilledSnapshots(searchResult.fulfilledSnapshots);

    // Set hits
    for (SearchResultHit<T> hit : searchResult.hits) {
      try {
        searchResultBuilder.addHits(
            AstraSearch.SearchResult.Hit.newBuilder()
                .setMessage(JsonUtil.writeAsString(hit.message()))
                .addAllSortValues(
                    hit.sortValues().stream().map(SearchResultUtils::toHitSortValueProto).toList())
                .build());
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }

    ByteString bytes =
        ByteString.copyFrom(
            OpenSearchInternalAggregation.toByteArray(searchResult.internalAggregations));
    searchResultBuilder.setInternalAggregations(bytes);
    span.finish();
    return searchResultBuilder.build();
  }
}
