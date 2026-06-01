package com.slack.astra.elasticsearchApi;

import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.proto.service.AstraSearch;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.StreamSupport;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.SearchModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for parsing an OpenSearch NDJSON search request into a list of appropriate
 * AstraSearch.SearchRequests, that can be provided to the GRPC Search API. This class is
 * responsible for taking a raw payload string, performing any validation as appropriate, and
 * building a complete working list of queries to be performed.
 */
public class OpenSearchRequest {
  private static final String DEFAULT_DATE_FORMAT = "strict_date_optional_time||epoch_millis";
  private static final ObjectMapper OM =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final OpenSearchAdapter openSearchAdapter =
      new OpenSearchAdapter(Collections.EMPTY_MAP);
  private static final Logger log = LoggerFactory.getLogger(OpenSearchRequest.class);

  private static class InvalidDateRangeException extends IllegalArgumentException {
    private InvalidDateRangeException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class DateRangeQueryBuilderVistor implements QueryBuilderVisitor {
    private static final QueryBuilderVisitor DISABLED_VISITOR =
        new QueryBuilderVisitor() {
          @Override
          public void accept(QueryBuilder qb) {}

          @Override
          public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            return this;
          }
        };

    private int dateRangeCount;
    private Long dateRangeStart;
    private Long dateRangeEnd;

    @Override
    public void accept(QueryBuilder qb) {
      if (qb instanceof RangeQueryBuilder rangeQueryBuilder) {
        if (!rangeQueryBuilder.fieldName().equals("@timestamp")
            && !rangeQueryBuilder
                .fieldName()
                .equals(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)) {
          return;
        }

        Object from = rangeQueryBuilder.from();
        Object to = rangeQueryBuilder.to();
        String format = rangeQueryBuilder.format();
        String timeZone = rangeQueryBuilder.timeZone();

        dateRangeCount++;
        dateRangeStart = toEpochMillis(from, format, timeZone, false);
        dateRangeEnd = toEpochMillis(to, format, timeZone, true);
      }
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
      return switch (occur) {
        case MUST, FILTER -> this;
        case SHOULD, MUST_NOT -> DISABLED_VISITOR;
      };
    }
  }

  public List<AstraSearch.SearchRequest> parseMultiSearchRequest(String postBody)
      throws JsonProcessingException {
    // the body contains an NDJSON format, with alternating rows as header/body
    // @see http://ndjson.org/
    // @see
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html#search-multi-search-api-desc

    List<String> lines = postBody.lines().toList();
    if ((lines.size() & 1) != 0) {
      throw new IllegalArgumentException("NDJSON body has an unmatched header line");
    }
    List<AstraSearch.SearchRequest> searchRequests = new ArrayList<>(lines.size() / 2);

    for (int i = 0; i < lines.size(); i += 2) {
      JsonNode header = OM.readTree(lines.get(i));
      JsonNode body = OM.readTree(lines.get(i + 1));
      searchRequests.add(toSearchRequest(getDataset(header), body));
    }
    return searchRequests;
  }

  public AstraSearch.SearchRequest parseSingleSearchRequest(String dataset, String postBody)
      throws JsonProcessingException {
    return toSearchRequest(resolveDataset(dataset), OM.readTree(postBody));
  }

  private static AstraSearch.SearchRequest toSearchRequest(String dataset, JsonNode body) {
    String query = getQuery(body);
    DateRangeQueryBuilderVistor dateRangeQueryBuilderVistor = getDateRange(query);
    long startTimeEpochMs = 0L;
    long endTimeEpochMs = MAX_TIME;
    if (dateRangeQueryBuilderVistor != null
        && dateRangeQueryBuilderVistor.dateRangeCount == 1
        && dateRangeQueryBuilderVistor.dateRangeStart != null
        && dateRangeQueryBuilderVistor.dateRangeEnd != null) {
      startTimeEpochMs = dateRangeQueryBuilderVistor.dateRangeStart;
      endTimeEpochMs = dateRangeQueryBuilderVistor.dateRangeEnd;
    }

    return AstraSearch.SearchRequest.newBuilder()
        .setDataset(dataset)
        .setHowMany(getHowMany(body))
        .setQuery(query)
        .setSourceFieldFilter(getSourceFieldFilter(body))
        .setAggregationJson(getAggregationJson(body))
        .setTrackTotalHits(getTrackTotalHits(body))
        .setStartTimeEpochMs(startTimeEpochMs)
        .setEndTimeEpochMs(endTimeEpochMs)
        .build();
  }

  private static AstraSearch.SearchRequest.SourceFieldFilter getSourceFieldFilter(JsonNode body) {
    if (body.has("_source") && body.get("_source") != null) {
      JsonNode sourceNode = body.get("_source");
      if (sourceNode.isBoolean()) {
        return AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .setIncludeAll(sourceNode.booleanValue())
            .build();

      } else if (sourceNode.isTextual()) {
        return AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addIncludeWildcards(sourceNode.textValue())
            .build();
      } else if (sourceNode.isArray()) {
        ArrayNode includeArrayNode = (ArrayNode) sourceNode;
        HashMap<String, Boolean> includes = new HashMap<>();

        AstraSearch.SearchRequest.SourceFieldFilter.Builder fieldInclusionBuilder =
            AstraSearch.SearchRequest.SourceFieldFilter.newBuilder();

        for (JsonNode jsonNode : includeArrayNode) {
          String fieldname = jsonNode.asText();
          if (fieldname.contains("*")) {
            fieldInclusionBuilder.addIncludeWildcards(fieldname);
          } else {
            includes.put(fieldname, true);
          }
        }

        return fieldInclusionBuilder.putAllIncludeFields(includes).build();

      } else if (sourceNode.isObject()) {
        AstraSearch.SearchRequest.SourceFieldFilter.Builder sourceFieldFilterBuilder =
            AstraSearch.SearchRequest.SourceFieldFilter.newBuilder();

        if (sourceNode.has("includes")) {
          ArrayNode includeArrayNode = (ArrayNode) sourceNode.get("includes");
          HashMap<String, Boolean> includes = new HashMap<>();

          for (JsonNode jsonNode : includeArrayNode) {
            String fieldname = jsonNode.asText();
            if (fieldname.contains("*")) {
              sourceFieldFilterBuilder.addIncludeWildcards(fieldname);
            } else {
              includes.put(fieldname, true);
            }
          }

          sourceFieldFilterBuilder.putAllIncludeFields(includes);
        }

        if (sourceNode.has("excludes")) {
          ArrayNode includeArrayNode = (ArrayNode) sourceNode.get("excludes");
          HashMap<String, Boolean> excludes = new HashMap<>();

          for (JsonNode jsonNode : includeArrayNode) {
            String fieldname = jsonNode.asText();
            if (fieldname.contains("*")) {
              sourceFieldFilterBuilder.addExcludeWildcards(fieldname);
            } else {
              excludes.put(fieldname, true);
            }
          }
          sourceFieldFilterBuilder.putAllExcludeFields(excludes);
        }
        return sourceFieldFilterBuilder.build();
      }
    }
    return AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().build();
  }

  private static DateRangeQueryBuilderVistor getDateRange(String queryBody) {
    try {
      openSearchAdapter.reloadSchema();
      JsonXContentParser jsonXContentParser =
          new JsonXContentParser(
              new NamedXContentRegistry(
                  new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()),
              DeprecationHandler.IGNORE_DEPRECATIONS,
              OM.createParser(queryBody));

      QueryBuilder queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(jsonXContentParser);
      DateRangeQueryBuilderVistor dateRangeQueryBuilderVistor = new DateRangeQueryBuilderVistor();
      queryBuilder.visit(dateRangeQueryBuilderVistor);
      return dateRangeQueryBuilderVistor;

    } catch (Exception e) {
      log.error("Unable to parse date/time range from query body: {}. Error: {}", queryBody, e);
      return null;
    }
  }

  private static String getQuery(JsonNode body) {
    JsonNode queryNode = body.get("query");
    if (queryNode != null && !queryNode.isNull() && !queryNode.isEmpty()) {
      return rewriteBareWildcardQueryStringsAsMatchAll(queryNode).toString();
    }
    return matchAllQueryNode().toString();
  }

  /**
   * TODO: fix plain query_string "*" handling in the OpenSearch/Lucene query layer so this
   * parser-level compatibility rewrite can be removed.
   */
  private static JsonNode rewriteBareWildcardQueryStringsAsMatchAll(JsonNode queryNode) {
    return rewriteBareWildcardQueryStringsAsMatchAllInternal(queryNode).rewrittenNode();
  }

  private static RewriteResult rewriteBareWildcardQueryStringsAsMatchAllInternal(
      JsonNode queryNode) {
    if (queryNode == null || queryNode.isNull()) {
      return new RewriteResult(queryNode, false);
    }

    if (queryNode.isArray()) {
      List<JsonNode> rewrittenChildren = new ArrayList<>(queryNode.size());
      boolean changed = false;
      for (JsonNode child : queryNode) {
        RewriteResult childRewrite = rewriteBareWildcardQueryStringsAsMatchAllInternal(child);
        rewrittenChildren.add(childRewrite.rewrittenNode());
        changed |= childRewrite.changed();
      }
      if (!changed) {
        return new RewriteResult(queryNode, false);
      }
      ArrayNode rewrittenArray = OM.createArrayNode();
      rewrittenChildren.forEach(rewrittenArray::add);
      return new RewriteResult(rewrittenArray, true);
    }

    if (!(queryNode instanceof ObjectNode objectNode)) {
      return new RewriteResult(queryNode, false);
    }

    if (objectNode.size() == 1
        && objectNode.has("query_string")
        && isPlainWildcardQueryString(objectNode.get("query_string"))) {
      return new RewriteResult(matchAllQueryNode(), true);
    }

    Map<String, JsonNode> rewrittenFields = new LinkedHashMap<>(objectNode.size());
    boolean changed = false;
    var fields = objectNode.fields();
    while (fields.hasNext()) {
      var entry = fields.next();
      RewriteResult fieldRewrite =
          rewriteBareWildcardQueryStringsAsMatchAllInternal(entry.getValue());
      rewrittenFields.put(entry.getKey(), fieldRewrite.rewrittenNode());
      changed |= fieldRewrite.changed();
    }
    if (!changed) {
      return new RewriteResult(queryNode, false);
    }
    ObjectNode rewrittenNode = OM.createObjectNode();
    rewrittenFields.forEach(rewrittenNode::set);
    return new RewriteResult(rewrittenNode, true);
  }

  private static final Set<String> BARE_WILDCARD_ALLOWED_FIELDS =
      Set.of("query", "analyze_wildcard");

  private record RewriteResult(JsonNode rewrittenNode, boolean changed) {}

  private static boolean isPlainWildcardQueryString(JsonNode queryStringNode) {
    if (!(queryStringNode instanceof ObjectNode objectNode) || !objectNode.has("query")) {
      return false;
    }

    boolean allFieldsAllowed =
        StreamSupport.stream(
                ((Iterable<String>) () -> objectNode.fieldNames()).spliterator(), false)
            .allMatch(BARE_WILDCARD_ALLOWED_FIELDS::contains);

    return allFieldsAllowed
        && objectNode.get("query").isTextual()
        && "*".equals(objectNode.get("query").asText());
  }

  private static ObjectNode matchAllQueryNode() {
    ObjectNode matchAllNode = OM.createObjectNode();
    matchAllNode.set("match_all", OM.createObjectNode());
    return matchAllNode;
  }

  private static String getDataset(JsonNode header) {
    return resolveDataset(header.path("index").asText(null));
  }

  private static String resolveDataset(String dataset) {
    return (dataset == null || dataset.isBlank()) ? "_all" : dataset;
  }

  private static int getHowMany(JsonNode body) {
    return body.path("size").asInt(10);
  }

  private static AstraSearch.SearchRequest.TrackTotalHits getTrackTotalHits(JsonNode body) {
    JsonNode trackTotalHitsNode = body.get("track_total_hits");
    AstraSearch.SearchRequest.TrackTotalHits.Builder builder =
        AstraSearch.SearchRequest.TrackTotalHits.newBuilder();
    if (trackTotalHitsNode == null || trackTotalHitsNode.isNull()) {
      return builder
          .setThreshold(
              AstraSearch.SearchRequest.TrackTotalHits.Threshold.newBuilder()
                  .setValue(SearchQuery.TotalHitsPolicy.DEFAULT_THRESHOLD))
          .build();
    }
    if (trackTotalHitsNode.isBoolean()) {
      return trackTotalHitsNode.booleanValue()
          ? builder
              .setExact(AstraSearch.SearchRequest.TrackTotalHits.Exact.getDefaultInstance())
              .build()
          : builder
              .setDisabled(AstraSearch.SearchRequest.TrackTotalHits.Disabled.getDefaultInstance())
              .build();
    }
    if (trackTotalHitsNode.isIntegralNumber()
        && trackTotalHitsNode.canConvertToInt()
        && trackTotalHitsNode.asInt() >= 0) {
      return builder
          .setThreshold(
              AstraSearch.SearchRequest.TrackTotalHits.Threshold.newBuilder()
                  .setValue(trackTotalHitsNode.asInt()))
          .build();
    }
    throw new IllegalArgumentException(
        "track_total_hits must be a boolean or non-negative integer");
  }

  private static String getAggregationJson(JsonNode body) {
    JsonNode aggsNode = body.has("aggs") ? body.get("aggs") : body.get("aggregations");
    if (aggsNode == null || aggsNode.isEmpty()) {
      return "";
    }
    return aggsNode.toString();
  }

  private static Long toEpochMillis(Object value, String format, String timeZone, boolean roundUp) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number numberValue) {
      return numberValue.longValue();
    }
    if (!(value instanceof String stringValue)) {
      return null;
    }

    return parseStringDateRangeValue(stringValue, format, timeZone, roundUp);
  }

  private static long parseStringDateRangeValue(
      String value, String format, String timeZone, boolean roundUp) {
    String dateFormat = resolveDateFormat(format);
    try {
      return parseWithDateMath(value, dateFormat, timeZone, roundUp);
    } catch (OpenSearchParseException | DateTimeException parseFailure) {
      return tryParseEpochMillisString(value)
          .or(() -> tryParseIsoInstant(value))
          .orElseThrow(() -> invalidDateRange(value, dateFormat, parseFailure));
    }
  }

  private static String resolveDateFormat(String format) {
    return (format == null || format.isBlank()) ? DEFAULT_DATE_FORMAT : format;
  }

  private static long parseWithDateMath(
      String value, String dateFormat, String timeZone, boolean roundUp) {
    LongSupplier now = System::currentTimeMillis;
    return DateFormatter.forPattern(dateFormat)
        .toDateMathParser()
        .parse(value, now, roundUp, resolveTimeZone(timeZone))
        .toEpochMilli();
  }

  private static ZoneId resolveTimeZone(String timeZone) {
    if (timeZone == null || timeZone.isBlank()) {
      return ZoneOffset.UTC;
    }
    return ZoneId.of(timeZone);
  }

  private static Optional<Long> tryParseEpochMillisString(String value) {
    try {
      return Optional.of(Long.valueOf(value));
    } catch (NumberFormatException ignored) {
      return Optional.empty();
    }
  }

  private static Optional<Long> tryParseIsoInstant(String value) {
    try {
      return Optional.of(Instant.parse(value).toEpochMilli());
    } catch (RuntimeException ignored) {
      return Optional.empty();
    }
  }

  private static InvalidDateRangeException invalidDateRange(
      String value, String dateFormat, Exception cause) {
    return new InvalidDateRangeException(
        String.format("Unable to parse date range value '%s' with format '%s'", value, dateFormat),
        cause);
  }
}
