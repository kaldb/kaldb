package com.slack.astra.elasticsearchApi;

import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.slack.astra.proto.service.AstraSearch;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

public class OpenSearchRequestTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String getRawQueryString(String filename) throws IOException {
    return Resources.toString(
        Resources.getResource(String.format("opensearchRequest/%s.ndjson", filename)),
        StandardCharsets.UTF_8);
  }

  private String getSingleSearchBody(String filename) throws IOException {
    return secondNdjsonLine(getRawQueryString(filename));
  }

  private String secondNdjsonLine(String requestBody) {
    return requestBody.lines().skip(1).findFirst().orElseThrow();
  }

  private String queryFromRequestBody(String requestBody) throws IOException {
    return OBJECT_MAPPER.readTree(requestBody).get("query").toString();
  }

  private boolean containsSubtree(JsonNode root, JsonNode expected) {
    if (root.equals(expected)) {
      return true;
    }
    if (root.isArray()) {
      for (JsonNode child : root) {
        if (containsSubtree(child, expected)) {
          return true;
        }
      }
    } else if (root.isObject()) {
      Iterator<JsonNode> fields = root.elements();
      while (fields.hasNext()) {
        if (containsSubtree(fields.next(), expected)) {
          return true;
        }
      }
    }
    return false;
  }

  private JsonNode findFirstRangeClause(JsonNode root) {
    if (root.isObject()) {
      if (root.has("range") && root.get("range").isObject()) {
        return root;
      }
      Iterator<JsonNode> fields = root.elements();
      while (fields.hasNext()) {
        JsonNode found = findFirstRangeClause(fields.next());
        if (found != null) {
          return found;
        }
      }
    } else if (root.isArray()) {
      for (JsonNode child : root) {
        JsonNode found = findFirstRangeClause(child);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private boolean containsPlainWildcardQueryString(JsonNode root) {
    if (root.isObject()) {
      JsonNode queryStringNode = root.get("query_string");
      if (queryStringNode != null
          && queryStringNode.isObject()
          && "*".equals(queryStringNode.path("query").asText(null))) {
        return true;
      }
      Iterator<JsonNode> fields = root.elements();
      while (fields.hasNext()) {
        if (containsPlainWildcardQueryString(fields.next())) {
          return true;
        }
      }
    } else if (root.isArray()) {
      for (JsonNode child : root) {
        if (containsPlainWildcardQueryString(child)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean containsMatchAllQuery(JsonNode root) {
    if (root.isObject()) {
      if (root.has("match_all") && root.get("match_all").isObject()) {
        return true;
      }
      Iterator<JsonNode> fields = root.elements();
      while (fields.hasNext()) {
        if (containsMatchAllQuery(fields.next())) {
          return true;
        }
      }
    } else if (root.isArray()) {
      for (JsonNode child : root) {
        if (containsMatchAllQuery(child)) {
          return true;
        }
      }
    }
    return false;
  }

  private void assertFixturePlainWildcardWasRewritten(String requestBody, String actualQuery)
      throws IOException {
    JsonNode originalQuery = OBJECT_MAPPER.readTree(secondNdjsonLine(requestBody)).get("query");
    JsonNode parsedQuery = OBJECT_MAPPER.readTree(actualQuery);
    JsonNode originalRangeClause = findFirstRangeClause(originalQuery);

    assertThat(originalRangeClause).isNotNull();
    assertThat(containsSubtree(parsedQuery, originalRangeClause)).isTrue();
    assertThat(containsPlainWildcardQueryString(parsedQuery)).isFalse();
    assertThat(containsMatchAllQuery(parsedQuery)).isTrue();
  }

  @Test
  public void testGetAggregationJson() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    JsonNode parsedRequest = OBJECT_MAPPER.readTree(secondNdjsonLine(rawRequest));
    assertThat(request.getAggregationJson()).isEqualTo(parsedRequest.get("aggs").toString());
  }

  @Test
  public void testGetAggregationJsonWithMultipleTopLevelAggs() throws Exception {
    String searchBody =
        """
        {
          "size": 0,
          "aggs": {
            "over_time": {
              "date_histogram": {
                "field": "_timesinceepoch",
                "interval": "10m",
                "min_doc_count": 0,
                "extended_bounds": {
                  "min": 1676498801027,
                  "max": 1676500240688
                },
                "format": "epoch_millis"
              },
              "aggs": {}
            },
            "services": {
              "terms": {
                "field": "service_name",
                "size": 10
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    JsonNode parsedRequest = OBJECT_MAPPER.readTree(searchBody);
    assertThat(request.getAggregationJson()).isEqualTo(parsedRequest.get("aggs").toString());
  }

  @Test
  public void testExplicitEmptyAggsTakesPrecedenceOverAggregations() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "aggs": {},
          "aggregations": {
            "levels": {
              "terms": {
                "field": "level"
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    // "aggs" and "aggregations" are aliases. If "aggs" is explicitly present but empty, treat
    // that as "no aggregations" instead of falling back to the non-empty "aggregations" alias.
    assertThat(request.getAggregationJson()).isEmpty();
  }

  @Test
  public void testGetDateRangeFromAtTimestamp() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_@timestamp_range");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);
  }

  @Test
  public void testGetDateRangeFromStringValue() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_string_time_range");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1726766654000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1726768454000L);
  }

  @Test
  public void testGetDateRangeFromEpochMillisStringValue() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_epoch_millis_date_range_as_string");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);
  }

  @Test
  public void testGetDateRangeFromGtLt() throws Exception {
    String rawRequest = getRawQueryString("bool_query_with_gt_lt_time_range");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1726766654000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1726768454000L);
  }

  @Test
  public void testGetDateRangeFromStringValueWithTimeZone() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "range": {
              "@timestamp": {
                "gte": "2026-03-10T00:00:00",
                "lte": "2026-03-10T01:00:00",
                "time_zone": "-08:00"
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getStartTimeEpochMs())
        .isEqualTo(OffsetDateTime.parse("2026-03-10T00:00:00-08:00").toInstant().toEpochMilli());
    assertThat(request.getEndTimeEpochMs())
        .isEqualTo(
            OffsetDateTime.parse("2026-03-10T01:00:00.999-08:00").toInstant().toEpochMilli());
  }

  @Test
  public void testGetDateMathRangeFromStringValueWithTimeZone() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "range": {
              "@timestamp": {
                "gte": "2026-03-10||/d",
                "lte": "2026-03-10||/d",
                "format": "strict_date_optional_time",
                "time_zone": "-08:00"
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getStartTimeEpochMs())
        .isEqualTo(OffsetDateTime.parse("2026-03-10T00:00:00-08:00").toInstant().toEpochMilli());
    assertThat(request.getEndTimeEpochMs())
        .isEqualTo(
            OffsetDateTime.parse("2026-03-10T23:59:59.999-08:00").toInstant().toEpochMilli());
  }

  @Test
  public void testSourceIncludesBooleanFilter() throws Exception {
    String rawRequest = getRawQueryString("boolean_source_includes_filter");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap()).isEmpty();
    assertThat(request.getSourceFieldFilter().getIncludeWildcardsCount()).isZero();
    assertThat(request.getSourceFieldFilter().hasIncludeAll()).isTrue();
    assertThat(request.getSourceFieldFilter().getIncludeAll()).isTrue();

    // Assert that the excludes fields are set correctly
    assertThat(request.getSourceFieldFilter().getExcludeFieldsMap()).isEmpty();
    assertThat(request.getSourceFieldFilter().getExcludeWildcardsCount()).isZero();
    assertThat(request.getSourceFieldFilter().hasExcludeAll()).isFalse();

    assertFixturePlainWildcardWasRewritten(rawRequest, request.getQuery());
  }

  @Test
  public void testSourceIncludesListFilter() throws Exception {
    String rawRequest = getRawQueryString("list_source_includes_filter");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap()).hasSize(1);
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap().get("normal_field_test"))
        .isTrue();
    assertThat(request.getSourceFieldFilter().getIncludeWildcardsCount()).isOne();
    assertThat(request.getSourceFieldFilter().getIncludeWildcards(0)).isEqualTo("wildcard_test.*");
    assertThat(request.getSourceFieldFilter().hasIncludeAll()).isFalse();

    // Assert that the excludes fields are set correctly
    assertThat(request.getSourceFieldFilter().getExcludeFieldsMap()).isEmpty();
    assertThat(request.getSourceFieldFilter().getExcludeWildcardsCount()).isZero();
    assertThat(request.getSourceFieldFilter().hasExcludeAll()).isFalse();

    assertFixturePlainWildcardWasRewritten(rawRequest, request.getQuery());
  }

  @Test
  public void testSourceIncludesObjectFilter() throws Exception {
    String rawRequest = getRawQueryString("object_source_includes_filter");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    // Assert that the includes fields are set correctly
    assertThat(request.getSourceFieldFilter().getIncludeFieldsMap()).hasSize(1);
    assertThat(
            request.getSourceFieldFilter().getIncludeFieldsMap().get("include_normal_field_test"))
        .isTrue();
    assertThat(request.getSourceFieldFilter().getIncludeWildcardsCount()).isOne();
    assertThat(request.getSourceFieldFilter().getIncludeWildcards(0))
        .isEqualTo("include_wildcard_test.*");
    assertThat(request.getSourceFieldFilter().hasIncludeAll()).isFalse();

    // Assert that the excludes fields are set correctly
    assertThat(request.getSourceFieldFilter().getExcludeFieldsMap()).hasSize(1);
    assertThat(
            request.getSourceFieldFilter().getExcludeFieldsMap().get("exclude_normal_field_test"))
        .isTrue();
    assertThat(request.getSourceFieldFilter().getExcludeWildcardsCount()).isOne();
    assertThat(request.getSourceFieldFilter().getExcludeWildcards(0))
        .isEqualTo("exclude_wildcard_test.*");
    assertThat(request.getSourceFieldFilter().getExcludeAll()).isFalse();

    assertFixturePlainWildcardWasRewritten(rawRequest, request.getQuery());
  }

  @Test
  public void testNoAggs() throws Exception {
    String rawRequest = getRawQueryString("noaggs");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1680551083859L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1680554683859L);

    assertFixturePlainWildcardWasRewritten(rawRequest, request.getQuery());
  }

  @Test
  public void testGeneralFields() throws Exception {
    String rawRequest = getRawQueryString("datehistogram");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);

    assertThat(parsedRequestList.size()).isEqualTo(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(1);
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1676498801027L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1676500240688L);

    assertFixturePlainWildcardWasRewritten(rawRequest, request.getQuery());
  }

  @Test
  public void testDashboardsDiscoverQuerySupportsEmptyAggsAndDateMath() throws Exception {
    String rawRequest = getRawQueryString("dashboards_discover");
    long beforeParse = Instant.now().toEpochMilli();

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    List<AstraSearch.SearchRequest> parsedRequestList =
        openSearchRequest.parseMultiSearchRequest(rawRequest);
    long afterParse = Instant.now().toEpochMilli();

    assertThat(parsedRequestList).hasSize(1);

    AstraSearch.SearchRequest request = parsedRequestList.get(0);
    assertThat(request.getDataset()).isEqualTo("test");
    assertThat(request.getHowMany()).isEqualTo(500);
    assertThat(request.getAggregationJson()).isEmpty();
    assertThat(request.getStartTimeEpochMs())
        .isBetween(
            beforeParse - ChronoUnit.HOURS.getDuration().toMillis() * 3,
            afterParse - ChronoUnit.HOURS.getDuration().toMillis() * 3);
    assertThat(request.getEndTimeEpochMs()).isBetween(beforeParse, afterParse);

    assertThat(request.getQuery()).isEqualTo(queryFromRequestBody(secondNdjsonLine(rawRequest)));
  }

  @Test
  public void testSingleSearchUsesPathDatasetAndParsesBody() throws Exception {
    String searchBody = getSingleSearchBody("terms");

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    JsonNode parsedRequest = OBJECT_MAPPER.readTree(searchBody);
    assertThat(request.getDataset()).isEqualTo("test");
    assertThat(request.getHowMany()).isEqualTo(parsedRequest.path("size").asInt(10));
    assertThat(request.getAggregationJson()).isEqualTo(parsedRequest.get("aggs").toString());
    assertFixturePlainWildcardWasRewritten(
        """
        {}
        %s
        """
            .formatted(searchBody),
        request.getQuery());
  }

  @Test
  public void testSingleSearchDefaultsDatasetToAll() throws Exception {
    String searchBody = "{\"size\":5,\"query\":{\"match_all\":{}}}";

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request = openSearchRequest.parseSingleSearchRequest("", searchBody);

    assertThat(request.getDataset()).isEqualTo("_all");
    assertThat(request.getHowMany()).isEqualTo(5);
    assertThat(request.getQuery()).isEqualTo("{\"match_all\":{}}");
  }

  @Test
  public void testSimpleWildcardQueryStringIsNormalizedToMatchAll() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "bool": {
              "filter": [
                {
                  "range": {
                    "_timesinceepoch": {
                      "gte": 1773390000000,
                      "lte": 1773410000000,
                      "format": "epoch_millis"
                    }
                  }
                },
                {
                  "query_string": {
                    "query": "*",
                    "analyze_wildcard": true
                  }
                }
              ]
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    JsonNode parsedQuery = OBJECT_MAPPER.readTree(request.getQuery());
    assertThat(parsedQuery.at("/bool/filter/0/range/_timesinceepoch/gte").asLong())
        .isEqualTo(1773390000000L);
    assertThat(parsedQuery.at("/bool/filter/1/match_all").isObject()).isTrue();
  }

  @Test
  public void testWildcardQueryStringIsPreserved() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "bool": {
              "filter": [
                {
                  "range": {
                    "@timestamp": {
                      "gte": "2026-03-10T12:00:00.000Z",
                      "lte": "2026-03-10T13:00:00.000Z"
                    }
                  }
                },
                {
                  "query_string": {
                    "analyze_wildcard": true,
                    "query": "*\uFEFF"
                  }
                }
              ]
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    JsonNode parsedQuery = OBJECT_MAPPER.readTree(request.getQuery());
    assertThat(parsedQuery.at("/bool/filter/1/query_string/query").asText()).isEqualTo("*\uFEFF");
    assertThat(parsedQuery.at("/bool/filter/1/query_string/analyze_wildcard").asBoolean()).isTrue();
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1773144000000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1773147600000L);
  }

  @Test
  public void testWildcardQueryStringWithAdditionalOptionsIsPreserved() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "query_string": {
              "query": "*\uFEFF",
              "fields": ["message"],
              "boost": 2.0
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    JsonNode parsedQuery = OBJECT_MAPPER.readTree(request.getQuery());
    assertThat(parsedQuery.has("query_string")).isTrue();
    assertThat(parsedQuery.at("/query_string/query").asText()).isEqualTo("*\uFEFF");
    assertThat(parsedQuery.at("/query_string/fields/0").asText()).isEqualTo("message");
    assertThat(parsedQuery.at("/query_string/boost").asDouble()).isEqualTo(2.0);
  }

  @Test
  public void testInvalidDateRangeFallsBackToDefaultTimeRange() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "range": {
              "@timestamp": {
                "gte": "not-a-date",
                "lte": "now"
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getStartTimeEpochMs()).isEqualTo(0L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(MAX_TIME);
  }

  @Test
  public void testFloatingPointEpochMillisIsTruncatedToLong() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "range": {
              "@timestamp": {
                "gte": 1773144000000.9,
                "lte": 1773147600000.1
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getStartTimeEpochMs()).isEqualTo(1773144000000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1773147600000L);
  }

  @Test
  public void testTimeRangeOnlyQueryKeepsDocumentLevelFilter() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "range": {
              "@timestamp": {
                "gte": "2026-03-10T12:00:00.000Z",
                "lte": "2026-03-10T13:00:00.000Z"
              }
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getQuery())
        .isEqualTo(OBJECT_MAPPER.readTree(searchBody).get("query").toString());
    assertThat(request.getStartTimeEpochMs()).isEqualTo(1773144000000L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(1773147600000L);
  }

  @Test
  public void testMultipleTimeRangesDoNotSilentlyNarrowChunkSelection() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "bool": {
              "should": [
                {
                  "range": {
                    "@timestamp": {
                      "gte": "2026-03-10T12:00:00.000Z",
                      "lte": "2026-03-10T13:00:00.000Z"
                    }
                  }
                },
                {
                  "range": {
                    "@timestamp": {
                      "gte": "2026-03-10T15:00:00.000Z",
                      "lte": "2026-03-10T16:00:00.000Z"
                    }
                  }
                }
              ]
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getQuery())
        .isEqualTo(OBJECT_MAPPER.readTree(searchBody).get("query").toString());
    assertThat(request.getStartTimeEpochMs()).isEqualTo(0L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(MAX_TIME);
  }

  @Test
  public void testSingleShouldTimeRangeDoesNotSilentlyNarrowChunkSelection() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "bool": {
              "must": [
                {
                  "query_string": {
                    "query": "level:ERROR"
                  }
                }
              ],
              "should": [
                {
                  "range": {
                    "@timestamp": {
                      "gte": "2026-03-10T12:00:00.000Z",
                      "lte": "2026-03-10T13:00:00.000Z"
                    }
                  }
                }
              ]
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getQuery())
        .isEqualTo(OBJECT_MAPPER.readTree(searchBody).get("query").toString());
    assertThat(request.getStartTimeEpochMs()).isEqualTo(0L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(MAX_TIME);
  }

  @Test
  public void testSingleMustNotTimeRangeDoesNotSilentlyNarrowChunkSelection() throws Exception {
    String searchBody =
        """
        {
          "size": 5,
          "query": {
            "bool": {
              "must": [
                {
                  "match_all": {}
                }
              ],
              "must_not": [
                {
                  "range": {
                    "@timestamp": {
                      "gte": "2026-03-10T12:00:00.000Z",
                      "lte": "2026-03-10T13:00:00.000Z"
                    }
                  }
                }
              ]
            }
          }
        }
        """;

    OpenSearchRequest openSearchRequest = new OpenSearchRequest();
    AstraSearch.SearchRequest request =
        openSearchRequest.parseSingleSearchRequest("test", searchBody);

    assertThat(request.getQuery())
        .isEqualTo(OBJECT_MAPPER.readTree(searchBody).get("query").toString());
    assertThat(request.getStartTimeEpochMs()).isEqualTo(0L);
    assertThat(request.getEndTimeEpochMs()).isEqualTo(MAX_TIME);
  }
}
