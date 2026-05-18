package com.slack.astra.elasticsearchApi;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Timestamp;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.AstraLocalQueryService;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Readable direct-API compatibility specs for ClickBench queries.
 *
 * <p>Query numbers follow ClickHouse/ClickBench's {@code clickhouse/queries.sql} ordering.
 *
 * <p>These tests intentionally duplicate lower-level feature tests. Deleting this file should not
 * delete the only coverage for sorting, filtering, aggregations, or pagination.
 */
class ClickBenchCompatibilityTest {
  private static final String S3_TEST_BUCKET = "test-astra-clickbench";
  private static final String DEFAULT_CLUSTER_NAME = "astra";
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 8081;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private static final Instant START = Instant.parse("2026-05-18T05:00:00Z");

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static ClickBenchSpec.Builder clickBench(String queryId) {
    return new ClickBenchSpec.Builder(queryId);
  }

  private static ClickBenchRow.Builder row(int id) {
    return new ClickBenchRow.Builder(id);
  }

  private static ClickBenchExpectation jsonNumber(String path, double expectedValue) {
    return (response, spec) ->
        assertThat(jsonAt(response, path).asDouble())
            .as("%s: %s -> %s", spec.queryId(), spec.expects(), path)
            .isEqualTo(expectedValue);
  }

  private static ClickBenchExpectation sourceValues(String field, String... expectedValues) {
    return (response, spec) ->
        assertThat(sourceValuesFromResponse(response, field))
            .as("%s: %s -> _source.%s", spec.queryId(), spec.expects(), field)
            .containsExactly(expectedValues);
  }

  private static ClickBenchExpectation buckets(String aggregationName, ExpectedBucket... buckets) {
    return (response, spec) -> {
      JsonNode bucketNodes = response.path("aggregations").path(aggregationName).path("buckets");
      assertThat(bucketNodes.size())
          .as("%s: %s -> %s bucket count", spec.queryId(), spec.expects(), aggregationName)
          .isEqualTo(buckets.length);
      for (int i = 0; i < buckets.length; i++) {
        int bucketIndex = i;
        ExpectedBucket bucket = buckets[i];
        JsonNode actualBucket = bucketNodes.get(i);
        assertThat(bucketKey(actualBucket))
            .as("%s: %s -> %s bucket %s key", spec.queryId(), spec.expects(), aggregationName, i)
            .isEqualTo(bucket.key());
        assertThat(actualBucket.path("doc_count").asLong())
            .as(
                "%s: %s -> %s bucket %s doc_count",
                spec.queryId(), spec.expects(), aggregationName, i)
            .isEqualTo(bucket.docCount());
        bucket
            .metricValues()
            .forEach(
                (metricPath, metricValue) ->
                    assertThat(jsonAt(actualBucket, metricPath).asDouble())
                        .as(
                            "%s: %s -> %s bucket %s metric %s",
                            spec.queryId(),
                            spec.expects(),
                            aggregationName,
                            bucketIndex,
                            metricPath)
                        .isCloseTo(metricValue, org.assertj.core.data.Offset.offset(0.001)));
      }
    };
  }

  private static ClickBenchExpectation containsBuckets(
      String aggregationName, ExpectedBucket... buckets) {
    return (response, spec) -> {
      JsonNode bucketNodes = response.path("aggregations").path(aggregationName).path("buckets");
      for (ExpectedBucket bucket : buckets) {
        JsonNode actualBucket = findBucket(bucketNodes, bucket.key());
        assertThat(actualBucket.isMissingNode())
            .as(
                "%s: %s -> %s bucket %s exists",
                spec.queryId(), spec.expects(), aggregationName, bucket.key())
            .isFalse();
        assertThat(actualBucket.path("doc_count").asLong())
            .as(
                "%s: %s -> %s bucket %s doc_count",
                spec.queryId(), spec.expects(), aggregationName, bucket.key())
            .isEqualTo(bucket.docCount());
        bucket
            .metricValues()
            .forEach(
                (metricPath, metricValue) ->
                    assertThat(jsonAt(actualBucket, metricPath).asDouble())
                        .as(
                            "%s: %s -> %s bucket %s metric %s",
                            spec.queryId(),
                            spec.expects(),
                            aggregationName,
                            bucket.key(),
                            metricPath)
                        .isCloseTo(metricValue, org.assertj.core.data.Offset.offset(0.001)));
      }
    };
  }

  private static ExpectedBucket bucket(Object key, long docCount) {
    return new ExpectedBucket(List.of(String.valueOf(key)), docCount, Map.of());
  }

  private static ExpectedBucket bucket(
      Object key, long docCount, Map<String, Double> metricValues) {
    return new ExpectedBucket(List.of(String.valueOf(key)), docCount, metricValues);
  }

  private static ExpectedBucket bucket(List<?> key, long docCount) {
    return new ExpectedBucket(key.stream().map(String::valueOf).toList(), docCount, Map.of());
  }

  private static ExpectedBucket bucket(
      List<?> key, long docCount, Map<String, Double> metricValues) {
    return new ExpectedBucket(key.stream().map(String::valueOf).toList(), docCount, metricValues);
  }

  private static JsonNode jsonAt(JsonNode node, String path) {
    JsonNode current = node;
    for (String part : path.split("\\.")) {
      current =
          part.chars().allMatch(Character::isDigit)
              ? current.path(Integer.parseInt(part))
              : current.path(part);
    }
    return current;
  }

  private static List<String> bucketKey(JsonNode bucket) {
    JsonNode key = bucket.path("key");
    if (key.isArray()) {
      List<String> keyParts = new ArrayList<>();
      key.forEach(keyPart -> keyParts.add(keyPart.asText()));
      return keyParts;
    }
    return List.of(key.asText());
  }

  private static JsonNode findBucket(JsonNode buckets, List<String> key) {
    for (JsonNode bucket : buckets) {
      if (bucketKey(bucket).equals(key)) {
        return bucket;
      }
    }
    return com.fasterxml.jackson.databind.node.MissingNode.getInstance();
  }

  private static Trace.KeyValue keywordField(String key, String value) {
    return Trace.KeyValue.newBuilder()
        .setKey(key)
        .setFieldType(Schema.SchemaFieldType.KEYWORD)
        .setVStr(value)
        .build();
  }

  private static Trace.KeyValue integerField(String key, int value) {
    return Trace.KeyValue.newBuilder()
        .setKey(key)
        .setFieldType(Schema.SchemaFieldType.INTEGER)
        .setVInt32(value)
        .build();
  }

  private static Trace.KeyValue longField(String key, long value) {
    return Trace.KeyValue.newBuilder()
        .setKey(key)
        .setFieldType(Schema.SchemaFieldType.LONG)
        .setVInt64(value)
        .build();
  }

  private static Trace.KeyValue booleanField(String key, boolean value) {
    return Trace.KeyValue.newBuilder()
        .setKey(key)
        .setFieldType(Schema.SchemaFieldType.BOOLEAN)
        .setVBool(value)
        .build();
  }

  private static Trace.KeyValue dateField(String key, Instant value) {
    return Trace.KeyValue.newBuilder()
        .setKey(key)
        .setFieldType(Schema.SchemaFieldType.DATE)
        .setVDate(
            Timestamp.newBuilder().setSeconds(value.getEpochSecond()).setNanos(value.getNano()))
        .build();
  }

  private static List<ClickBenchRow> hitRows() {
    return List.of(
        row(1)
            .eventTime(START.plusSeconds(3))
            .url("https://mail.google.example")
            .searchPhrase("zulu")
            .build(),
        row(2).eventTime(START.plusSeconds(1)).url("https://slack.example").build(),
        row(3)
            .eventTime(START.plusSeconds(2))
            .url("https://google.example/search")
            .searchPhrase("delta")
            .build(),
        row(4)
            .eventTime(START.plusSeconds(4))
            .url("https://astra.example")
            .searchPhrase("bravo")
            .build(),
        row(5).eventTime(START.plusSeconds(5)).url("https://google.example/maps").build(),
        row(6)
            .eventTime(START.plusSeconds(6))
            .url("https://astra.example/docs")
            .searchPhrase("alpha")
            .build());
  }

  private static List<ClickBenchRow> hitRowsWithEventTimeTie() {
    return List.of(
        row(1)
            .eventTime(START.plusSeconds(3))
            .url("https://mail.google.example")
            .searchPhrase("zulu")
            .build(),
        row(2).eventTime(START.plusSeconds(1)).url("https://slack.example").build(),
        row(3)
            .eventTime(START.plusSeconds(2))
            .url("https://google.example/search")
            .searchPhrase("delta")
            .build(),
        row(4)
            .eventTime(START.plusSeconds(4))
            .url("https://astra.example")
            .searchPhrase("bravo")
            .build(),
        row(5).eventTime(START.plusSeconds(5)).url("https://google.example/maps").build(),
        row(6)
            .eventTime(START.plusSeconds(2))
            .url("https://astra.example/docs")
            .searchPhrase("alpha")
            .build());
  }

  private static List<ClickBenchRow> benchmarkRows() {
    return List.of(
        row(1)
            .eventTime(Instant.parse("2013-07-01T00:00:00Z"))
            .eventDate("2013-07-01T00:00:00Z")
            .advEngineId(0)
            .resolutionWidth(100)
            .userId(100)
            .regionId(1)
            .mobilePhone(1)
            .mobilePhoneModel("iPhone")
            .searchPhrase("alpha")
            .searchEngineId(1)
            .url("https://google.example/a")
            .title("Google Alpha")
            .counterId(62)
            .isLink(1)
            .build(),
        row(2)
            .eventTime(Instant.parse("2013-07-01T00:01:00Z"))
            .eventDate("2013-07-01T00:00:00Z")
            .advEngineId(2)
            .resolutionWidth(200)
            .userId(101)
            .regionId(1)
            .mobilePhone(1)
            .mobilePhoneModel("iPhone")
            .searchPhrase("alpha")
            .searchEngineId(1)
            .url("https://mail.google.example/b")
            .title("Alpha Result")
            .counterId(62)
            .isLink(1)
            .build(),
        row(3)
            .eventTime(Instant.parse("2013-07-02T00:02:00Z"))
            .eventDate("2013-07-02T00:00:00Z")
            .advEngineId(2)
            .resolutionWidth(300)
            .userId(101)
            .regionId(1)
            .mobilePhone(1)
            .mobilePhoneModel("iPhone")
            .searchPhrase("beta")
            .searchEngineId(2)
            .url("https://google.example/c")
            .title("Google Beta")
            .counterId(62)
            .isLink(1)
            .build(),
        row(4)
            .eventTime(Instant.parse("2013-07-02T00:03:00Z"))
            .eventDate("2013-07-02T00:00:00Z")
            .advEngineId(3)
            .resolutionWidth(400)
            .userId(102)
            .regionId(2)
            .mobilePhone(2)
            .mobilePhoneModel("Android")
            .searchPhrase("beta")
            .searchEngineId(2)
            .url("https://astra.example/d")
            .title("Google Beta")
            .counterId(62)
            .isLink(1)
            .build(),
        row(5)
            .eventTime(Instant.parse("2013-07-03T00:04:00Z"))
            .eventDate("2013-07-03T00:00:00Z")
            .advEngineId(3)
            .resolutionWidth(500)
            .userId(103)
            .regionId(2)
            .mobilePhone(2)
            .mobilePhoneModel("Android")
            .searchPhrase("gamma")
            .searchEngineId(2)
            .url("https://google.example/e")
            .title("Other")
            .counterId(62)
            .isRefresh(true)
            .isLink(1)
            .build(),
        row(6)
            .eventTime(Instant.parse("2013-07-03T00:05:00Z"))
            .eventDate("2013-07-03T00:00:00Z")
            .advEngineId(3)
            .resolutionWidth(600)
            .userId(104)
            .regionId(2)
            .mobilePhone(2)
            .mobilePhoneModel("Android")
            .counterId(61)
            .build(),
        row(7)
            .eventTime(Instant.parse("2013-07-14T00:06:00Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .advEngineId(4)
            .resolutionWidth(700)
            .userId(105)
            .regionId(3)
            .mobilePhone(3)
            .mobilePhoneModel("Pixel")
            .searchPhrase("gamma")
            .searchEngineId(1)
            .url("https://example.org/f")
            .title("Other")
            .counterId(62)
            .dontCountHits(true)
            .build(),
        row(8)
            .eventTime(Instant.parse("2013-07-15T00:07:00Z"))
            .eventDate("2013-07-15T00:00:00Z")
            .advEngineId(4)
            .resolutionWidth(800)
            .userId(106)
            .regionId(3)
            .mobilePhone(3)
            .mobilePhoneModel("Pixel")
            .url("https://google.example/f")
            .title("Google F")
            .counterId(62)
            .isLink(1)
            .build(),
        row(9)
            .eventTime(Instant.parse("2013-07-15T00:08:00Z"))
            .eventDate("2013-07-15T00:00:00Z")
            .advEngineId(5)
            .resolutionWidth(900)
            .userId(435090932899640449L)
            .regionId(2)
            .mobilePhone(2)
            .mobilePhoneModel("Android")
            .searchPhrase("gamma")
            .searchEngineId(1)
            .url("https://slack.example/g")
            .title("Slack")
            .counterId(62)
            .isLink(1)
            .build());
  }

  private static List<ClickBenchRow> advEngineRows() {
    return List.of(
        row(1).advEngineId(3).build(),
        row(2).advEngineId(3).build(),
        row(3).advEngineId(3).build(),
        row(4).advEngineId(2).build(),
        row(5).advEngineId(2).build(),
        row(6).advEngineId(1).build(),
        row(7).advEngineId(0).build());
  }

  private static List<ClickBenchRow> urlRows() {
    return List.of(
        row(1).url("https://popular.example").title("Popular").build(),
        row(2).url("https://popular.example").title("Popular").build(),
        row(3).url("https://popular.example").title("Popular").build(),
        row(4).url("https://middle.example").title("Middle").build(),
        row(5).url("https://middle.example").title("Middle").build(),
        row(6).url("https://rare.example").title("Rare").build());
  }

  private static List<ClickBenchRow> urlRowsForPageViews() {
    return List.of(
        row(1)
            .eventTime(Instant.parse("2013-07-14T00:00:01Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://page.example/a")
            .title("Title A")
            .build(),
        row(2)
            .eventTime(Instant.parse("2013-07-14T00:00:02Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://page.example/a")
            .title("Title A")
            .build(),
        row(3)
            .eventTime(Instant.parse("2013-07-14T00:01:01Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://page.example/a")
            .title("Title A")
            .build(),
        row(4)
            .eventTime(Instant.parse("2013-07-14T00:01:02Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://page.example/b")
            .title("Title B")
            .build(),
        row(5)
            .eventTime(Instant.parse("2013-07-14T00:01:03Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://page.example/b")
            .title("Title B")
            .build(),
        row(6)
            .eventTime(Instant.parse("2013-07-14T00:02:01Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://excluded-refresh.example")
            .title("Excluded Refresh")
            .isRefresh(true)
            .build(),
        row(7)
            .eventTime(Instant.parse("2013-07-14T00:03:01Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .counterId(62)
            .url("https://excluded-dont-count.example")
            .title("Excluded Dont Count")
            .dontCountHits(true)
            .build(),
        row(8)
            .eventTime(Instant.parse("2013-08-01T00:00:01Z"))
            .eventDate("2013-08-01T00:00:00Z")
            .counterId(62)
            .url("https://excluded-date.example")
            .title("Excluded Date")
            .build());
  }

  private ElasticsearchApiService elasticsearchApiService;
  private SimpleMeterRegistry metricsRegistry;
  private ChunkManagerUtil<LogMessage> chunkManagerUtil;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        ChunkManagerUtil.makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            1000000L,
            AstraConfigUtil.makeIndexerConfig());
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    AstraLocalQueryService<LogMessage> searcher =
        new AstraLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3));
    elasticsearchApiService =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    chunkManagerUtil.close();
    metricsRegistry.close();
  }

  @Test
  public void q1CountAllRows() throws Exception {
    verify(
        clickBench("Q1")
            .expects("count all rows")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "row_count": {
                      "value_count": {
                        "field": "@timestamp"
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(jsonNumber("aggregations.row_count.value", 9)));
  }

  @Test
  public void q2CountRowsWhereAdvEngineIdIsNotZero() throws Exception {
    verify(
        clickBench("Q2")
            .expects("count rows where AdvEngineID is non-zero")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "AdvEngineID": 0
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "row_count": {
                      "value_count": {
                        "field": "@timestamp"
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(jsonNumber("aggregations.row_count.value", 8)));
  }

  @Test
  public void q3SumCountAndAverage() throws Exception {
    verify(
        clickBench("Q3")
            .expects("sum AdvEngineID, count rows, and average ResolutionWidth")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "adv_engine_sum": {
                      "sum": {
                        "field": "AdvEngineID"
                      }
                    },
                    "row_count": {
                      "value_count": {
                        "field": "@timestamp"
                      }
                    },
                    "avg_resolution_width": {
                      "avg": {
                        "field": "ResolutionWidth"
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                jsonNumber("aggregations.adv_engine_sum.value", 26),
                jsonNumber("aggregations.row_count.value", 9),
                jsonNumber("aggregations.avg_resolution_width.value", 500)));
  }

  @Test
  public void q4AverageUserId() throws Exception {
    verify(
        clickBench("Q4")
            .expects("average UserID")
            .givenRows(
                List.of(
                    row(1).userId(10).build(),
                    row(2).userId(20).build(),
                    row(3).userId(30).build()))
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "avg_user_id": {
                      "avg": {
                        "field": "UserID"
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(jsonNumber("aggregations.avg_user_id.value", 20)));
  }

  @Test
  public void q7MinAndMaxEventDate() throws Exception {
    verify(
        clickBench("Q7")
            .expects("minimum and maximum EventDate")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "min_event_date": {
                      "min": {
                        "field": "EventDate"
                      }
                    },
                    "max_event_date": {
                      "max": {
                        "field": "EventDate"
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                jsonNumber(
                    "aggregations.min_event_date.value",
                    Instant.parse("2013-07-01T00:00:00Z").toEpochMilli()),
                jsonNumber(
                    "aggregations.max_event_date.value",
                    Instant.parse("2013-07-15T00:00:00Z").toEpochMilli())));
  }

  @Test
  public void q8AdvEngineIdCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q8")
            .expects("AdvEngineID buckets for non-zero values, ordered by count descending")
            .givenRows(advEngineRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "AdvEngineID": 0
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "adv_engines": {
                      "terms": {
                        "field": "AdvEngineID",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets("adv_engines", bucket(3, 3), bucket(2, 2), bucket(1, 1))));
  }

  @Test
  public void q9RegionIdOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q9")
            .expects("RegionID buckets ordered by distinct UserID count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "regions": {
                      "terms": {
                        "field": "RegionID",
                        "size": 10,
                        "order": {
                          "users": "desc"
                        }
                      },
                      "aggs": {
                        "users": {
                          "cardinality": {
                            "field": "UserID"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "regions",
                    bucket(2, 4, Map.of("users.value", 4.0)),
                    bucket(1, 3, Map.of("users.value", 2.0)),
                    bucket(3, 2, Map.of("users.value", 2.0)))));
  }

  @Test
  public void q10RegionIdWithSiblingMetricsOrderedByCount() throws Exception {
    verify(
        clickBench("Q10")
            .expects("RegionID buckets with sum, count, average, and distinct-user metrics")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "regions": {
                      "terms": {
                        "field": "RegionID",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "adv_engine_sum": {
                          "sum": {
                            "field": "AdvEngineID"
                          }
                        },
                        "avg_resolution_width": {
                          "avg": {
                            "field": "ResolutionWidth"
                          }
                        },
                        "users": {
                          "cardinality": {
                            "field": "UserID"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "regions",
                    bucket(
                        2,
                        4,
                        Map.of(
                            "adv_engine_sum.value",
                            14.0,
                            "avg_resolution_width.value",
                            600.0,
                            "users.value",
                            4.0)),
                    bucket(
                        1,
                        3,
                        Map.of(
                            "adv_engine_sum.value",
                            4.0,
                            "avg_resolution_width.value",
                            200.0,
                            "users.value",
                            2.0)),
                    bucket(
                        3,
                        2,
                        Map.of(
                            "adv_engine_sum.value",
                            8.0,
                            "avg_resolution_width.value",
                            750.0,
                            "users.value",
                            2.0)))));
  }

  @Test
  public void q11MobilePhoneModelOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q11")
            .expects("non-empty MobilePhoneModel buckets ordered by distinct UserID count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "MobilePhoneModel": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "models": {
                      "terms": {
                        "field": "MobilePhoneModel",
                        "size": 10,
                        "order": {
                          "users": "desc"
                        }
                      },
                      "aggs": {
                        "users": {
                          "cardinality": {
                            "field": "UserID"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "models",
                    bucket("Android", 4, Map.of("users.value", 4.0)),
                    bucket("iPhone", 3, Map.of("users.value", 2.0)),
                    bucket("Pixel", 2, Map.of("users.value", 2.0)))));
  }

  @Test
  public void q12MobilePhoneAndModelOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q12")
            .expects("MobilePhone plus MobilePhoneModel buckets ordered by distinct UserID count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "MobilePhoneModel": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "phones": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "MobilePhone"
                          },
                          {
                            "field": "MobilePhoneModel"
                          }
                        ],
                        "size": 10,
                        "order": {
                          "users": "desc"
                        }
                      },
                      "aggs": {
                        "users": {
                          "cardinality": {
                            "field": "UserID"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "phones",
                    bucket(List.of(2, "Android"), 4, Map.of("users.value", 4.0)),
                    bucket(List.of(1, "iPhone"), 3, Map.of("users.value", 2.0)),
                    bucket(List.of(3, "Pixel"), 2, Map.of("users.value", 2.0)))));
  }

  @Test
  public void q13SearchPhraseCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q13")
            .expects("non-empty SearchPhrase buckets ordered by count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "phrases": {
                      "terms": {
                        "field": "SearchPhrase",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "phrases", bucket("gamma", 3), bucket("alpha", 2), bucket("beta", 2))));
  }

  @Test
  public void q14SearchPhraseOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q14")
            .expects("non-empty SearchPhrase buckets ordered by distinct UserID count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "phrases": {
                      "terms": {
                        "field": "SearchPhrase",
                        "size": 10,
                        "order": {
                          "users": "desc"
                        }
                      },
                      "aggs": {
                        "users": {
                          "cardinality": {
                            "field": "UserID"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "phrases",
                    bucket("gamma", 3, Map.of("users.value", 3.0)),
                    bucket("alpha", 2, Map.of("users.value", 2.0)),
                    bucket("beta", 2, Map.of("users.value", 2.0)))));
  }

  @Test
  public void q15SearchEngineAndPhraseCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q15")
            .expects("SearchEngineID plus SearchPhrase buckets ordered by count")
            .givenRows(
                List.of(
                    row(1).searchEngineId(1).searchPhrase("alpha").build(),
                    row(2).searchEngineId(1).searchPhrase("alpha").build(),
                    row(3).searchEngineId(1).searchPhrase("alpha").build(),
                    row(4).searchEngineId(2).searchPhrase("beta").build(),
                    row(5).searchEngineId(2).searchPhrase("beta").build(),
                    row(6).searchEngineId(1).searchPhrase("gamma").build()))
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "engine_phrases": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "SearchEngineID"
                          },
                          {
                            "field": "SearchPhrase"
                          }
                        ],
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "engine_phrases",
                    bucket(List.of(1, "alpha"), 3),
                    bucket(List.of(2, "beta"), 2),
                    bucket(List.of(1, "gamma"), 1))));
  }

  @Test
  public void q16UserIdCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q16")
            .expects("UserID buckets ordered by count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "users": {
                      "terms": {
                        "field": "UserID",
                        "size": 1,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(containsBuckets("users", bucket(101, 2))));
  }

  @Test
  public void q20FindSpecificUserId() throws Exception {
    verify(
        clickBench("Q20")
            .expects("return hits for a specific UserID")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 10,
                  "query": {
                    "term": {
                      "UserID": 435090932899640449
                    }
                  }
                }
                """)
            .thenResponseContains(sourceValues("SearchPhrase", "gamma")));
  }

  @Test
  public void q22GoogleUrlSearchPhrasesOrderedByCount() throws Exception {
    verify(
        clickBench("Q22")
            .expects("SearchPhrase buckets for google URLs, ordered by count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "filter": [
                        {
                          "wildcard": {
                            "URL": {
                              "value": "*google*"
                            }
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "phrases": {
                      "terms": {
                        "field": "SearchPhrase",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "phrases", bucket("alpha", 2), bucket("beta", 1), bucket("gamma", 1))));
  }

  @Test
  public void q23GoogleTitleWithoutGoogleSubdomainSearchPhrasesOrderedByCount() throws Exception {
    verify(
        clickBench("Q23")
            .expects("SearchPhrase buckets for Google titles outside google subdomains")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "filter": [
                        {
                          "wildcard": {
                            "Title": {
                              "value": "*Google*"
                            }
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "wildcard": {
                            "URL": {
                              "value": "*.google.*"
                            }
                          }
                        },
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "phrases": {
                      "terms": {
                        "field": "SearchPhrase",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets("phrases", bucket("beta", 2), bucket("alpha", 1))));
  }

  @Test
  public void q24UrlContainsGoogleOrderByEventTimeLimit10() throws Exception {
    verify(
        clickBench("Q24")
            .expects("URL contains google, ordered by EventTime ascending, limited to 10 hits")
            .givenRows(hitRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 10,
                  "query": {
                    "wildcard": {
                      "URL": {
                        "value": "*google*"
                      }
                    }
                  },
                  "sort": [
                    {
                      "@timestamp": {
                        "order": "asc"
                      }
                    }
                  ]
                }
                """)
            .thenResponseContains(
                sourceValues(
                    "URL",
                    "https://google.example/search",
                    "https://mail.google.example",
                    "https://google.example/maps")));
  }

  @Test
  public void q25SearchPhraseNotEmptyOrderByEventTimeLimit10() throws Exception {
    verify(
        clickBench("Q25")
            .expects(
                "non-empty SearchPhrase values, ordered by EventTime ascending, limited to 10 hits")
            .givenRows(hitRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 10,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "sort": [
                    {
                      "@timestamp": {
                        "order": "asc"
                      }
                    }
                  ]
                }
                """)
            .thenResponseContains(sourceValues("SearchPhrase", "delta", "zulu", "bravo", "alpha")));
  }

  @Test
  public void q26SearchPhraseNotEmptyOrderBySearchPhraseLimit10() throws Exception {
    verify(
        clickBench("Q26")
            .expects(
                "non-empty SearchPhrase values, ordered by SearchPhrase ascending, limited to 10"
                    + " hits")
            .givenRows(hitRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 10,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "sort": [
                    {
                      "SearchPhrase": {
                        "order": "asc"
                      }
                    }
                  ]
                }
                """)
            .thenResponseContains(sourceValues("SearchPhrase", "alpha", "bravo", "delta", "zulu")));
  }

  @Test
  public void q27SearchPhraseNotEmptyOrderByEventTimeThenSearchPhraseLimit10() throws Exception {
    verify(
        clickBench("Q27")
            .expects(
                "non-empty SearchPhrase values, ordered by EventTime ascending and SearchPhrase"
                    + " ascending, limited to 10 hits")
            .givenRows(hitRowsWithEventTimeTie())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 10,
                  "query": {
                    "bool": {
                      "must_not": [
                        {
                          "term": {
                            "SearchPhrase": ""
                          }
                        }
                      ]
                    }
                  },
                  "sort": [
                    {
                      "@timestamp": {
                        "order": "asc"
                      }
                    },
                    {
                      "SearchPhrase": {
                        "order": "asc"
                      }
                    }
                  ]
                }
                """)
            .thenResponseContains(sourceValues("SearchPhrase", "alpha", "delta", "zulu", "bravo")));
  }

  @Test
  public void q30ManySiblingResolutionWidthSums() throws Exception {
    verify(
        clickBench("Q30")
            .expects("many sibling SUM expressions over ResolutionWidth")
            .givenRows(
                List.of(row(1).resolutionWidth(10).build(), row(2).resolutionWidth(20).build()))
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "sum_width_plus_0": {
                      "sum": {
                        "script": {
                          "source": "doc['ResolutionWidth'].value"
                        }
                      }
                    },
                    "sum_width_plus_1": {
                      "sum": {
                        "script": {
                          "source": "doc['ResolutionWidth'].value + 1"
                        }
                      }
                    },
                    "sum_width_plus_2": {
                      "sum": {
                        "script": {
                          "source": "doc['ResolutionWidth'].value + 2"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                jsonNumber("aggregations.sum_width_plus_0.value", 30),
                jsonNumber("aggregations.sum_width_plus_1.value", 32),
                jsonNumber("aggregations.sum_width_plus_2.value", 34)));
  }

  @Test
  public void q34UrlCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q34")
            .expects("URL buckets ordered by count")
            .givenRows(urlRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "urls": {
                      "terms": {
                        "field": "URL",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "urls",
                    bucket("https://popular.example", 3),
                    bucket("https://middle.example", 2),
                    bucket("https://rare.example", 1))));
  }

  @Test
  public void q35ConstantAndUrlCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q35")
            .expects("constant-one plus URL buckets ordered by count")
            .givenRows(urlRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "constant_urls": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "ConstantOne"
                          },
                          {
                            "field": "URL"
                          }
                        ],
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                containsBuckets(
                    "constant_urls",
                    bucket(List.of(1, "https://popular.example"), 3),
                    bucket(List.of(1, "https://middle.example"), 2),
                    bucket(List.of(1, "https://rare.example"), 1))));
  }

  @Test
  public void q37PageViewUrlsForCounterAndDateRange() throws Exception {
    verify(
        clickBench("Q37")
            .expects(
                "URL page-view buckets after counter, date, refresh, hit-count, and URL filters")
            .givenRows(urlRowsForPageViews())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "filter": [
                        {
                          "term": {
                            "CounterID": 62
                          }
                        },
                        {
                          "range": {
                            "EventDate": {
                              "gte": "2013-07-01T00:00:00Z",
                              "lte": "2013-07-31T23:59:59Z"
                            }
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "DontCountHits": true
                          }
                        },
                        {
                          "term": {
                            "IsRefresh": true
                          }
                        },
                        {
                          "term": {
                            "URL": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "urls": {
                      "terms": {
                        "field": "URL",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                buckets(
                    "urls",
                    bucket("https://page.example/a", 3),
                    bucket("https://page.example/b", 2))));
  }

  @Test
  public void q38PageViewTitlesForCounterAndDateRange() throws Exception {
    verify(
        clickBench("Q38")
            .expects(
                "Title page-view buckets after counter, date, refresh, hit-count, and Title filters")
            .givenRows(urlRowsForPageViews())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "filter": [
                        {
                          "term": {
                            "CounterID": 62
                          }
                        },
                        {
                          "range": {
                            "EventDate": {
                              "gte": "2013-07-01T00:00:00Z",
                              "lte": "2013-07-31T23:59:59Z"
                            }
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "DontCountHits": true
                          }
                        },
                        {
                          "term": {
                            "IsRefresh": true
                          }
                        },
                        {
                          "term": {
                            "Title": ""
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "titles": {
                      "terms": {
                        "field": "Title",
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(buckets("titles", bucket("Title A", 3), bucket("Title B", 2))));
  }

  @Test
  public void q43MinuteBucketsForCounterAndDateRange() throws Exception {
    verify(
        clickBench("Q43")
            .expects("minute buckets for counter, date, refresh, and hit-count filters")
            .givenRows(urlRowsForPageViews())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "bool": {
                      "filter": [
                        {
                          "term": {
                            "CounterID": 62
                          }
                        },
                        {
                          "range": {
                            "EventDate": {
                              "gte": "2013-07-14T00:00:00Z",
                              "lte": "2013-07-15T23:59:59Z"
                            }
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "DontCountHits": true
                          }
                        },
                        {
                          "term": {
                            "IsRefresh": true
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "minutes": {
                      "date_histogram": {
                        "field": "@timestamp",
                        "interval": "1m",
                        "min_doc_count": 1
                      }
                    }
                  }
                }
                """)
            .thenResponseContains(
                buckets(
                    "minutes",
                    bucket(Instant.parse("2013-07-14T00:00:00Z").toEpochMilli(), 2),
                    bucket(Instant.parse("2013-07-14T00:01:00Z").toEpochMilli(), 3))));
  }

  private void verify(ClickBenchSpec spec) throws Exception {
    addRows(spec.rows());
    JsonNode response = searchJson(spec.requestJson());
    spec.expectations().forEach(expectation -> expectation.verify(response, spec));
  }

  private void addRows(List<ClickBenchRow> rows) throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    int offset = 1;
    for (ClickBenchRow row : rows) {
      Trace.Span span = span(row);
      chunkManager.addMessage(span, span.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
  }

  private Trace.Span span(ClickBenchRow row) {
    return SpanUtil.makeSpan(
        row.id(),
        "clickbench-message-" + row.id(),
        row.eventTime(),
        List.of(
            integerField("AdvEngineID", row.advEngineId()),
            integerField("ResolutionWidth", row.resolutionWidth()),
            longField("UserID", row.userId()),
            dateField("EventDate", row.eventDate()),
            integerField("RegionID", row.regionId()),
            integerField("MobilePhone", row.mobilePhone()),
            keywordField("MobilePhoneModel", row.mobilePhoneModel()),
            keywordField("SearchPhrase", row.searchPhrase()),
            integerField("SearchEngineID", row.searchEngineId()),
            keywordField("URL", row.url()),
            keywordField("Title", row.title()),
            integerField("CounterID", row.counterId()),
            booleanField("DontCountHits", row.dontCountHits()),
            booleanField("IsRefresh", row.isRefresh()),
            integerField("IsLink", row.isLink()),
            integerField("IsDownload", row.isDownload()),
            integerField("TraficSourceID", row.traficSourceId()),
            longField("RefererHash", row.refererHash()),
            longField("URLHash", row.urlHash()),
            integerField("WindowClientWidth", row.windowClientWidth()),
            integerField("WindowClientHeight", row.windowClientHeight()),
            integerField("ConstantOne", 1)));
  }

  private JsonNode searchJson(String postBody) throws Exception {
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    return OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));
  }

  private static List<String> sourceValuesFromResponse(JsonNode searchResponse, String field) {
    List<String> values = new ArrayList<>();
    for (JsonNode hit : searchResponse.path("hits").path("hits")) {
      values.add(hit.path("_source").path(field).asText());
    }
    return values;
  }

  private interface ClickBenchExpectation {
    void verify(JsonNode response, ClickBenchSpec spec);
  }

  private record ExpectedBucket(
      List<String> key, long docCount, Map<String, Double> metricValues) {}

  private record ClickBenchRow(
      int id,
      Instant eventTime,
      int advEngineId,
      int resolutionWidth,
      long userId,
      Instant eventDate,
      int regionId,
      int mobilePhone,
      String mobilePhoneModel,
      String searchPhrase,
      int searchEngineId,
      String url,
      String title,
      int counterId,
      boolean dontCountHits,
      boolean isRefresh,
      int isLink,
      int isDownload,
      int traficSourceId,
      long refererHash,
      long urlHash,
      int windowClientWidth,
      int windowClientHeight) {
    private static final class Builder {
      private final int id;
      private Instant eventTime = START;
      private int advEngineId;
      private int resolutionWidth = 1;
      private long userId = 1L;
      private Instant eventDate = Instant.parse("2013-07-01T00:00:00Z");
      private int regionId;
      private int mobilePhone;
      private String mobilePhoneModel = "";
      private String searchPhrase = "";
      private int searchEngineId;
      private String url = "";
      private String title = "";
      private int counterId;
      private boolean dontCountHits;
      private boolean isRefresh;
      private int isLink;
      private int isDownload;
      private int traficSourceId;
      private long refererHash = 3594120000172545465L;
      private long urlHash = 2868770270353813622L;
      private int windowClientWidth = 1024;
      private int windowClientHeight = 768;

      private Builder(int id) {
        this.id = id;
      }

      private Builder eventTime(Instant eventTime) {
        this.eventTime = eventTime;
        return this;
      }

      private Builder eventDate(String eventDate) {
        this.eventDate = Instant.parse(eventDate);
        return this;
      }

      private Builder advEngineId(int advEngineId) {
        this.advEngineId = advEngineId;
        return this;
      }

      private Builder resolutionWidth(int resolutionWidth) {
        this.resolutionWidth = resolutionWidth;
        return this;
      }

      private Builder userId(long userId) {
        this.userId = userId;
        return this;
      }

      private Builder regionId(int regionId) {
        this.regionId = regionId;
        return this;
      }

      private Builder mobilePhone(int mobilePhone) {
        this.mobilePhone = mobilePhone;
        return this;
      }

      private Builder mobilePhoneModel(String mobilePhoneModel) {
        this.mobilePhoneModel = mobilePhoneModel;
        return this;
      }

      private Builder searchPhrase(String searchPhrase) {
        this.searchPhrase = searchPhrase;
        return this;
      }

      private Builder searchEngineId(int searchEngineId) {
        this.searchEngineId = searchEngineId;
        return this;
      }

      private Builder url(String url) {
        this.url = url;
        return this;
      }

      private Builder title(String title) {
        this.title = title;
        return this;
      }

      private Builder counterId(int counterId) {
        this.counterId = counterId;
        return this;
      }

      private Builder dontCountHits(boolean dontCountHits) {
        this.dontCountHits = dontCountHits;
        return this;
      }

      private Builder isRefresh(boolean isRefresh) {
        this.isRefresh = isRefresh;
        return this;
      }

      private Builder isLink(int isLink) {
        this.isLink = isLink;
        return this;
      }

      private ClickBenchRow build() {
        return new ClickBenchRow(
            id,
            eventTime,
            advEngineId,
            resolutionWidth,
            userId,
            eventDate,
            regionId,
            mobilePhone,
            mobilePhoneModel,
            searchPhrase,
            searchEngineId,
            url,
            title,
            counterId,
            dontCountHits,
            isRefresh,
            isLink,
            isDownload,
            traficSourceId,
            refererHash,
            urlHash,
            windowClientWidth,
            windowClientHeight);
      }
    }
  }

  private record ClickBenchSpec(
      String queryId,
      String expects,
      List<ClickBenchRow> rows,
      String requestJson,
      List<ClickBenchExpectation> expectations) {
    private static final class Builder {
      private final String queryId;
      private String expects;
      private List<ClickBenchRow> rows;
      private String requestJson;

      private Builder(String queryId) {
        this.queryId = queryId;
      }

      private Builder expects(String expects) {
        this.expects = expects;
        return this;
      }

      private Builder givenRows(List<ClickBenchRow> rows) {
        this.rows = rows;
        return this;
      }

      private Builder whenAstraReceivesEquivalentOpenSearch(String requestJson) {
        this.requestJson = requestJson;
        return this;
      }

      private ClickBenchSpec thenResponseContains(ClickBenchExpectation... expectations) {
        return new ClickBenchSpec(queryId, expects, rows, requestJson, List.of(expectations));
      }
    }
  }
}
