package com.slack.astra.server;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.ZK_PATH_PREFIX;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.TestKafkaServer.produceMessagesToKafka;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.chunkManager.RollOverChunkTask;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * End-to-end compatibility specs for ClickBench queries.
 *
 * <p>Query numbers match benchmark.clickhouse.com labels, which are zero-based over ClickBench's
 * {@code clickhouse/queries.sql} ordering.
 *
 * <p>These tests boot real Kafka, indexer, and query services, then query the OpenSearch-compatible
 * HTTP API. They intentionally duplicate lower-level feature tests because compatibility failures
 * often happen in cross-service request parsing, fan-out, reduction, and response shaping.
 */
@EnabledIfSystemProperty(named = "astra.clickbench.compat", matches = "true")
class ClickBenchCompatibilityTest {
  private static final String S3_TEST_BUCKET = "test-astra-clickbench";
  private static final String TEST_KAFKA_TOPIC = "clickbench-test-topic";
  private static final String TEST_KAFKA_CLIENT = "clickbench-test-client";
  private static final int QUERY_SERVICE_PORT = 18881;
  private static final int INDEXER_SERVICE_PORT = 19000;
  private static final int RECOVERY_SERVICE_PORT = 19003;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
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

  private static ArrayRule orderedArrayBy(String path, SortKey... sortKeys) {
    return ArrayRule.ordered(path, List.of(sortKeys));
  }

  private static ArrayRule orderedBucketsBy(String path, SortKey... sortKeys) {
    return orderedArrayBy(path, sortKeys);
  }

  private static SortKey ascending(String path) {
    return new SortKey(path, SortDirection.ASC);
  }

  private static SortKey descending(String path) {
    return new SortKey(path, SortDirection.DESC);
  }

  private static ClickBenchRow.Builder row(int id) {
    return new ClickBenchRow.Builder(id);
  }

  private static void assertExpectedResponseRules(
      ExpectedResponse expectedResponse, JsonNode expectedJson, String assertionPath) {
    for (ArrayRule rule : expectedResponse.arrayRules()) {
      if (rule.sortKeys().isEmpty()) {
        continue;
      }

      JsonNode expectedArray = requireJsonPath(expectedJson, rule.path(), assertionPath);
      assertThat(expectedArray.isArray())
          .as("%s.%s is an expected array", assertionPath, rule.path())
          .isTrue();
      for (int i = 1; i < expectedArray.size(); i++) {
        String bucketPath = assertionPath + "." + rule.path();
        int comparison =
            compareBySortKeys(
                expectedArray.get(i - 1), expectedArray.get(i), rule.sortKeys(), bucketPath);
        assertThat(comparison)
            .as(
                "%s expected buckets at indexes %s and %s are ordered by %s",
                bucketPath, i - 1, i, rule.describeSort())
            .isLessThan(0);
      }
    }
  }

  private static void assertJsonContains(JsonNode actual, JsonNode expected, String path) {
    if (expected.isObject()) {
      Iterator<Entry<String, JsonNode>> fields = expected.fields();
      while (fields.hasNext()) {
        Entry<String, JsonNode> field = fields.next();
        String childAssertionPath = path + "." + field.getKey();
        assertThat(actual.has(field.getKey())).as("%s exists", childAssertionPath).isTrue();
        assertJsonContains(actual.get(field.getKey()), field.getValue(), childAssertionPath);
      }
      return;
    }

    if (expected.isArray()) {
      assertThat(actual.isArray()).as("%s is an array", path).isTrue();
      assertThat(actual.size()).as("%s array size", path).isEqualTo(expected.size());
      for (int i = 0; i < expected.size(); i++) {
        assertJsonContains(actual.get(i), expected.get(i), path + "[" + i + "]");
      }
      return;
    }

    if (expected.isNumber()) {
      assertThat(actual.isNumber()).as("%s is a number", path).isTrue();
      assertThat(actual.asDouble())
          .as(path)
          .isCloseTo(expected.asDouble(), org.assertj.core.data.Offset.offset(0.001));
      return;
    }

    assertThat(actual).as(path).isEqualTo(expected);
  }

  private static int compareBySortKeys(
      JsonNode left, JsonNode right, List<SortKey> sortKeys, String assertionPath) {
    for (SortKey sortKey : sortKeys) {
      JsonNode leftValue = requireJsonPath(left, sortKey.path(), assertionPath + " left bucket");
      JsonNode rightValue = requireJsonPath(right, sortKey.path(), assertionPath + " right bucket");
      int comparison = compareJsonValues(leftValue, rightValue);
      if (sortKey.direction() == SortDirection.DESC) {
        comparison = -comparison;
      }
      if (comparison != 0) {
        return comparison;
      }
    }
    return 0;
  }

  private static int compareJsonValues(JsonNode left, JsonNode right) {
    if (left.isNumber() && right.isNumber()) {
      return Double.compare(left.asDouble(), right.asDouble());
    }
    if (left.isTextual() && right.isTextual()) {
      return left.asText().compareTo(right.asText());
    }
    if (left.isBoolean() && right.isBoolean()) {
      return Boolean.compare(left.asBoolean(), right.asBoolean());
    }
    if (left.isArray() && right.isArray()) {
      int sizeComparison = Integer.compare(left.size(), right.size());
      for (int i = 0; i < Math.min(left.size(), right.size()); i++) {
        int comparison = compareJsonValues(left.get(i), right.get(i));
        if (comparison != 0) {
          return comparison;
        }
      }
      return sizeComparison;
    }
    return left.toString().compareTo(right.toString());
  }

  private static JsonNode requireJsonPath(JsonNode node, String path, String assertionPath) {
    JsonNode value = jsonPath(node, path);
    assertThat(value).as("%s.%s exists", assertionPath, path).isNotNull();
    return value;
  }

  private static JsonNode jsonPath(JsonNode node, String path) {
    JsonNode current = node;
    if (path.isEmpty()) {
      return current;
    }
    for (String part : path.split("\\.")) {
      if (current == null) {
        return null;
      }
      current = current.get(part);
    }
    return current;
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

  private static List<ClickBenchRow> searchPhraseSortRows() {
    return List.of(
        row(1).eventTime(START.plusSeconds(4)).searchPhrase("delta").build(),
        row(2).eventTime(START.plusSeconds(3)).searchPhrase("alpha").build(),
        row(3).eventTime(START.plusSeconds(2)).searchPhrase("zulu").build(),
        row(4).eventTime(START.plusSeconds(1)).searchPhrase("bravo").build());
  }

  private static List<ClickBenchRow> userPhraseRows() {
    return List.of(
        row(1).userId(101).searchPhrase("alpha").build(),
        row(2).userId(101).searchPhrase("alpha").build(),
        row(3).userId(101).searchPhrase("beta").build(),
        row(4).userId(102).searchPhrase("beta").build(),
        row(5).userId(103).build());
  }

  private static List<ClickBenchRow> userMinutePhraseRows() {
    return List.of(
        row(1)
            .userId(101)
            .eventTime(Instant.parse("2013-07-14T00:05:00Z"))
            .searchPhrase("alpha")
            .build(),
        row(2)
            .userId(101)
            .eventTime(Instant.parse("2013-07-14T00:05:12Z"))
            .searchPhrase("alpha")
            .build(),
        row(3)
            .userId(101)
            .eventTime(Instant.parse("2013-07-14T00:05:30Z"))
            .searchPhrase("beta")
            .build(),
        row(4)
            .userId(102)
            .eventTime(Instant.parse("2013-07-14T00:06:00Z"))
            .searchPhrase("beta")
            .build());
  }

  private static List<ClickBenchRow> searchEngineClientIpRows() {
    return List.of(
        row(1).searchEngineId(1).clientIp(10).searchPhrase("alpha").resolutionWidth(100).build(),
        row(2)
            .searchEngineId(1)
            .clientIp(10)
            .searchPhrase("alpha")
            .isRefresh(true)
            .resolutionWidth(200)
            .build(),
        row(3).searchEngineId(2).clientIp(20).searchPhrase("beta").resolutionWidth(300).build(),
        row(4).searchEngineId(2).clientIp(20).searchPhrase("beta").resolutionWidth(500).build(),
        row(5).searchEngineId(1).clientIp(30).searchPhrase("gamma").resolutionWidth(700).build(),
        row(6).searchEngineId(1).clientIp(40).resolutionWidth(900).build());
  }

  private static List<ClickBenchRow> watchClientIpRows() {
    return List.of(
        row(1).watchId(100).clientIp(10).searchPhrase("alpha").resolutionWidth(100).build(),
        row(2)
            .watchId(100)
            .clientIp(10)
            .searchPhrase("alpha")
            .isRefresh(true)
            .resolutionWidth(200)
            .build(),
        row(3).watchId(200).clientIp(20).searchPhrase("beta").resolutionWidth(300).build(),
        row(4).watchId(200).clientIp(20).searchPhrase("beta").resolutionWidth(500).build(),
        row(5).watchId(300).clientIp(30).searchPhrase("gamma").resolutionWidth(700).build(),
        row(6).watchId(400).clientIp(40).resolutionWidth(900).build());
  }

  private static List<ClickBenchRow> clientIpExpressionRows() {
    return List.of(
        row(1).clientIp(50).build(), row(2).clientIp(50).build(), row(3).clientIp(60).build());
  }

  private static List<ClickBenchRow> mobilePhoneModelRows() {
    return List.of(
        row(1).mobilePhone(2).mobilePhoneModel("Android").userId(102).build(),
        row(2).mobilePhone(2).mobilePhoneModel("Android").userId(103).build(),
        row(3).mobilePhone(2).mobilePhoneModel("Android").userId(104).build(),
        row(4).mobilePhone(1).mobilePhoneModel("iPhone").userId(100).build(),
        row(5).mobilePhone(1).mobilePhoneModel("iPhone").userId(101).build(),
        row(6).mobilePhone(3).mobilePhoneModel("Pixel").userId(105).build());
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

  private static List<ClickBenchRow> q38Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addUrlRows(rows, 1, "https://offset.example/a", 5);
    addUrlRows(rows, 6, "https://offset.example/b", 4);
    addUrlRows(rows, 10, "https://offset.example/c", 3);
    addUrlRows(rows, 13, "https://offset.example/d", 2);
    rows.add(
        row(15).counterId(62).url("https://excluded.example").isLink(1).isRefresh(true).build());
    rows.add(row(16).counterId(62).url("https://excluded.example").isLink(0).build());
    rows.add(row(17).counterId(62).url("https://excluded.example").isLink(1).isDownload(1).build());
    rows.add(
        row(18)
            .counterId(62)
            .url("https://excluded.example")
            .isLink(1)
            .eventDate("2013-08-01T00:00:00Z")
            .build());
    rows.add(row(19).counterId(61).url("https://excluded.example").isLink(1).build());
    return rows;
  }

  private static List<ClickBenchRow> q40Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addUrlHashDateRows(rows, 1, 1001L, "2013-07-14T00:00:00Z", 5);
    addUrlHashDateRows(rows, 6, 2002L, "2013-07-14T00:00:00Z", 4);
    addUrlHashDateRows(rows, 10, 3003L, "2013-07-14T00:00:00Z", 3);
    addUrlHashDateRows(rows, 13, 4004L, "2013-07-14T00:00:00Z", 2);
    rows.add(row(15).counterId(62).urlHash(9009L).isRefresh(true).build());
    rows.add(row(16).counterId(62).urlHash(9009L).traficSourceId(2).build());
    rows.add(row(17).counterId(62).urlHash(9009L).refererHash(1L).build());
    rows.add(row(18).counterId(62).urlHash(9009L).eventDate("2013-08-01T00:00:00Z").build());
    rows.add(row(19).counterId(61).urlHash(9009L).build());
    return rows;
  }

  private static List<ClickBenchRow> q41Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addWindowRows(rows, 1, 1024, 768, 5);
    addWindowRows(rows, 6, 1440, 900, 4);
    addWindowRows(rows, 10, 800, 600, 3);
    addWindowRows(rows, 13, 1920, 1080, 2);
    rows.add(row(15).counterId(62).windowSize(320, 200).isRefresh(true).build());
    rows.add(row(16).counterId(62).windowSize(320, 200).dontCountHits(true).build());
    rows.add(row(17).counterId(62).windowSize(320, 200).urlHash(1).build());
    rows.add(row(18).counterId(62).windowSize(320, 200).eventDate("2013-08-01T00:00:00Z").build());
    rows.add(row(19).counterId(61).windowSize(320, 200).build());
    return rows;
  }

  private static List<ClickBenchRow> q42Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addMinuteRows(rows, 1, "2013-07-14T00:00:00Z", 5);
    addMinuteRows(rows, 6, "2013-07-14T00:01:00Z", 4);
    addMinuteRows(rows, 10, "2013-07-14T00:02:00Z", 3);
    addMinuteRows(rows, 13, "2013-07-14T00:03:00Z", 2);
    rows.add(
        row(15)
            .counterId(62)
            .eventTime(Instant.parse("2013-07-14T00:04:00Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .isRefresh(true)
            .build());
    rows.add(
        row(16)
            .counterId(62)
            .eventTime(Instant.parse("2013-07-14T00:04:00Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .dontCountHits(true)
            .build());
    rows.add(
        row(17)
            .counterId(61)
            .eventTime(Instant.parse("2013-07-14T00:04:00Z"))
            .eventDate("2013-07-14T00:00:00Z")
            .build());
    rows.add(
        row(18)
            .counterId(62)
            .eventTime(Instant.parse("2013-07-14T00:04:00Z"))
            .eventDate("2013-08-01T00:00:00Z")
            .build());
    return rows;
  }

  private static void addUrlRows(List<ClickBenchRow> rows, int firstId, String url, int count) {
    for (int i = 0; i < count; i++) {
      rows.add(row(firstId + i).counterId(62).url(url).isLink(1).build());
    }
  }

  private static void addUrlHashDateRows(
      List<ClickBenchRow> rows, int firstId, long urlHash, String eventDate, int count) {
    for (int i = 0; i < count; i++) {
      rows.add(
          row(firstId + i)
              .counterId(62)
              .traficSourceId(6)
              .urlHash(urlHash)
              .eventDate(eventDate)
              .build());
    }
  }

  private static void addWindowRows(
      List<ClickBenchRow> rows,
      int firstId,
      int windowClientWidth,
      int windowClientHeight,
      int count) {
    for (int i = 0; i < count; i++) {
      rows.add(
          row(firstId + i).counterId(62).windowSize(windowClientWidth, windowClientHeight).build());
    }
  }

  private static void addMinuteRows(
      List<ClickBenchRow> rows, int firstId, String eventTime, int count) {
    Instant minute = Instant.parse(eventTime);
    for (int i = 0; i < count; i++) {
      rows.add(
          row(firstId + i)
              .counterId(62)
              .eventTime(minute.plusSeconds(i))
              .eventDate("2013-07-14T00:00:00Z")
              .build());
    }
  }

  private TestKafkaServer kafkaServer;
  private TestingServer zkServer;
  private S3AsyncClient s3Client;
  private AsyncCuratorFramework curatorFramework;
  private DatasetMetadataStore datasetMetadataStore;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;
  private PrometheusMeterRegistry queryMeterRegistry;
  private PrometheusMeterRegistry indexerMeterRegistry;
  private Astra queryService;
  private Astra indexer;
  private WebClient queryClient;
  private int receivedMessages;
  private int completedRollovers;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    zkServer = new TestingServer();
    kafkaServer = new TestKafkaServer();
    s3Client = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    queryMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    indexerMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    metadataStoreConfig = metadataStoreConfig();
    curatorFramework =
        CuratorBuilder.build(queryMeterRegistry, metadataStoreConfig.getZookeeperConfig());
    datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, metadataStoreConfig, queryMeterRegistry, true);
    queryClient = WebClient.of("http://127.0.0.1:" + QUERY_SERVICE_PORT);
    receivedMessages = 0;
    completedRollovers = 0;
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (indexer != null) {
      indexer.shutdown();
    }
    if (queryService != null) {
      queryService.shutdown();
    }
    if (datasetMetadataStore != null) {
      datasetMetadataStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (s3Client != null) {
      s3Client.close();
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
    if (queryMeterRegistry != null) {
      queryMeterRegistry.close();
    }
    if (indexerMeterRegistry != null) {
      indexerMeterRegistry.close();
    }
  }

  @Test
  public void q0CountAllRows() throws Exception {
    verify(
        clickBench("Q0")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "row_count": {
                      "value": 9
                    }
                  }
                }
                """));
  }

  @Test
  public void q1CountRowsWhereAdvEngineIdIsNotZero() throws Exception {
    verify(
        clickBench("Q1")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 8,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "row_count": {
                      "value": 8
                    }
                  }
                }
                """));
  }

  @Test
  public void q2SumCountAndAverage() throws Exception {
    verify(
        clickBench("Q2")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "adv_engine_sum": {
                      "value": 26
                    },
                    "row_count": {
                      "value": 9
                    },
                    "avg_resolution_width": {
                      "value": 500
                    }
                  }
                }
                """));
  }

  @Test
  public void q3AverageUserId() throws Exception {
    verify(
        clickBench("Q3")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 3,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "avg_user_id": {
                      "value": 20
                    }
                  }
                }
                """));
  }

  @Test
  public void q4DistinctUserIdCount() throws Exception {
    verify(
        clickBench("Q4")
            .expects("distinct UserID count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "users": {
                      "cardinality": {
                        "field": "UserID"
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "users": {
                      "value": 8
                    }
                  }
                }
                """));
  }

  @Test
  public void q5DistinctSearchPhraseCount() throws Exception {
    verify(
        clickBench("Q5")
            .expects("distinct SearchPhrase count")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "phrases": {
                      "cardinality": {
                        "field": "SearchPhrase"
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "phrases": {
                      "value": 4
                    }
                  }
                }
                """));
  }

  @Test
  public void q6MinAndMaxEventDate() throws Exception {
    verify(
        clickBench("Q6")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "min_event_date": {
                      "value": 1372636800000
                    },
                    "max_event_date": {
                      "value": 1373846400000
                    }
                  }
                }
                """));
  }

  @Test
  public void q7AdvEngineIdCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q7")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "adv_engines": {
                      "buckets": [
                        {
                          "key": 3,
                          "doc_count": 3
                        },
                        {
                          "key": 2,
                          "doc_count": 2
                        },
                        {
                          "key": 1,
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.adv_engines.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q8RegionIdOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q8")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "regions": {
                      "buckets": [
                        {
                          "key": 2,
                          "doc_count": 4,
                          "users": {
                            "value": 4
                          }
                        },
                        {
                          "key": 1,
                          "doc_count": 3,
                          "users": {
                            "value": 2
                          }
                        },
                        {
                          "key": 3,
                          "doc_count": 2,
                          "users": {
                            "value": 2
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.regions.buckets", descending("users.value"), ascending("key"))));
  }

  @Test
  public void q9RegionIdWithSiblingMetricsOrderedByCount() throws Exception {
    verify(
        clickBench("Q9")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "regions": {
                      "buckets": [
                        {
                          "key": 2,
                          "doc_count": 4,
                          "adv_engine_sum": {
                            "value": 14
                          },
                          "avg_resolution_width": {
                            "value": 600
                          },
                          "users": {
                            "value": 4
                          }
                        },
                        {
                          "key": 1,
                          "doc_count": 3,
                          "adv_engine_sum": {
                            "value": 4
                          },
                          "avg_resolution_width": {
                            "value": 200
                          },
                          "users": {
                            "value": 2
                          }
                        },
                        {
                          "key": 3,
                          "doc_count": 2,
                          "adv_engine_sum": {
                            "value": 8
                          },
                          "avg_resolution_width": {
                            "value": 750
                          },
                          "users": {
                            "value": 2
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.regions.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q10MobilePhoneModelOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q10")
            .expects("non-empty MobilePhoneModel buckets ordered by distinct UserID count")
            .givenRows(mobilePhoneModelRows())
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "models": {
                      "buckets": [
                        {
                          "key": "Android",
                          "doc_count": 3,
                          "users": {
                            "value": 3
                          }
                        },
                        {
                          "key": "iPhone",
                          "doc_count": 2,
                          "users": {
                            "value": 2
                          }
                        },
                        {
                          "key": "Pixel",
                          "doc_count": 1,
                          "users": {
                            "value": 1
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.models.buckets", descending("users.value"), ascending("key"))));
  }

  @Test
  public void q11MobilePhoneAndModelOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q11")
            .expects("MobilePhone plus MobilePhoneModel buckets ordered by distinct UserID count")
            .givenRows(mobilePhoneModelRows())
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "phones": {
                      "buckets": [
                        {
                          "key": [2, "Android"],
                          "doc_count": 3,
                          "users": {
                            "value": 3
                          }
                        },
                        {
                          "key": [1, "iPhone"],
                          "doc_count": 2,
                          "users": {
                            "value": 2
                          }
                        },
                        {
                          "key": [3, "Pixel"],
                          "doc_count": 1,
                          "users": {
                            "value": 1
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.phones.buckets", descending("users.value"), ascending("key"))));
  }

  @Test
  public void q12SearchPhraseCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q12")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 7,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "phrases": {
                      "buckets": [
                        {
                          "key": "gamma",
                          "doc_count": 3
                        },
                        {
                          "key": "alpha",
                          "doc_count": 2
                        },
                        {
                          "key": "beta",
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.phrases.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q13SearchPhraseOrderedByDistinctUsers() throws Exception {
    verify(
        clickBench("Q13")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 7,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "phrases": {
                      "buckets": [
                        {
                          "key": "gamma",
                          "doc_count": 3,
                          "users": {
                            "value": 3
                          }
                        },
                        {
                          "key": "alpha",
                          "doc_count": 2,
                          "users": {
                            "value": 2
                          }
                        },
                        {
                          "key": "beta",
                          "doc_count": 2,
                          "users": {
                            "value": 2
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.phrases.buckets", descending("users.value"), ascending("key"))));
  }

  @Test
  public void q14SearchEngineAndPhraseCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q14")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "engine_phrases": {
                      "buckets": [
                        {
                          "key": [1, "alpha"],
                          "doc_count": 3
                        },
                        {
                          "key": [2, "beta"],
                          "doc_count": 2
                        },
                        {
                          "key": [1, "gamma"],
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.engine_phrases.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q15UserIdCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q15")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 9,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "users": {
                      "buckets": [
                        {
                          "key": 101,
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """));
  }

  @Test
  public void q16UserIdAndSearchPhraseCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q16")
            .expects("UserID plus SearchPhrase buckets ordered by count")
            .givenRows(userPhraseRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "user_phrases": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "UserID"
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "user_phrases": {
                      "buckets": [
                        {
                          "key": [101, "alpha"],
                          "doc_count": 2
                        },
                        {
                          "key": [101, "beta"],
                          "doc_count": 1
                        },
                        {
                          "key": [102, "beta"],
                          "doc_count": 1
                        },
                        {
                          "key": [103, ""],
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.user_phrases.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q17UserIdAndSearchPhraseCountsLimit10() throws Exception {
    verify(
        clickBench("Q17")
            .expects("UserID plus SearchPhrase buckets limited to 10")
            .givenRows(userPhraseRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "user_phrases": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "UserID"
                          },
                          {
                            "field": "SearchPhrase"
                          }
                        ],
                        "size": 10
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "user_phrases": {
                      "buckets": [
                        {
                          "key": [101, "alpha"],
                          "doc_count": 2
                        },
                        {
                          "key": [101, "beta"],
                          "doc_count": 1
                        },
                        {
                          "key": [102, "beta"],
                          "doc_count": 1
                        },
                        {
                          "key": [103, ""],
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """));
  }

  @Test
  public void q18UserIdMinuteAndSearchPhraseCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q18")
            .expects("UserID plus EventTime minute plus SearchPhrase buckets ordered by count")
            .givenRows(userMinutePhraseRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "user_minute_phrases": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "UserID"
                          },
                          {
                            "script": {
                              "source": "doc['@timestamp'].value.getMinute()"
                            }
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 4,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "user_minute_phrases": {
                      "buckets": [
                        {
                          "key": [101, "5", "alpha"],
                          "doc_count": 2
                        },
                        {
                          "key": [101, "5", "beta"],
                          "doc_count": 1
                        },
                        {
                          "key": [102, "6", "beta"],
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.user_minute_phrases.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q19FindSpecificUserId() throws Exception {
    verify(
        clickBench("Q19")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 1,
                      "relation": "eq"
                    },
                    "hits": [
                      {
                        "_source": {
                          "SearchPhrase": "gamma"
                        }
                      }
                    ]
                  }
                }
                """));
  }

  @Test
  public void q20CountRowsWhereUrlContainsGoogle() throws Exception {
    verify(
        clickBench("Q20")
            .expects("count rows where URL contains google")
            .givenRows(benchmarkRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "query": {
                    "wildcard": {
                      "URL": {
                        "value": "*google*"
                      }
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "row_count": {
                      "value": 5
                    }
                  }
                }
                """));
  }

  @Test
  public void q21GoogleUrlSearchPhrasesOrderedByCount() throws Exception {
    verify(
        clickBench("Q21")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 4,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "phrases": {
                      "buckets": [
                        {
                          "key": "alpha",
                          "doc_count": 2
                        },
                        {
                          "key": "beta",
                          "doc_count": 1
                        },
                        {
                          "key": "gamma",
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.phrases.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q22GoogleTitleWithoutGoogleSubdomainSearchPhrasesOrderedByCount() throws Exception {
    verify(
        clickBench("Q22")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 3,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "phrases": {
                      "buckets": [
                        {
                          "key": "beta",
                          "doc_count": 2
                        },
                        {
                          "key": "alpha",
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.phrases.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q23UrlContainsGoogleOrderByEventTimeLimit10() throws Exception {
    verify(
        clickBench("Q23")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 3,
                      "relation": "eq"
                    },
                    "hits": [
                      {
                        "_source": {
                          "URL": "https://google.example/search"
                        }
                      },
                      {
                        "_source": {
                          "URL": "https://mail.google.example"
                        }
                      },
                      {
                        "_source": {
                          "URL": "https://google.example/maps"
                        }
                      }
                    ]
                  }
                }
                """));
  }

  @Test
  public void q24SearchPhraseNotEmptyOrderByEventTimeLimit10() throws Exception {
    verify(
        clickBench("Q24")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 4,
                      "relation": "eq"
                    },
                    "hits": [
                      {
                        "_source": {
                          "SearchPhrase": "delta"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "zulu"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "bravo"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "alpha"
                        }
                      }
                    ]
                  }
                }
                """));
  }

  @Test
  public void q25SearchPhraseNotEmptyOrderBySearchPhraseLimit10() throws Exception {
    verify(
        clickBench("Q25")
            .expects(
                "non-empty SearchPhrase values, ordered by SearchPhrase ascending, limited to 10"
                    + " hits")
            .givenRows(searchPhraseSortRows())
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 4,
                      "relation": "eq"
                    },
                    "hits": [
                      {
                        "_source": {
                          "SearchPhrase": "alpha"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "bravo"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "delta"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "zulu"
                        }
                      }
                    ]
                  }
                }
                """,
                orderedArrayBy("hits.hits", ascending("_source.SearchPhrase"))));
  }

  @Test
  public void q26SearchPhraseNotEmptyOrderByEventTimeThenSearchPhraseLimit10() throws Exception {
    verify(
        clickBench("Q26")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 4,
                      "relation": "eq"
                    },
                    "hits": [
                      {
                        "_source": {
                          "SearchPhrase": "alpha"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "delta"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "zulu"
                        }
                      },
                      {
                        "_source": {
                          "SearchPhrase": "bravo"
                        }
                      }
                    ]
                  }
                }
                """));
  }

  @Test
  public void q29ManySiblingResolutionWidthSums() throws Exception {
    verify(
        clickBench("Q29")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 2,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "sum_width_plus_0": {
                      "value": 30
                    },
                    "sum_width_plus_1": {
                      "value": 32
                    },
                    "sum_width_plus_2": {
                      "value": 34
                    }
                  }
                }
                """));
  }

  @Test
  public void q30SearchEngineClientIpMetricsOrderedByCount() throws Exception {
    verify(
        clickBench("Q30")
            .expects("SearchEngineID plus ClientIP buckets with count, refresh sum, and width avg")
            .givenRows(searchEngineClientIpRows())
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
                    "engine_clients": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "SearchEngineID"
                          },
                          {
                            "field": "ClientIP"
                          }
                        ],
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "refreshes": {
                          "sum": {
                            "script": {
                              "source": "doc['IsRefresh'].value ? 1 : 0"
                            }
                          }
                        },
                        "avg_resolution_width": {
                          "avg": {
                            "field": "ResolutionWidth"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "engine_clients": {
                      "buckets": [
                        {
                          "key": [1, 10],
                          "doc_count": 2,
                          "refreshes": {
                            "value": 1
                          },
                          "avg_resolution_width": {
                            "value": 150
                          }
                        },
                        {
                          "key": [2, 20],
                          "doc_count": 2,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 400
                          }
                        },
                        {
                          "key": [1, 30],
                          "doc_count": 1,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 700
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.engine_clients.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q31WatchIdClientIpMetricsForNonEmptySearchPhrase() throws Exception {
    verify(
        clickBench("Q31")
            .expects("WatchID plus ClientIP buckets with metrics for non-empty SearchPhrase")
            .givenRows(watchClientIpRows())
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
                    "watch_clients": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "WatchID"
                          },
                          {
                            "field": "ClientIP"
                          }
                        ],
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "refreshes": {
                          "sum": {
                            "script": {
                              "source": "doc['IsRefresh'].value ? 1 : 0"
                            }
                          }
                        },
                        "avg_resolution_width": {
                          "avg": {
                            "field": "ResolutionWidth"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "watch_clients": {
                      "buckets": [
                        {
                          "key": [100, 10],
                          "doc_count": 2,
                          "refreshes": {
                            "value": 1
                          },
                          "avg_resolution_width": {
                            "value": 150
                          }
                        },
                        {
                          "key": [200, 20],
                          "doc_count": 2,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 400
                          }
                        },
                        {
                          "key": [300, 30],
                          "doc_count": 1,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 700
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.watch_clients.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q32WatchIdClientIpMetricsOrderedByCount() throws Exception {
    verify(
        clickBench("Q32")
            .expects("WatchID plus ClientIP buckets with metrics")
            .givenRows(watchClientIpRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "watch_clients": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "WatchID"
                          },
                          {
                            "field": "ClientIP"
                          }
                        ],
                        "size": 10,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "refreshes": {
                          "sum": {
                            "script": {
                              "source": "doc['IsRefresh'].value ? 1 : 0"
                            }
                          }
                        },
                        "avg_resolution_width": {
                          "avg": {
                            "field": "ResolutionWidth"
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "watch_clients": {
                      "buckets": [
                        {
                          "key": [100, 10],
                          "doc_count": 2,
                          "refreshes": {
                            "value": 1
                          },
                          "avg_resolution_width": {
                            "value": 150
                          }
                        },
                        {
                          "key": [200, 20],
                          "doc_count": 2,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 400
                          }
                        },
                        {
                          "key": [300, 30],
                          "doc_count": 1,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 700
                          }
                        },
                        {
                          "key": [400, 40],
                          "doc_count": 1,
                          "refreshes": {
                            "value": 0
                          },
                          "avg_resolution_width": {
                            "value": 900
                          }
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.watch_clients.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q33UrlCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q33")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "urls": {
                      "buckets": [
                        {
                          "key": "https://popular.example",
                          "doc_count": 3
                        },
                        {
                          "key": "https://middle.example",
                          "doc_count": 2
                        },
                        {
                          "key": "https://rare.example",
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.urls.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q34ConstantAndUrlCountsOrderedByCount() throws Exception {
    verify(
        clickBench("Q34")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 6,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "constant_urls": {
                      "buckets": [
                        {
                          "key": [1, "https://popular.example"],
                          "doc_count": 3
                        },
                        {
                          "key": [1, "https://middle.example"],
                          "doc_count": 2
                        },
                        {
                          "key": [1, "https://rare.example"],
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.constant_urls.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q35ClientIpExpressionBucketsOrderedByCount() throws Exception {
    verify(
        clickBench("Q35")
            .expects("ClientIP plus arithmetic expression buckets ordered by count")
            .givenRows(clientIpExpressionRows())
            .whenAstraReceivesEquivalentOpenSearch(
                """
                {
                  "size": 0,
                  "aggs": {
                    "client_ip_expressions": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "ClientIP"
                          },
                          {
                            "script": {
                              "source": "doc['ClientIP'].value - 1"
                            }
                          },
                          {
                            "script": {
                              "source": "doc['ClientIP'].value - 2"
                            }
                          },
                          {
                            "script": {
                              "source": "doc['ClientIP'].value - 3"
                            }
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 3,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "client_ip_expressions": {
                      "buckets": [
                        {
                          "key": [50, "49", "48", "47"],
                          "doc_count": 2
                        },
                        {
                          "key": [60, "59", "58", "57"],
                          "doc_count": 1
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.client_ip_expressions.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q36PageViewUrlsForCounterAndDateRange() throws Exception {
    verify(
        clickBench("Q36")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "urls": {
                      "buckets": [
                        {
                          "key": "https://page.example/a",
                          "doc_count": 3
                        },
                        {
                          "key": "https://page.example/b",
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.urls.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q37PageViewTitlesForCounterAndDateRange() throws Exception {
    verify(
        clickBench("Q37")
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
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 5,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "titles": {
                      "buckets": [
                        {
                          "key": "Title A",
                          "doc_count": 3
                        },
                        {
                          "key": "Title B",
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.titles.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q38PageViewUrlsWithBucketOffset() throws Exception {
    verify(
        clickBench("Q38")
            .expects("URL page-view buckets after filters, count ordering, and bucket offset")
            .givenRows(q38Rows())
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
                        },
                        {
                          "term": {
                            "IsDownload": 0
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "IsRefresh": true
                          }
                        },
                        {
                          "term": {
                            "IsLink": 0
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "urls": {
                      "terms": {
                        "field": "URL",
                        "size": 4,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "page": {
                          "bucket_sort": {
                            "sort": [
                              {
                                "_count": {
                                  "order": "desc"
                                }
                              }
                            ],
                            "from": 2,
                            "size": 2
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 14,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "urls": {
                      "buckets": [
                        {
                          "key": "https://offset.example/c",
                          "doc_count": 3
                        },
                        {
                          "key": "https://offset.example/d",
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.urls.buckets", descending("doc_count"), ascending("key"))));
  }

  @Test
  public void q40UrlHashDatesWithBucketOffset() throws Exception {
    verify(
        clickBench("Q40")
            .expects("URLHash plus EventDate page-view buckets after filters and bucket offset")
            .givenRows(q40Rows())
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
                        },
                        {
                          "terms": {
                            "TraficSourceID": [-1, 6]
                          }
                        },
                        {
                          "term": {
                            "RefererHash": 3594120000172545465
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "IsRefresh": true
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "url_hash_dates": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "URLHash"
                          },
                          {
                            "field": "EventDate"
                          }
                        ],
                        "size": 4,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "page": {
                          "bucket_sort": {
                            "sort": [
                              {
                                "_count": {
                                  "order": "desc"
                                }
                              }
                            ],
                            "from": 2,
                            "size": 2
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 14,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "url_hash_dates": {
                      "buckets": [
                        {
                          "key": [3003, "2013-07-14T00:00:00.000Z"],
                          "doc_count": 3
                        },
                        {
                          "key": [4004, "2013-07-14T00:00:00.000Z"],
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.url_hash_dates.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q41WindowSizesWithBucketOffset() throws Exception {
    verify(
        clickBench("Q41")
            .expects("window-size page-view buckets after filters and bucket offset")
            .givenRows(q41Rows())
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
                        },
                        {
                          "term": {
                            "URLHash": 2868770270353813622
                          }
                        }
                      ],
                      "must_not": [
                        {
                          "term": {
                            "IsRefresh": true
                          }
                        },
                        {
                          "term": {
                            "DontCountHits": true
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "window_sizes": {
                      "multi_terms": {
                        "terms": [
                          {
                            "field": "WindowClientWidth"
                          },
                          {
                            "field": "WindowClientHeight"
                          }
                        ],
                        "size": 4,
                        "order": {
                          "_count": "desc"
                        }
                      },
                      "aggs": {
                        "page": {
                          "bucket_sort": {
                            "sort": [
                              {
                                "_count": {
                                  "order": "desc"
                                }
                              }
                            ],
                            "from": 2,
                            "size": 2
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 14,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "window_sizes": {
                      "buckets": [
                        {
                          "key": [800, 600],
                          "doc_count": 3
                        },
                        {
                          "key": [1920, 1080],
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy(
                    "aggregations.window_sizes.buckets",
                    descending("doc_count"),
                    ascending("key"))));
  }

  @Test
  public void q42MinuteBucketsForCounterAndDateRange() throws Exception {
    verify(
        clickBench("Q42")
            .expects("minute buckets after filters, key ordering, and bucket offset")
            .givenRows(q42Rows())
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
                      },
                      "aggs": {
                        "page": {
                          "bucket_sort": {
                            "sort": [
                              {
                                "_key": {
                                  "order": "asc"
                                }
                              }
                            ],
                            "from": 2,
                            "size": 2
                          }
                        }
                      }
                    }
                  }
                }
                """)
            .thenAstraRespondsWith(
                """
                {
                  "hits": {
                    "total": {
                      "value": 14,
                      "relation": "eq"
                    }
                  },
                  "aggregations": {
                    "minutes": {
                      "buckets": [
                        {
                          "key": 1373760120000,
                          "doc_count": 3
                        },
                        {
                          "key": 1373760180000,
                          "doc_count": 2
                        }
                      ]
                    }
                  }
                }
                """,
                orderedBucketsBy("aggregations.minutes.buckets", ascending("key"))));
  }

  private void verify(ClickBenchSpec spec) throws Exception {
    JsonNode expectedResponseJson = OBJECT_MAPPER.readTree(spec.expectedResponse().json());
    String assertionPath = spec.queryId() + ": " + spec.expects();
    assertExpectedResponseRules(spec.expectedResponse(), expectedResponseJson, assertionPath);

    String dataset = datasetName(spec);
    registerDataset(dataset);
    startQueryService();
    startIndexer(spec.rows().size());
    addRows(dataset, spec.rows());
    JsonNode response = searchJson(dataset, spec.requestJson());
    assertJsonContains(response, expectedResponseJson, assertionPath);
  }

  private static String datasetName(ClickBenchSpec spec) {
    return TEST_DATASET_NAME + "_" + spec.queryId().toLowerCase(Locale.ROOT);
  }

  private void registerDataset(String dataset) {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            dataset,
            "clickbench",
            1_000_000,
            List.of(DatasetPartitionMetadata.createActive(1, List.of("0"))),
            dataset);
    datasetMetadataStore.createSync(datasetMetadata);
    await()
        .until(
            () ->
                AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).stream()
                    .anyMatch(metadata -> metadata.name.equals(dataset)));
  }

  private void startQueryService() throws Exception {
    AstraConfigs.AstraConfig queryConfig =
        makeAstraConfig(-1, QUERY_SERVICE_PORT, AstraConfigs.NodeRole.QUERY);
    queryService = new Astra(queryConfig, queryMeterRegistry);
    queryService.start();
    queryService.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);
  }

  private void startIndexer(int maxMessagesPerChunk) throws Exception {
    AstraConfigs.AstraConfig indexerConfig =
        makeAstraConfig(INDEXER_SERVICE_PORT, -1, AstraConfigs.NodeRole.INDEX, maxMessagesPerChunk);
    indexer = new Astra(indexerConfig, s3Client, indexerMeterRegistry);
    indexer.start();
    indexer.serviceManager.awaitHealthy(DEFAULT_START_STOP_DURATION);
    await().until(() -> kafkaServer.getConnectedConsumerGroups() == 1);
  }

  private AstraConfigs.AstraConfig makeAstraConfig(
      int indexPort, int queryPort, AstraConfigs.NodeRole nodeRole) {
    return makeAstraConfig(indexPort, queryPort, nodeRole, 100);
  }

  private AstraConfigs.AstraConfig makeAstraConfig(
      int indexPort, int queryPort, AstraConfigs.NodeRole nodeRole, int maxMessagesPerChunk) {
    return AstraConfigUtil.makeAstraConfig(
            "localhost:" + kafkaServer.getBroker().getKafkaPort().get(),
            indexPort,
            TEST_KAFKA_TOPIC,
            0,
            TEST_KAFKA_CLIENT,
            S3_TEST_BUCKET,
            queryPort,
            zkServer.getConnectString(),
            ZK_PATH_PREFIX,
            nodeRole,
            1000,
            RECOVERY_SERVICE_PORT,
            maxMessagesPerChunk)
        .toBuilder()
        .setMetadataStoreConfig(metadataStoreConfig)
        .setClusterConfig(
            AstraConfigs.ClusterConfig.newBuilder().setClusterName("clickbench-compat").build())
        .build();
  }

  private AstraConfigs.MetadataStoreConfig metadataStoreConfig() {
    return AstraConfigs.MetadataStoreConfig.newBuilder()
        .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
        .setZookeeperConfig(
            AstraConfigs.ZookeeperConfig.newBuilder()
                .setZkConnectString(zkServer.getConnectString())
                .setZkPathPrefix(ZK_PATH_PREFIX)
                .setZkSessionTimeoutMs(1000)
                .setZkConnectionTimeoutMs(1000)
                .setSleepBetweenRetriesMs(1000)
                .setZkCacheInitTimeoutMs(1000)
                .build())
        .build();
  }

  private void addRows(String dataset, List<ClickBenchRow> rows) throws Exception {
    List<Trace.Span> spans = rows.stream().map(row -> span(dataset, row)).toList();
    int produced = produceMessagesToKafka(kafkaServer.getBroker(), TEST_KAFKA_TOPIC, 0, spans);
    receivedMessages += produced;
    completedRollovers++;
    await()
        .until(() -> getCount(MESSAGES_RECEIVED_COUNTER, indexerMeterRegistry) == receivedMessages);
    await()
        .until(
            () ->
                getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, indexerMeterRegistry)
                    == completedRollovers);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, indexerMeterRegistry)).isZero();
  }

  private Trace.Span span(String dataset, ClickBenchRow row) {
    return Trace.Span.newBuilder()
        .setTimestamp(
            TimeUnit.MICROSECONDS.convert(Instant.now().toEpochMilli(), TimeUnit.MILLISECONDS))
        .setId(ByteString.copyFromUtf8(dataset + "-" + row.id()))
        .addTags(keywordField(LogMessage.ReservedField.SERVICE_NAME.fieldName, dataset))
        .addAllTags(
            List.of(
                dateField("@timestamp", row.eventTime()),
                integerField("AdvEngineID", row.advEngineId()),
                integerField("ResolutionWidth", row.resolutionWidth()),
                longField("UserID", row.userId()),
                longField("ClientIP", row.clientIp()),
                longField("WatchID", row.watchId()),
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
                integerField("ConstantOne", 1)))
        .build();
  }

  private JsonNode searchJson(String dataset, String postBody) throws Exception {
    AggregatedHttpResponse aggregatedRes =
        queryClient.post("/" + dataset + "/_search", postBody).aggregate().join();

    assertThat(aggregatedRes.status().code())
        .as("%s search response: %s", dataset, aggregatedRes.content(StandardCharsets.UTF_8))
        .isEqualTo(200);
    return OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));
  }

  private record ClickBenchRow(
      int id,
      Instant eventTime,
      int advEngineId,
      int resolutionWidth,
      long userId,
      long clientIp,
      long watchId,
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
      private long clientIp = 1L;
      private long watchId = 1L;
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

      private Builder clientIp(long clientIp) {
        this.clientIp = clientIp;
        return this;
      }

      private Builder watchId(long watchId) {
        this.watchId = watchId;
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

      private Builder isDownload(int isDownload) {
        this.isDownload = isDownload;
        return this;
      }

      private Builder traficSourceId(int traficSourceId) {
        this.traficSourceId = traficSourceId;
        return this;
      }

      private Builder refererHash(long refererHash) {
        this.refererHash = refererHash;
        return this;
      }

      private Builder urlHash(long urlHash) {
        this.urlHash = urlHash;
        return this;
      }

      private Builder windowSize(int windowClientWidth, int windowClientHeight) {
        this.windowClientWidth = windowClientWidth;
        this.windowClientHeight = windowClientHeight;
        return this;
      }

      private ClickBenchRow build() {
        return new ClickBenchRow(
            id,
            eventTime,
            advEngineId,
            resolutionWidth,
            userId,
            clientIp,
            watchId,
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
      ExpectedResponse expectedResponse) {
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

      private ClickBenchSpec thenAstraRespondsWith(String expectedResponseJson) {
        return thenAstraRespondsWith(expectedResponseJson, new ArrayRule[0]);
      }

      private ClickBenchSpec thenAstraRespondsWith(
          String expectedResponseJson, ArrayRule... arrayRules) {
        return new ClickBenchSpec(
            queryId,
            expects,
            rows,
            requestJson,
            new ExpectedResponse(expectedResponseJson, List.of(arrayRules)));
      }
    }
  }

  private enum SortDirection {
    ASC,
    DESC
  }

  private record ExpectedResponse(String json, List<ArrayRule> arrayRules) {
    private ExpectedResponse {
      for (int i = 0; i < arrayRules.size(); i++) {
        for (int j = i + 1; j < arrayRules.size(); j++) {
          assertThat(arrayRules.get(i).path())
              .as("%s has one array assertion rule", arrayRules.get(i).path())
              .isNotEqualTo(arrayRules.get(j).path());
        }
      }
      arrayRules = List.copyOf(arrayRules);
    }
  }

  private record ArrayRule(String path, List<SortKey> sortKeys) {
    private ArrayRule {
      sortKeys = List.copyOf(sortKeys);
    }

    private static ArrayRule ordered(String path, List<SortKey> sortKeys) {
      assertThat(sortKeys).as("%s sort keys", path).isNotEmpty();
      return new ArrayRule(path, sortKeys);
    }

    private String describeSort() {
      List<String> parts = new ArrayList<>();
      for (SortKey sortKey : sortKeys) {
        parts.add(sortKey.describe());
      }
      return String.join(", ", parts);
    }
  }

  private record SortKey(String path, SortDirection direction) {
    private String describe() {
      return path + " " + direction.name().toLowerCase(Locale.ROOT);
    }
  }
}
