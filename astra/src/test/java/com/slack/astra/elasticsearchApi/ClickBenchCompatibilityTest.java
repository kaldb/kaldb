package com.slack.astra.elasticsearchApi;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  private static List<ClickBenchRow> hitRows() {
    return List.of(
        new ClickBenchRow(1, START.plusSeconds(3), "https://mail.google.example", "zulu"),
        new ClickBenchRow(2, START.plusSeconds(1), "https://slack.example", ""),
        new ClickBenchRow(3, START.plusSeconds(2), "https://google.example/search", "delta"),
        new ClickBenchRow(4, START.plusSeconds(4), "https://astra.example", "bravo"),
        new ClickBenchRow(5, START.plusSeconds(5), "https://google.example/maps", ""),
        new ClickBenchRow(6, START.plusSeconds(6), "https://astra.example/docs", "alpha"));
  }

  private static List<ClickBenchRow> hitRowsWithEventTimeTie() {
    return List.of(
        new ClickBenchRow(1, START.plusSeconds(3), "https://mail.google.example", "zulu"),
        new ClickBenchRow(2, START.plusSeconds(1), "https://slack.example", ""),
        new ClickBenchRow(3, START.plusSeconds(2), "https://google.example/search", "delta"),
        new ClickBenchRow(4, START.plusSeconds(4), "https://astra.example", "bravo"),
        new ClickBenchRow(5, START.plusSeconds(5), "https://google.example/maps", ""),
        new ClickBenchRow(6, START.plusSeconds(2), "https://astra.example/docs", "alpha"));
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
            .thenReturnsSourceFieldValues(
                "URL",
                "https://google.example/search",
                "https://mail.google.example",
                "https://google.example/maps"));
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
            .thenReturnsSourceFieldValues("SearchPhrase", "delta", "zulu", "bravo", "alpha"));
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
            .thenReturnsSourceFieldValues("SearchPhrase", "alpha", "bravo", "delta", "zulu"));
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
            .thenReturnsSourceFieldValues("SearchPhrase", "alpha", "delta", "zulu", "bravo"));
  }

  private void verify(ClickBenchSpec spec) throws Exception {
    addRows(spec.rows());
    JsonNode response = searchJson(spec.requestJson());

    assertThat(sourceValues(response, spec.hitExpectation().sourceField()))
        .as("%s: %s", spec.queryId(), spec.expects())
        .containsExactlyElementsOf(spec.hitExpectation().values());
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
            Trace.KeyValue.newBuilder()
                .setKey("URL")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(row.url())
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("SearchPhrase")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(row.searchPhrase())
                .build()));
  }

  private JsonNode searchJson(String postBody) throws Exception {
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    return OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));
  }

  private List<String> sourceValues(JsonNode searchResponse, String field) {
    List<String> values = new ArrayList<>();
    for (JsonNode hit : searchResponse.path("hits").path("hits")) {
      values.add(hit.path("_source").path(field).asText());
    }
    return values;
  }

  private record ClickBenchRow(int id, Instant eventTime, String url, String searchPhrase) {}

  private record HitExpectation(String sourceField, List<String> values) {}

  private record ClickBenchSpec(
      String queryId,
      String expects,
      List<ClickBenchRow> rows,
      String requestJson,
      HitExpectation hitExpectation) {
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

      private ClickBenchSpec thenReturnsSourceFieldValues(String sourceField, String... values) {
        return new ClickBenchSpec(
            queryId, expects, rows, requestJson, new HitExpectation(sourceField, List.of(values)));
      }
    }
  }
}
