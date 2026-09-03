package com.slack.astra.logstore.search;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.ChunkManagerUtil.makeChunkManagerUtil;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.chunkManager.ChunkManager;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.chunkManager.RollOverChunkTask;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.util.GrpcCleanupExtension;
import com.slack.astra.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.metrics.InternalMax;

public class AstraLocalQueryServiceTest {
  private static final String TEST_KAFKA_PARITION_ID = "10";
  private static final String S3_TEST_BUCKET = "test-astra-logs";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  @RegisterExtension public final GrpcCleanupExtension grpcCleanup = new GrpcCleanupExtension();

  private ChunkManagerUtil<LogMessage> chunkManagerUtil;
  private AstraLocalQueryService<LogMessage> astraLocalQueryService;
  private SimpleMeterRegistry metricsRegistry;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    chunkManagerUtil =
        makeChunkManagerUtil(
            S3_MOCK_EXTENSION,
            S3_TEST_BUCKET,
            metricsRegistry,
            10 * 1024 * 1024 * 1024L,
            100,
            AstraConfigUtil.makeIndexerConfig(1000, 1000, 100));
    chunkManagerUtil.chunkManager.startAsync();
    chunkManagerUtil.chunkManager.awaitRunning(DEFAULT_START_STOP_DURATION);
    astraLocalQueryService =
        new AstraLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(3), false);
  }

  @AfterEach
  public void tearDown() throws IOException, TimeoutException {
    if (chunkManagerUtil != null) {
      chunkManagerUtil.close();
    }
  }

  private static String buildHistogramRequestJSON(long startMs, long endMs, int numBuckets) {
    String histogramRequest =
        """
        {"%s":{"date_histogram":{"interval":"%ds","field":"%s","min_doc_count":"%d","extended_bounds":{"min":1676498801027,"max":1676500240688},"format":"epoch_millis","offset":"5s"},"aggs":{}}}
    """
            .formatted(
                "1",
                (endMs - startMs) / numBuckets,
                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                1);
    return histogramRequest;
  }

  private static String buildQueryFromQueryString(
      String queryString, Long startTime, Long endTime) {
    return "{\"bool\":{\"filter\":[{\"range\":{\"_timesinceepoch\":{\"gte\":%d,\"lte\":%d,\"format\":\"epoch_millis\"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"%s\"}}]}}"
        .formatted(startTime, endTime, queryString);
  }

  private static Trace.Span makeDimensionSpan(
      int id, Instant timestamp, String country, String browser, long latency) {
    return SpanUtil.makeSpan(
        id,
        "dimension-message-" + id,
        timestamp,
        List.of(
            Trace.KeyValue.newBuilder()
                .setKey("country")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(country)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("browser")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(browser)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("latency")
                .setFieldType(Schema.SchemaFieldType.LONG)
                .setVInt64(latency)
                .build()));
  }

  @Test
  public void testAstraSearch() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.
    assertThat(chunkManager.getChunkList().size()).isEqualTo(1);
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(100);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_INITIATED, metricsRegistry)).isEqualTo(1);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_FAILED, metricsRegistry)).isEqualTo(0);
    assertThat(getCount(RollOverChunkTask.ROLLOVERS_COMPLETED, metricsRegistry)).isEqualTo(1);

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (100 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(
                    buildQueryFromQueryString("Message100", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getRequestedSnapshots()).isEqualTo(1);
    assertThat(response.getFulfilledSnapshots()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHits(0).getMessage()).contains("Message100");
    List<AstraSearch.SearchResult.Hit> hits = response.getHitsList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).getMessage(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_DATASET_NAME);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(100);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(100);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(100.0);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(100.0);
    assertThat((String) m.getSource().get("message")).contains("Message100");

    // Test histogram buckets
    InternalAggregations internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(
            response.getInternalAggregations().toByteArray());
    InternalDateHistogram dateHistogram = internalAggregations.get("1");
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(1);
    assertThat(dateHistogram.getBuckets().get(0).getDocCount()).isEqualTo(1);

    // TODO: Query multiple chunks.
  }

  @Test
  public void testAstraSearchNoData() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("blah", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    assertThat(response.getHitsCount()).isZero();
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getHitsList().size()).isZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getRequestedSnapshots()).isEqualTo(1);
    assertThat(response.getFulfilledSnapshots()).isEqualTo(1);

    // Test histogram buckets
    InternalAggregations internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(
            response.getInternalAggregations().toByteArray());
    InternalDateHistogram dateHistogram = internalAggregations.get("1");
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(0);
  }

  @Test
  public void testAstraSearchNoHits() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    // TODO: Query multiple chunks.
    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(0)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    assertThat(response.getHitsCount()).isEqualTo(0);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getRequestedSnapshots()).isEqualTo(1);
    assertThat(response.getFulfilledSnapshots()).isEqualTo(1);
    assertThat(response.getHitsList().size()).isZero();

    // Test histogram buckets
    InternalAggregations internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(
            response.getInternalAggregations().toByteArray());
    InternalDateHistogram dateHistogram = internalAggregations.get("1");
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(1);
    assertThat(dateHistogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAstraSearchNoHistogram() throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            searchRequestBuilder
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson("")
                .build());

    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getRequestedSnapshots()).isEqualTo(1);
    assertThat(response.getFulfilledSnapshots()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHitsList().size()).isEqualTo(1);
    assertThat(response.getHits(0).getMessage()).contains("Message1");
    List<AstraSearch.SearchResult.Hit> hits = response.getHitsList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).getMessage(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_DATASET_NAME);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.getSource().get("message")).contains("Message1");

    // Test histogram buckets
    assertThat(response.getInternalAggregations().size()).isEqualTo(0);
  }

  /** Verifies local multi_terms produces compound buckets and nested metric aggregations. */
  @Test
  public void testAstraSearchWithMultiTermsAndSubAggregation() throws IOException {
    Instant startTime = Instant.now().minusSeconds(10);
    List<Trace.Span> rows =
        List.of(
            makeDimensionSpan(1, startTime.plusMillis(1), "US", "Chrome", 12),
            makeDimensionSpan(2, startTime.plusMillis(2), "US", "Chrome", 30),
            makeDimensionSpan(3, startTime.plusMillis(3), "US", "Safari", 20),
            makeDimensionSpan(4, startTime.plusMillis(4), "CA", "Chrome", 40));
    Map<List<Object>, Long> expectedCounts =
        Map.of(
            List.of("US", "Chrome"), 2L,
            List.of("US", "Safari"), 1L,
            List.of("CA", "Chrome"), 1L);
    Map<List<Object>, Double> expectedMaximumLatency =
        Map.of(
            List.of("US", "Chrome"), 30.0,
            List.of("US", "Safari"), 20.0,
            List.of("CA", "Chrome"), 40.0);

    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    int offset = 1;
    for (Trace.Span row : rows) {
      chunkManager.addMessage(row, row.toString().length(), TEST_KAFKA_PARITION_ID, offset++);
    }
    chunkManager.getActiveChunk().commit();

    AstraSearch.SearchResult response =
        astraLocalQueryService.doSearch(
            AstraSearch.SearchRequest.newBuilder()
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setStartTimeEpochMs(startTime.toEpochMilli())
                .setEndTimeEpochMs(startTime.plusSeconds(1).toEpochMilli())
                .setHowMany(0)
                .setAggregationJson(
                    """
                    {
                      "dimensions": {
                        "multi_terms": {
                          "terms": [
                            {
                              "field": "country"
                            },
                            {
                              "field": "browser"
                            }
                          ],
                          "size": 10,
                          "order": {
                            "_count": "desc"
                          }
                        },
                        "aggs": {
                          "max_latency": {
                            "max": {
                              "field": "latency"
                            }
                          }
                        }
                      }
                    }
                    """)
                .build());

    InternalAggregations aggregations =
        OpenSearchInternalAggregation.fromByteArray(
            response.getInternalAggregations().toByteArray());
    InternalMultiTerms dimensions = (InternalMultiTerms) aggregations.get("dimensions");
    Map<List<Object>, Long> actualCounts =
        dimensions.getBuckets().stream()
            .collect(
                Collectors.toMap(
                    InternalMultiTerms.Bucket::getKey, InternalMultiTerms.Bucket::getDocCount));
    Map<List<Object>, Double> actualMaximumLatency =
        dimensions.getBuckets().stream()
            .collect(
                Collectors.toMap(
                    InternalMultiTerms.Bucket::getKey,
                    bucket ->
                        ((InternalMax) bucket.getAggregations().get("max_latency")).getValue()));

    assertThat(actualCounts).isEqualTo(expectedCounts);
    assertThat(actualMaximumLatency).isEqualTo(expectedMaximumLatency);
  }

  @Test
  public void testAstraBadArgSearch() throws Throwable {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                astraLocalQueryService.doSearch(
                    searchRequestBuilder
                        .setDataset(MessageUtil.TEST_DATASET_NAME)
                        .setQuery(
                            buildQueryFromQueryString(
                                "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                        .setStartTimeEpochMs(chunk1StartTimeMs)
                        .setEndTimeEpochMs(chunk1EndTimeMs)
                        .setHowMany(0)
                        .setAggregationJson("")
                        .build()));
  }

  /** Verifies local soft errors are propagated as shard failures in the search response. */
  @Test
  public void testAstraSearchPropagatesSoftErrorAsShardFailure() {
    @SuppressWarnings("unchecked")
    ChunkManager<LogMessage> chunkManager = mock(ChunkManager.class);
    when(chunkManager.query(any(), any())).thenReturn(SearchResult.localSoftFailure());

    AstraLocalQueryService<LogMessage> serviceUnderTest =
        new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3), false);

    Instant now = Instant.now();
    AstraSearch.SearchResult response =
        serviceUnderTest.doSearch(
            AstraSearch.SearchRequest.newBuilder()
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(
                    buildQueryFromQueryString("Message1", now.toEpochMilli(), now.toEpochMilli()))
                .setStartTimeEpochMs(now.toEpochMilli())
                .setEndTimeEpochMs(now.toEpochMilli())
                .setHowMany(10)
                .build());

    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isZero();
    assertThat(response.getRequestedSnapshots()).isEqualTo(1);
    assertThat(response.getFulfilledSnapshots()).isZero();
  }

  @Test
  public void shouldDisableDatasetFilterForDedicatedOnlyClusters() {
    @SuppressWarnings("unchecked")
    ChunkManager<LogMessage> chunkManager = mock(ChunkManager.class);
    when(chunkManager.query(any(), any())).thenReturn(SearchResult.localSoftFailure());
    AstraLocalQueryService<LogMessage> serviceUnderTest =
        new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3), true);

    serviceUnderTest.doSearch(
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setHowMany(1)
            .build());

    ArgumentCaptor<SearchQuery> queryCaptor = ArgumentCaptor.forClass(SearchQuery.class);
    verify(chunkManager).query(queryCaptor.capture(), any());
    assertThat(queryCaptor.getValue().applyDatasetFilter).isFalse();
  }

  @Test
  public void testAstraGrpcSearch() throws IOException {
    // Load test data into chunk manager.
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    // Setup a InProcess Grpc Server so we can query it.
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3), false))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    AstraServiceGrpc.AstraServiceBlockingStub blockingAstraClient =
        AstraServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a search request
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    AstraSearch.SearchResult response =
        blockingAstraClient.search(
            AstraSearch.SearchRequest.newBuilder()
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setQuery(buildQueryFromQueryString("Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                .setStartTimeEpochMs(chunk1StartTimeMs)
                .setEndTimeEpochMs(chunk1EndTimeMs)
                .setHowMany(10)
                .setAggregationJson(
                    buildHistogramRequestJSON(chunk1StartTimeMs, chunk1EndTimeMs, 2))
                .build());

    // Validate search response
    assertThat(response.getHitsCount()).isEqualTo(1);
    assertThat(response.getTookMicros()).isNotZero();
    assertThat(response.getFailedNodes()).isZero();
    assertThat(response.getTotalNodes()).isEqualTo(1);
    assertThat(response.getRequestedSnapshots()).isEqualTo(1);
    assertThat(response.getFulfilledSnapshots()).isEqualTo(1);

    // Test hit contents
    assertThat(response.getHits(0).getMessage()).contains("Message1");
    List<AstraSearch.SearchResult.Hit> hits = response.getHitsList();
    assertThat(hits.size()).isEqualTo(1);
    LogWireMessage hit = JsonUtil.read(hits.get(0).getMessage(), LogWireMessage.class);
    LogMessage m = LogMessage.fromWireMessage(hit);
    assertThat(m.getType()).isEqualTo(MessageUtil.TEST_MESSAGE_TYPE);
    assertThat(m.getIndex()).isEqualTo(MessageUtil.TEST_DATASET_NAME);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_LONG_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_INT_PROPERTY)).isEqualTo(1);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_FLOAT_PROPERTY)).isEqualTo(1.0);
    assertThat(m.getSource().get(MessageUtil.TEST_SOURCE_DOUBLE_PROPERTY)).isEqualTo(1.0);
    assertThat((String) m.getSource().get("message")).contains("Message1");

    // Test histogram buckets
    InternalAggregations internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(
            response.getInternalAggregations().toByteArray());
    InternalDateHistogram dateHistogram = internalAggregations.get("1");
    assertThat(dateHistogram.getBuckets().size()).isEqualTo(1);
    assertThat(dateHistogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAstraGrpcSearchThrowsException() throws IOException {
    // Load test data into chunk manager.
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;

    final Instant startTime = Instant.now();
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 100, 1000, startTime);
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARITION_ID, offset);
      offset++;
    }
    // No need to commit the active chunk since the last chunk is already closed.

    // Setup a InProcess Grpc Server so we can query it.
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new AstraLocalQueryService<>(chunkManager, Duration.ofSeconds(3), false))
            .build()
            .start());

    // Create a client channel and register for automatic graceful shutdown.
    AstraServiceGrpc.AstraServiceBlockingStub blockingStub =
        AstraServiceGrpc.newBlockingStub(
            // Create a client channel and register for automatic graceful shutdown.
            grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));

    // Build a bad search request.
    final long chunk1StartTimeMs = startTime.toEpochMilli();
    final long chunk1EndTimeMs = chunk1StartTimeMs + (10 * 1000);
    assertThatExceptionOfType(StatusRuntimeException.class)
        .isThrownBy(
            () ->
                blockingStub.search(
                    AstraSearch.SearchRequest.newBuilder()
                        .setDataset(MessageUtil.TEST_DATASET_NAME)
                        .setQuery(
                            buildQueryFromQueryString(
                                "Message1", chunk1StartTimeMs, chunk1EndTimeMs))
                        .setStartTimeEpochMs(chunk1StartTimeMs)
                        .setEndTimeEpochMs(chunk1EndTimeMs)
                        .setHowMany(0)
                        .setAggregationJson("")
                        .build()));
  }
}
