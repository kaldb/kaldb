package com.slack.astra.elasticsearchApi;

import static com.slack.astra.chunk.ChunkInfo.toSnapshotMetadata;
import static com.slack.astra.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static com.slack.astra.chunk.ReadWriteChunk.toSearchMetadata;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.Timestamp;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.chunk.SearchContext;
import com.slack.astra.logstore.DocumentBuilder;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.LuceneIndexStoreConfig;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.logstore.search.AstraDistributedQueryService;
import com.slack.astra.logstore.search.LogIndexSearcherImpl;
import com.slack.astra.logstore.search.SearchQuery;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.logstore.search.SearchResultUtils;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import com.slack.astra.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Readable query-node compatibility specs for ClickBench queries that require distributed final
 * reduction.
 *
 * <p>Query numbers match benchmark.clickhouse.com labels, which are zero-based over ClickBench's
 * {@code clickhouse/queries.sql} ordering. These tests intentionally keep benchmark coverage
 * separate from lower-level distributed service tests.
 */
class ClickBenchDistributedCompatibilityTest {
  private static final String DEFAULT_CLUSTER_NAME = "astra";
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 8081;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Instant NODE1_START = Instant.parse("2013-07-14T00:00:00Z");
  private static final Instant NODE2_START = Instant.parse("2013-07-14T01:00:00Z");
  private static final SearchContext INDEXER_1_SEARCH_CONTEXT =
      new SearchContext("indexer_host1", 10000);
  private static final SearchContext INDEXER_2_SEARCH_CONTEXT =
      new SearchContext("indexer_host2", 10001);

  private static ClickBenchSpec.Builder clickBench(String queryId) {
    return new ClickBenchSpec.Builder(queryId);
  }

  private static ClickBenchRow.Builder row(int id) {
    return new ClickBenchRow.Builder(id);
  }

  private static ClickBenchExpectation buckets(String aggregationName, ExpectedBucket... buckets) {
    return (response, spec) -> {
      JsonNode bucketNodes = response.path("aggregations").path(aggregationName).path("buckets");
      assertThat(bucketNodes.size())
          .as("%s: %s -> %s bucket count", spec.queryId(), spec.expects(), aggregationName)
          .isEqualTo(buckets.length);
      for (int i = 0; i < buckets.length; i++) {
        ExpectedBucket expectedBucket = buckets[i];
        JsonNode actualBucket = bucketNodes.get(i);
        assertThat(bucketKey(actualBucket))
            .as("%s: %s -> %s bucket %s key", spec.queryId(), spec.expects(), aggregationName, i)
            .isEqualTo(expectedBucket.key());
        assertThat(actualBucket.path("doc_count").asLong())
            .as(
                "%s: %s -> %s bucket %s doc_count",
                spec.queryId(), spec.expects(), aggregationName, i)
            .isEqualTo(expectedBucket.docCount());
      }
    };
  }

  private static ExpectedBucket bucket(Object key, long docCount) {
    return new ExpectedBucket(List.of(String.valueOf(key)), docCount);
  }

  private static ExpectedBucket bucket(List<?> key, long docCount) {
    return new ExpectedBucket(key.stream().map(String::valueOf).toList(), docCount);
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

  private static List<ClickBenchRow> q38Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addUrlRows(rows, 1, 1, "https://offset.example/a", 3);
    addUrlRows(rows, 4, 1, "https://offset.example/b", 1);
    addUrlRows(rows, 5, 1, "https://offset.example/c", 2);
    addUrlRows(rows, 7, 1, "https://offset.example/d", 1);
    addUrlRows(rows, 8, 2, "https://offset.example/a", 2);
    addUrlRows(rows, 10, 2, "https://offset.example/b", 3);
    addUrlRows(rows, 13, 2, "https://offset.example/c", 1);
    addUrlRows(rows, 14, 2, "https://offset.example/d", 1);
    rows.add(row(15).node(1).url("https://excluded.example").isRefresh(true).build());
    rows.add(row(16).node(1).url("https://excluded.example").isLink(0).build());
    rows.add(row(17).node(2).url("https://excluded.example").isDownload(1).build());
    rows.add(
        row(18).node(2).url("https://excluded.example").eventDate("2013-08-01T00:00:00Z").build());
    rows.add(row(19).node(2).url("https://excluded.example").counterId(61).build());
    return rows;
  }

  private static List<ClickBenchRow> q40Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addUrlHashDateRows(rows, 1, 1, 1001L, "2013-07-14T00:00:00Z", 3);
    addUrlHashDateRows(rows, 4, 1, 2002L, "2013-07-14T00:00:00Z", 1);
    addUrlHashDateRows(rows, 5, 1, 3003L, "2013-07-14T00:00:00Z", 2);
    addUrlHashDateRows(rows, 7, 1, 4004L, "2013-07-14T00:00:00Z", 1);
    addUrlHashDateRows(rows, 8, 2, 1001L, "2013-07-14T00:00:00Z", 2);
    addUrlHashDateRows(rows, 10, 2, 2002L, "2013-07-14T00:00:00Z", 3);
    addUrlHashDateRows(rows, 13, 2, 3003L, "2013-07-14T00:00:00Z", 1);
    addUrlHashDateRows(rows, 14, 2, 4004L, "2013-07-14T00:00:00Z", 1);
    rows.add(row(15).node(1).urlHash(9009L).isRefresh(true).build());
    rows.add(row(16).node(1).urlHash(9009L).traficSourceId(2).build());
    rows.add(row(17).node(2).urlHash(9009L).refererHash(1L).build());
    rows.add(row(18).node(2).urlHash(9009L).eventDate("2013-08-01T00:00:00Z").build());
    rows.add(row(19).node(2).urlHash(9009L).counterId(61).build());
    return rows;
  }

  private static List<ClickBenchRow> q41Rows() {
    List<ClickBenchRow> rows = new ArrayList<>();
    addWindowRows(rows, 1, 1, 1024, 768, 3);
    addWindowRows(rows, 4, 1, 1440, 900, 1);
    addWindowRows(rows, 5, 1, 800, 600, 2);
    addWindowRows(rows, 7, 1, 1920, 1080, 1);
    addWindowRows(rows, 8, 2, 1024, 768, 2);
    addWindowRows(rows, 10, 2, 1440, 900, 3);
    addWindowRows(rows, 13, 2, 800, 600, 1);
    addWindowRows(rows, 14, 2, 1920, 1080, 1);
    rows.add(row(15).node(1).windowSize(320, 200).isRefresh(true).build());
    rows.add(row(16).node(1).windowSize(320, 200).dontCountHits(true).build());
    rows.add(row(17).node(2).windowSize(320, 200).urlHash(1).build());
    rows.add(row(18).node(2).windowSize(320, 200).eventDate("2013-08-01T00:00:00Z").build());
    rows.add(row(19).node(2).windowSize(320, 200).counterId(61).build());
    return rows;
  }

  private static void addUrlRows(
      List<ClickBenchRow> rows, int firstId, int node, String url, int count) {
    for (int i = 0; i < count; i++) {
      rows.add(row(firstId + i).node(node).url(url).build());
    }
  }

  private static void addUrlHashDateRows(
      List<ClickBenchRow> rows, int firstId, int node, long urlHash, String eventDate, int count) {
    for (int i = 0; i < count; i++) {
      rows.add(row(firstId + i).node(node).urlHash(urlHash).eventDate(eventDate).build());
    }
  }

  private static void addWindowRows(
      List<ClickBenchRow> rows,
      int firstId,
      int node,
      int windowClientWidth,
      int windowClientHeight,
      int count) {
    for (int i = 0; i < count; i++) {
      rows.add(
          row(firstId + i).node(node).windowSize(windowClientWidth, windowClientHeight).build());
    }
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

  private static AstraConfigs.ZookeeperConfig zkConfig(TestingServer testZKServer) {
    return AstraConfigs.ZookeeperConfig.newBuilder()
        .setZkConnectString(testZKServer.getConnectString())
        .setZkPathPrefix("clickbenchDistributedTest")
        .setZkSessionTimeoutMs(1000)
        .setZkConnectionTimeoutMs(1000)
        .setSleepBetweenRetriesMs(1000)
        .setZkCacheInitTimeoutMs(1000)
        .build();
  }

  private static AstraConfigs.MetadataStoreConfig metadataStoreConfig(
      AstraConfigs.ZookeeperConfig zkConfig) {
    return AstraConfigs.MetadataStoreConfig.newBuilder()
        .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
        .setZookeeperConfig(zkConfig)
        .build();
  }

  private static AstraServiceGrpc.AstraServiceFutureStub mockSearchFutureStub(
      List<Trace.Span> spans) throws Exception {
    AstraServiceGrpc.AstraServiceFutureStub futureStub =
        mock(AstraServiceGrpc.AstraServiceFutureStub.class);
    when(futureStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(futureStub);
    when(futureStub.withInterceptors(any())).thenReturn(futureStub);
    when(futureStub.search(any(AstraSearch.SearchRequest.class)))
        .thenAnswer(
            invocation ->
                Futures.immediateFuture(
                    SearchResultUtils.toSearchResultProto(
                        searchLocally(invocation.getArgument(0), spans))));
    return futureStub;
  }

  private static SearchResult<LogMessage> searchLocally(
      AstraSearch.SearchRequest request, List<Trace.Span> spans) throws IOException {
    SearchQuery searchQuery = SearchResultUtils.fromSearchRequest(request);
    File tempFolder = java.nio.file.Files.createTempDirectory("clickbench-distributed").toFile();
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            Duration.of(1, ChronoUnit.MINUTES),
            Duration.of(1, ChronoUnit.MINUTES),
            tempFolder.getCanonicalPath(),
            false);
    MeterRegistry tempMetricsRegistry = new SimpleMeterRegistry();
    DocumentBuilder documentBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD,
            true,
            tempMetricsRegistry);
    LogStore logStore =
        new LuceneIndexStoreImpl(indexStoreCfg, documentBuilder, tempMetricsRegistry);
    LogIndexSearcherImpl logSearcher =
        new LogIndexSearcherImpl(logStore.getAstraSearcherManager(), logStore.getSchema());

    for (Trace.Span span : spans) {
      logStore.addMessage(span);
    }
    logStore.commit();
    logStore.refresh();

    try {
      return logSearcher.search(
          searchQuery.dataset,
          searchQuery.leafHowMany(),
          searchQuery.sortFieldSpecs,
          searchQuery.queryBuilder,
          searchQuery.sourceFieldFilter,
          searchQuery.aggregatorFactoriesBuilder);
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
      tempMetricsRegistry.close();
    }
  }

  private static Instant rowTimestamp(int node, int id) {
    return (node == 1 ? NODE1_START : NODE2_START).plusMillis(id);
  }

  private ElasticsearchApiService elasticsearchApiService;
  private SimpleMeterRegistry metricsRegistry;
  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private DatasetMetadataStore datasetMetadataStore;
  private TestingServer testZKServer;
  private TestDistributedQueryService distributedQueryService;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    metricsRegistry = new SimpleMeterRegistry();
    testZKServer = new TestingServer();

    AstraConfigs.ZookeeperConfig zkConfig = zkConfig(testZKServer);
    AstraConfigs.MetadataStoreConfig metadataStoreConfig = metadataStoreConfig(zkConfig);
    curatorFramework = CuratorBuilder.build(metricsRegistry, zkConfig);
    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry);
    searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry, true);
    datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, metadataStoreConfig, metricsRegistry, true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (distributedQueryService != null) {
      distributedQueryService.close();
    }
    if (snapshotMetadataStore != null) {
      snapshotMetadataStore.close();
    }
    if (searchMetadataStore != null) {
      searchMetadataStore.close();
    }
    if (datasetMetadataStore != null) {
      datasetMetadataStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (metricsRegistry != null) {
      metricsRegistry.close();
    }
    if (testZKServer != null) {
      testZKServer.close();
    }
  }

  @Test
  public void q38PageViewUrlsWithBucketOffset() throws Exception {
    verify(
        clickBench("Q38")
            .expects(
                "URL page-view buckets after Q38 filters, count ordering, and scaled bucket"
                    + " offset")
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
            .thenResponseContains(
                buckets(
                    "urls",
                    bucket("https://offset.example/c", 3),
                    bucket("https://offset.example/d", 2))));
  }

  @Test
  public void q40UrlHashDatesWithBucketOffset() throws Exception {
    verify(
        clickBench("Q40")
            .expects(
                "URL-hash date buckets after Q40 filters, count ordering, and scaled bucket"
                    + " offset")
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
            .thenResponseContains(
                buckets(
                    "url_hash_dates",
                    bucket(List.of(3003L, "2013-07-14T00:00:00.000Z"), 3),
                    bucket(List.of(4004L, "2013-07-14T00:00:00.000Z"), 2))));
  }

  @Test
  public void q41WindowSizesWithBucketOffset() throws Exception {
    verify(
        clickBench("Q41")
            .expects(
                "window-size page-view buckets after Q41 filters, count ordering, and scaled"
                    + " bucket offset")
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
            .thenResponseContains(
                buckets(
                    "window_sizes", bucket(List.of(800, 600), 3), bucket(List.of(1920, 1080), 2))));
  }

  private void verify(ClickBenchSpec spec) throws Exception {
    registerRows(spec.rows());
    AstraServiceGrpc.AstraServiceFutureStub node1Stub = mockSearchFutureStub(spans(spec.rows(), 1));
    AstraServiceGrpc.AstraServiceFutureStub node2Stub = mockSearchFutureStub(spans(spec.rows(), 2));
    startDistributedApiService();
    distributedQueryService.putStub(INDEXER_1_SEARCH_CONTEXT.toString(), node1Stub);
    distributedQueryService.putStub(INDEXER_2_SEARCH_CONTEXT.toString(), node2Stub);
    JsonNode response = searchJson(spec.requestJson());
    spec.expectations().forEach(expectation -> expectation.verify(response, spec));
  }

  private void startDistributedApiService() {
    distributedQueryService =
        new TestDistributedQueryService(
            searchMetadataStore,
            snapshotMetadataStore,
            datasetMetadataStore,
            metricsRegistry,
            Duration.ofSeconds(10),
            Duration.ofSeconds(10));
    elasticsearchApiService =
        new ElasticsearchApiService(
            distributedQueryService,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            datasetMetadataStore);
  }

  private void registerRows(List<ClickBenchRow> rows) {
    long start =
        rows.stream()
            .map(row -> row.eventTime())
            .min(Comparator.naturalOrder())
            .orElseThrow()
            .toEpochMilli();
    long end =
        rows.stream()
            .map(row -> row.eventTime())
            .max(Comparator.naturalOrder())
            .orElseThrow()
            .toEpochMilli();
    datasetMetadataStore.createSync(
        new DatasetMetadata(
            TEST_DATASET_NAME,
            "testOwner",
            1,
            List.of(
                new DatasetPartitionMetadata(start, Math.max(start + 1, end), List.of("1", "2"))),
            TEST_DATASET_NAME));

    registerPartitionRows("1", INDEXER_1_SEARCH_CONTEXT, rows);
    registerPartitionRows("2", INDEXER_2_SEARCH_CONTEXT, rows);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size() == 1);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore).size() == 4);
    await().until(() -> AstraMetadataTestUtils.listSyncUncached(searchMetadataStore).size() == 2);
  }

  private void registerPartitionRows(
      String partition, SearchContext searchContext, List<ClickBenchRow> rows) {
    List<ClickBenchRow> partitionRows =
        rows.stream().filter(row -> String.valueOf(row.node()).equals(partition)).toList();
    Instant start =
        partitionRows.stream()
            .map(ClickBenchRow::eventTime)
            .min(Comparator.naturalOrder())
            .orElseThrow();
    Instant end =
        partitionRows.stream()
            .map(ClickBenchRow::eventTime)
            .max(Comparator.naturalOrder())
            .orElseThrow();
    createIndexerZKMetadata(start, end, partition, searchContext);
  }

  private void createIndexerZKMetadata(
      Instant chunkCreationTime,
      Instant chunkEndTime,
      String partition,
      SearchContext searchContext) {
    SnapshotMetadata liveSnapshotMetadata =
        createSnapshot(chunkCreationTime, chunkEndTime, true, partition);
    searchMetadataStore.createSync(toSearchMetadata(liveSnapshotMetadata.name, searchContext));
  }

  private SnapshotMetadata createSnapshot(
      Instant chunkCreationTime, Instant chunkEndTime, boolean isLive, String partition) {
    String chunkName = "logStore_" + chunkCreationTime.getEpochSecond() + "_" + UUID.randomUUID();
    ChunkInfo chunkInfo =
        new ChunkInfo(
            chunkName,
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkCreationTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            chunkEndTime.toEpochMilli(),
            1234,
            partition,
            0);
    SnapshotMetadata snapshotMetadata =
        toSnapshotMetadata(chunkInfo, isLive ? LIVE_SNAPSHOT_PREFIX : "");
    snapshotMetadataStore.createSync(snapshotMetadata);

    if (isLive) {
      snapshotMetadataStore.createSync(toSnapshotMetadata(chunkInfo, ""));
    }

    return snapshotMetadata;
  }

  private List<Trace.Span> spans(List<ClickBenchRow> rows, int node) {
    return rows.stream().filter(row -> row.node() == node).map(this::span).toList();
  }

  private Trace.Span span(ClickBenchRow row) {
    return SpanUtil.makeSpan(
        row.id(),
        "clickbench-distributed-message-" + row.id(),
        row.eventTime(),
        List.of(
            keywordField("URL", row.url()),
            integerField("CounterID", row.counterId()),
            dateField("EventDate", row.eventDate()),
            booleanField("IsRefresh", row.isRefresh()),
            integerField("IsLink", row.isLink()),
            integerField("IsDownload", row.isDownload()),
            booleanField("DontCountHits", row.dontCountHits()),
            longField("URLHash", row.urlHash()),
            integerField("TraficSourceID", row.traficSourceId()),
            longField("RefererHash", row.refererHash()),
            integerField("WindowClientWidth", row.windowClientWidth()),
            integerField("WindowClientHeight", row.windowClientHeight())));
  }

  private JsonNode searchJson(String postBody) throws Exception {
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    return OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));
  }

  private interface ClickBenchExpectation {
    void verify(JsonNode response, ClickBenchSpec spec);
  }

  private record ExpectedBucket(List<String> key, long docCount) {}

  private record ClickBenchRow(
      int id,
      int node,
      Instant eventTime,
      Instant eventDate,
      String url,
      int counterId,
      boolean isRefresh,
      int isLink,
      int isDownload,
      boolean dontCountHits,
      long urlHash,
      int traficSourceId,
      long refererHash,
      int windowClientWidth,
      int windowClientHeight) {
    private static final class Builder {
      private final int id;
      private int node = 1;
      private Instant eventTime;
      private Instant eventDate = Instant.parse("2013-07-14T00:00:00Z");
      private String url = "";
      private int counterId = 62;
      private boolean isRefresh;
      private int isLink = 1;
      private int isDownload;
      private boolean dontCountHits;
      private long urlHash = 2868770270353813622L;
      private int traficSourceId = 6;
      private long refererHash = 3594120000172545465L;
      private int windowClientWidth = 1024;
      private int windowClientHeight = 768;

      private Builder(int id) {
        this.id = id;
      }

      private Builder node(int node) {
        this.node = node;
        return this;
      }

      private Builder eventDate(String eventDate) {
        this.eventDate = Instant.parse(eventDate);
        return this;
      }

      private Builder url(String url) {
        this.url = url;
        return this;
      }

      private Builder counterId(int counterId) {
        this.counterId = counterId;
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

      private Builder dontCountHits(boolean dontCountHits) {
        this.dontCountHits = dontCountHits;
        return this;
      }

      private Builder urlHash(long urlHash) {
        this.urlHash = urlHash;
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

      private Builder windowSize(int windowClientWidth, int windowClientHeight) {
        this.windowClientWidth = windowClientWidth;
        this.windowClientHeight = windowClientHeight;
        return this;
      }

      private ClickBenchRow build() {
        Instant rowEventTime = eventTime == null ? rowTimestamp(node, id) : eventTime;
        return new ClickBenchRow(
            id,
            node,
            rowEventTime,
            eventDate,
            url,
            counterId,
            isRefresh,
            isLink,
            isDownload,
            dontCountHits,
            urlHash,
            traficSourceId,
            refererHash,
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

  private static final class TestDistributedQueryService extends AstraDistributedQueryService {
    private TestDistributedQueryService(
        SearchMetadataStore searchMetadataStore,
        SnapshotMetadataStore snapshotMetadataStore,
        DatasetMetadataStore datasetMetadataStore,
        MeterRegistry meterRegistry,
        Duration requestTimeout,
        Duration defaultQueryTimeout) {
      super(
          searchMetadataStore,
          snapshotMetadataStore,
          datasetMetadataStore,
          meterRegistry,
          requestTimeout,
          defaultQueryTimeout);
    }

    private void putStub(String url, AstraServiceGrpc.AstraServiceFutureStub stub) {
      stubs.put(url, stub);
    }
  }
}
