package com.slack.astra.elasticsearchApi;

import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser.convertRequestToDocument;
import static com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParserTest.getIndexRequestBytes;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.writer.LogMessageWriterImplTest.consumerRecordWithValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.slack.astra.bulkIngestApi.opensearch.BulkApiRequestParser;
import com.slack.astra.chunkManager.IndexingChunkManager;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.AstraLocalQueryService;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.schema.SchemaUtil;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.testlib.AstraConfigUtil;
import com.slack.astra.testlib.ChunkManagerUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.writer.LogMessageWriterImpl;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestDocument;

@SuppressWarnings("UnstableApiUsage")
public class ElasticsearchApiServiceTest {
  private static final String S3_TEST_BUCKET = "test-astra-logs";
  private static final String DEFAULT_CLUSTER_NAME = "astra";
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 8081;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private static final String TEST_KAFKA_PARTITION_ID = "10";

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
  public void testSchemaIsRetainedAndDynamicFieldsAreDroppedOverLimit() throws Exception {
    // Load schema from test_schema.yaml
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    // Build a request with all schema fields (reusing test fixture) and extra dynamic fields
    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    // Insert schema-based spans first
    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    // Now add one large span with 3000 dynamic fields
    Trace.Span.Builder spanBuilder = SpanUtil.makeSpan(100).toBuilder();
    for (int i = 0; i < 3000; i++) {
      spanBuilder.addTags(
          Trace.KeyValue.newBuilder()
              .setKey("dynamic.extra_field." + i)
              .setVStr("value" + i)
              .setIndexSignal(Trace.IndexSignal.DYNAMIC_INDEX)
              .build());
    }
    Trace.Span spanWithExtras = spanBuilder.build();
    ConsumerRecord<String, byte[]> extraSpanRecord =
        consumerRecordWithValue(spanWithExtras.toByteArray());
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
    assertThat(messageWriter.insertRecord(extraSpanRecord)).isTrue();

    // Validate counts
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    // Fetch and parse mapping
    HttpResponse response =
        elasticsearchApiService.mapping("test", Optional.of(0L), Optional.of(Long.MAX_VALUE));

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);
    assertThat(jsonNode).isNotNull();

    Map<String, Object> map =
        OBJECT_MAPPER.convertValue(
            jsonNode.get("test").get("mappings").get("properties"), Map.class);

    Set<String> schemaKeys =
        Set.of(
            "host",
            "message",
            "ip",
            "my_date",
            "success",
            "cost",
            "amount",
            "amount_half_float",
            "message.keyword",
            "value",
            "count",
            "count_scaled_long",
            "count_short",
            "bucket");

    // Verify all original schema fields are retained
    assertThat(map.keySet()).containsAll(schemaKeys);

    // Additional Keys that are required via indexing logic.
    Set<String> requiredKeys =
        Set.of(
            "@timestamp",
            "_all",
            "_id",
            "_index",
            "_source",
            "_timesinceepoch",
            "doubleproperty",
            "duration",
            "floatproperty",
            "intproperty",
            "longproperty",
            "name",
            "parent_id",
            "service_name",
            "stringproperty",
            "trace_id",
            "binaryproperty");

    // Verify all required keys are retained
    assertThat(map.keySet()).containsAll(requiredKeys);

    int dynamicFieldsCount = map.keySet().size() - schemaKeys.size() - requiredKeys.size();
    assertThat(dynamicFieldsCount)
        .withFailMessage(
            "Expected dynamic field count to not exceed limit but got %s", dynamicFieldsCount)
        .isEqualTo(1500);
  }

  @Test
  public void testSchemaIsRetainedAndDynamicFieldsAreNotDroppedWhenUNKNOWN() throws Exception {
    // Load schema from test_schema.yaml
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    // Build a request with all schema fields (reusing test fixture) and extra dynamic fields
    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    // Insert schema-based spans first
    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    // Now add one large span with 3000 dynamic fields
    Trace.Span.Builder spanBuilder = SpanUtil.makeSpan(100).toBuilder();
    for (int i = 0; i < 3000; i++) {
      spanBuilder.addTags(
          Trace.KeyValue.newBuilder()
              .setKey("dynamic.extra_field." + i)
              .setVStr("value" + i)
              .build());
    }
    Trace.Span spanWithExtras = spanBuilder.build();
    ConsumerRecord<String, byte[]> extraSpanRecord =
        consumerRecordWithValue(spanWithExtras.toByteArray());
    LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
    assertThat(messageWriter.insertRecord(extraSpanRecord)).isTrue();

    // Validate counts
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    // Fetch and parse mapping
    HttpResponse response =
        elasticsearchApiService.mapping("test", Optional.of(0L), Optional.of(Long.MAX_VALUE));

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);
    assertThat(jsonNode).isNotNull();

    Map<String, Object> map =
        OBJECT_MAPPER.convertValue(
            jsonNode.get("test").get("mappings").get("properties"), Map.class);

    Set<String> schemaKeys =
        Set.of(
            "host",
            "message",
            "ip",
            "my_date",
            "success",
            "cost",
            "amount",
            "amount_half_float",
            "value",
            "count",
            "count_scaled_long",
            "count_short",
            "bucket");

    // Verify all original schema fields are retained
    assertThat(map.keySet()).containsAll(schemaKeys);

    // Additional Keys that are required via indexing logic.
    Set<String> requiredKeys =
        Set.of(
            "@timestamp",
            "_all",
            "_id",
            "_index",
            "_source",
            "_timesinceepoch",
            "doubleproperty",
            "duration",
            "floatproperty",
            "intproperty",
            "longproperty",
            "message.keyword",
            "name",
            "parent_id",
            "service_name",
            "stringproperty",
            "trace_id",
            "username",
            "binaryproperty");

    // Verify all required keys are retained
    assertThat(map.keySet()).containsAll(requiredKeys);

    // No dynamic fields are dropped with the "old" logic when preprocessor does not have
    // indexSignal set.
    int dynamicFieldsCount = map.keySet().size() - schemaKeys.size() - requiredKeys.size();
    assertThat(dynamicFieldsCount)
        .withFailMessage(
            "Expected dynamic field count to not exceed limit but got %s", dynamicFieldsCount)
        .isEqualTo(3000);
  }

  @Test
  public void testSchemaFields() throws Exception {
    final File schemaFile =
        new File(getClass().getClassLoader().getResource("schema/test_schema.yaml").getFile());
    Schema.IngestSchema schema = SchemaUtil.parseSchema(schemaFile.toPath());

    byte[] rawRequest = getIndexRequestBytes("index_all_schema_fields");
    List<IndexRequest> indexRequests = BulkApiRequestParser.parseBulkRequest(rawRequest);
    assertThat(indexRequests.size()).isEqualTo(2);

    for (IndexRequest indexRequest : indexRequests) {
      IngestDocument ingestDocument = convertRequestToDocument(indexRequest);
      Trace.Span span = BulkApiRequestParser.fromIngestDocument(ingestDocument, schema);
      ConsumerRecord<String, byte[]> spanRecord = consumerRecordWithValue(span.toByteArray());
      LogMessageWriterImpl messageWriter = new LogMessageWriterImpl(chunkManagerUtil.chunkManager);
      assertThat(messageWriter.insertRecord(spanRecord)).isTrue();
    }

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, metricsRegistry)).isEqualTo(0);
    chunkManagerUtil.chunkManager.getActiveChunk().commit();

    HttpResponse response =
        elasticsearchApiService.mapping("test", Optional.of(0L), Optional.of(Long.MAX_VALUE));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);
    assertThat(jsonNode).isNotNull();

    Map<String, Object> map =
        OBJECT_MAPPER.convertValue(
            jsonNode.get("test").get("mappings").get("properties"), Map.class);
    assertThat(map).isNotNull();
    assertThat(map.size()).isEqualTo(26);
  }

  // todo - test mapping
  @Test
  public void testResultsAreReturnedForValidQuery() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody = readResource("elasticsearchApi/multisearch_query_500results.ndjson");
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(100);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .findValue("message")
                .asText()
                .endsWith("Message100"))
        .isTrue();
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(99)
                .findValue("message")
                .asText()
                .endsWith("Message1"))
        .isTrue();
  }

  @Test
  public void testSearchStringWithOneResult() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody = readResource("elasticsearchApi/multisearch_query_1results.ndjson");
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(1);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .findValue("message")
                .asText()
                .endsWith("Message70"))
        .isTrue();
  }

  @Test
  public void testSearchStringWithNoResult() throws Exception {
    // add 100 results around now
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    // queries for 1 second duration in year 2056
    String postBody = readResource("elasticsearchApi/multisearch_query_0results.ndjson");
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(0);
  }

  @Test
  public void testResultSizeIsRespected() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody = readResource("elasticsearchApi/multisearch_query_10results.ndjson");
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(10);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .findValue("message")
                .asText()
                .endsWith("Message100"))
        .isTrue();
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(9)
                .findValue("message")
                .asText()
                .endsWith("Message91"))
        .isTrue();
  }

  @Test
  public void testSingleSearchReturnsAttributeAggregation() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        """
        {"size":0,"query":{"match_all":{}},"aggs":{"services":{"terms":{"field":"service_name","size":10}}}}
        """;
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("services").get("buckets").size()).isEqualTo(1);
    assertThat(
            jsonNode.get("aggregations").get("services").get("buckets").get(0).get("key").asText())
        .isEqualTo(TEST_DATASET_NAME);
    assertThat(
            jsonNode
                .get("aggregations")
                .get("services")
                .get("buckets")
                .get(0)
                .get("doc_count")
                .asInt())
        .isEqualTo(100);
    assertThat(jsonNode.get("_shards").get("total").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("_shards").get("failed").asInt()).isEqualTo(0);
  }

  @Test
  public void testSingleSearchReturnsMultipleSiblingAggregations() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        """
        {
          "size": 0,
          "query": {
            "match_all": {}
          },
          "aggs": {
            "over_time": {
              "date_histogram": {
                "field": "@timestamp",
                "interval": "1h",
                "min_doc_count": 1
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
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("over_time").get("buckets").size()).isEqualTo(1);
    assertThat(jsonNode.get("aggregations").get("services").get("buckets").size()).isEqualTo(1);
    assertThat(
            jsonNode
                .get("aggregations")
                .get("services")
                .get("buckets")
                .get(0)
                .get("doc_count")
                .asInt())
        .isEqualTo(100);
  }

  @Test
  public void testSingleSearchReturnsSiblingMetricAggregationsOnSameField() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        """
        {
          "size": 0,
          "query": {
            "match_all": {}
          },
          "aggs": {
            "avg_longproperty": {
              "avg": {
                "field": "longproperty"
              }
            },
            "max_longproperty": {
              "max": {
                "field": "longproperty"
              }
            }
          }
        }
        """;
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("avg_longproperty").get("value").asDouble())
        .isCloseTo(50.5, Offset.offset(0.001));
    assertThat(jsonNode.get("aggregations").get("max_longproperty").get("value").asDouble())
        .isCloseTo(100.0, Offset.offset(0.001));
    assertThat(jsonNode.get("_shards").get("total").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("_shards").get("failed").asInt()).isEqualTo(0);
  }

  @Test
  public void testSingleSearchReturnsSiblingMetricAggregationsAcrossFields() throws Exception {
    Instant start = Instant.parse("2026-05-08T08:00:00Z");
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, start));

    String postBody =
        """
        {
          "size": 0,
          "query": {
            "match_all": {}
          },
          "aggs": {
            "max_longproperty": {
              "max": {
                "field": "longproperty"
              }
            },
            "min_timestamp": {
              "min": {
                "field": "@timestamp"
              }
            }
          }
        }
        """;
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("max_longproperty").get("value").asDouble())
        .isCloseTo(100.0, Offset.offset(0.001));
    assertThat(jsonNode.get("aggregations").get("min_timestamp").get("value").asDouble())
        .isCloseTo((double) start.toEpochMilli(), Offset.offset(0.001));
    assertThat(jsonNode.get("_shards").get("total").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("_shards").get("failed").asInt()).isEqualTo(0);
  }

  @Test
  public void testSingleSearchReturnsMinAndMaxTimestampAggregations() throws Exception {
    Instant start = Instant.parse("2026-05-08T09:00:00Z");
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, start));

    String postBody =
        """
        {
          "size": 0,
          "query": {
            "match_all": {}
          },
          "aggs": {
            "min_timestamp": {
              "min": {
                "field": "@timestamp"
              }
            },
            "max_timestamp": {
              "max": {
                "field": "@timestamp"
              }
            }
          }
        }
        """;
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("min_timestamp").get("value").asDouble())
        .isCloseTo((double) start.toEpochMilli(), Offset.offset(0.001));
    assertThat(jsonNode.get("aggregations").get("max_timestamp").get("value").asDouble())
        .isCloseTo((double) start.plusMillis(99).toEpochMilli(), Offset.offset(0.001));
    assertThat(jsonNode.get("_shards").get("total").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("_shards").get("failed").asInt()).isEqualTo(0);
  }

  @Test
  public void testSingleSearchReturnsNestedAndSiblingAggregations() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));

    String postBody =
        """
        {
          "size": 0,
          "query": {
            "match_all": {}
          },
          "aggs": {
            "over_time": {
              "date_histogram": {
                "field": "@timestamp",
                "interval": "1h",
                "min_doc_count": 1
              },
              "aggs": {
                "bucket_services": {
                  "terms": {
                    "field": "service_name",
                    "size": 10
                  }
                }
              }
            },
            "all_services": {
              "terms": {
                "field": "service_name",
                "size": 10
              }
            }
          }
        }
        """;
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("over_time").get("buckets").size()).isEqualTo(1);
    assertThat(jsonNode.get("aggregations").get("all_services").get("buckets").size()).isEqualTo(1);
    assertThat(
            jsonNode
                .get("aggregations")
                .get("over_time")
                .get("buckets")
                .get(0)
                .get("bucket_services")
                .get("buckets")
                .size())
        .isEqualTo(1);
    assertThat(
            jsonNode
                .get("aggregations")
                .get("over_time")
                .get("buckets")
                .get(0)
                .get("bucket_services")
                .get("buckets")
                .get(0)
                .get("doc_count")
                .asInt())
        .isEqualTo(100);
    assertThat(
            jsonNode
                .get("aggregations")
                .get("all_services")
                .get("buckets")
                .get(0)
                .get("doc_count")
                .asInt())
        .isEqualTo(100);
  }

  @Test
  public void testSingleSearchReturnsMultiTermsAggregation() throws Exception {
    Instant start = Instant.parse("2026-05-18T03:00:00Z");
    addMessagesToChunkManager(
        List.of(
            makeWindowSpan(1, start.plusSeconds(1), 62, 1024, 768, false, false),
            makeWindowSpan(2, start.plusSeconds(2), 62, 1024, 768, false, false),
            makeWindowSpan(3, start.plusSeconds(3), 62, 1440, 900, false, false),
            makeWindowSpan(4, start.plusSeconds(4), 62, 800, 600, false, false),
            makeWindowSpan(5, start.plusSeconds(5), 62, 1600, 900, false, true),
            makeWindowSpan(6, start.plusSeconds(6), 61, 2560, 1440, false, false),
            makeWindowSpan(7, start.plusSeconds(7), 62, 1024, 768, false, false),
            makeWindowSpan(8, start.plusSeconds(8), 62, 1024, 768, false, false),
            makeWindowSpan(9, start.plusSeconds(9), 62, 1440, 900, false, false),
            makeWindowSpan(10, start.plusSeconds(10), 62, 1440, 900, false, false),
            makeWindowSpan(11, start.plusSeconds(11), 62, 800, 600, false, false),
            makeWindowSpan(12, start.plusSeconds(12), 62, 1920, 1080, true, false)));

    String postBody =
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
              }
            }
          }
        }
        """;
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);

    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);
    Map<List<Integer>, Integer> bucketCounts = new java.util.HashMap<>();
    for (JsonNode bucket : jsonNode.get("aggregations").get("window_sizes").get("buckets")) {
      bucketCounts.put(
          List.of(bucket.get("key").get(0).asInt(), bucket.get("key").get(1).asInt()),
          bucket.get("doc_count").asInt());
    }

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("aggregations").get("window_sizes").get("buckets").size()).isEqualTo(3);
    assertThat(bucketCounts)
        .containsEntry(List.of(1024, 768), 4)
        .containsEntry(List.of(1440, 900), 3)
        .containsEntry(List.of(800, 600), 2);
  }

  @Test
  public void testSingleSearchReturnsHitsSortedByRequestedField() throws Exception {
    Instant start = Instant.parse("2026-05-18T04:00:00Z");
    addMessagesToChunkManager(
        List.of(
            makeWindowSpan(1, start.plusSeconds(1), 62, 1024, 768, false, false, "bravo"),
            makeWindowSpan(2, start.plusSeconds(2), 62, 800, 600, false, false, "charlie"),
            makeWindowSpan(3, start.plusSeconds(3), 62, 1440, 900, false, false, "alpha")));

    String postBody =
        """
        {
          "size": 3,
          "query": {
            "term": {
              "CounterID": 62
            }
          },
          "sort": [
            {
              "WindowClientWidth": {
                "order": "asc"
              }
            }
          ]
        }
        """;
    JsonNode jsonNode = searchJson(postBody);

    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(3);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .get("_source")
                .get("WindowClientWidth")
                .asInt())
        .isEqualTo(800);
    assertThat(jsonNode.findValue("hits").get("hits").get(0).get("sort").get(0).asInt())
        .isEqualTo(800);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(1)
                .get("_source")
                .get("WindowClientWidth")
                .asInt())
        .isEqualTo(1024);
    assertThat(
            jsonNode
                .findValue("hits")
                .get("hits")
                .get(2)
                .get("_source")
                .get("WindowClientWidth")
                .asInt())
        .isEqualTo(1440);

    String stringSortPostBody =
        """
        {
          "size": 3,
          "query": {
            "term": {
              "CounterID": 62
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
        """;
    JsonNode stringSortJsonNode = searchJson(stringSortPostBody);

    assertThat(
            stringSortJsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .get("_source")
                .get("SearchPhrase")
                .asText())
        .isEqualTo("alpha");
    assertThat(stringSortJsonNode.findValue("hits").get("hits").get(0).get("sort").get(0).asText())
        .isEqualTo("alpha");
    assertThat(
            stringSortJsonNode
                .findValue("hits")
                .get("hits")
                .get(1)
                .get("_source")
                .get("SearchPhrase")
                .asText())
        .isEqualTo("bravo");
    assertThat(
            stringSortJsonNode
                .findValue("hits")
                .get("hits")
                .get(2)
                .get("_source")
                .get("SearchPhrase")
                .asText())
        .isEqualTo("charlie");

    String pagedStringSortPostBody =
        """
        {
          "from": 1,
          "size": 1,
          "query": {
            "term": {
              "CounterID": 62
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
        """;
    JsonNode pagedStringSortJsonNode = searchJson(pagedStringSortPostBody);

    assertThat(pagedStringSortJsonNode.findValue("hits").get("hits").size()).isEqualTo(1);
    assertThat(
            pagedStringSortJsonNode
                .findValue("hits")
                .get("hits")
                .get(0)
                .get("_source")
                .get("SearchPhrase")
                .asText())
        .isEqualTo("bravo");
  }

  @Test
  public void testSingleSearchSupportsClickBenchHitSortQueries() throws Exception {
    Instant start = Instant.parse("2026-05-18T05:00:00Z");
    addMessagesToChunkManager(
        List.of(
            makeClickBenchHitSpan(1, start.plusSeconds(3), "https://mail.google.example", "zulu"),
            makeClickBenchHitSpan(2, start.plusSeconds(1), "https://slack.example", ""),
            makeClickBenchHitSpan(
                3, start.plusSeconds(2), "https://google.example/search", "delta"),
            makeClickBenchHitSpan(4, start.plusSeconds(4), "https://astra.example", "bravo"),
            makeClickBenchHitSpan(5, start.plusSeconds(5), "https://google.example/maps", ""),
            makeClickBenchHitSpan(6, start.plusSeconds(2), "https://astra.example/docs", "alpha")));

    JsonNode q24 =
        searchJson(
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
            """);
    assertThat(sourceValues(q24, "URL"))
        .containsExactly(
            "https://google.example/search",
            "https://mail.google.example",
            "https://google.example/maps");

    JsonNode q25 =
        searchJson(
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
            """);
    assertThat(sourceValues(q25, "SearchPhrase"))
        .containsExactly("delta", "alpha", "zulu", "bravo");

    JsonNode q26 =
        searchJson(
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
            """);
    assertThat(sourceValues(q26, "SearchPhrase"))
        .containsExactly("alpha", "bravo", "delta", "zulu");

    JsonNode q27 =
        searchJson(
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
            """);
    assertThat(sourceValues(q27, "SearchPhrase"))
        .containsExactly("alpha", "delta", "zulu", "bravo");
  }

  @Test
  public void testLargeSetOfQueries() throws Exception {
    addMessagesToChunkManager(SpanUtil.makeSpansWithTimeDifference(1, 100, 1, Instant.now()));
    String postBody = readResource("elasticsearchApi/multisearch_query_10results.ndjson");
    AstraLocalQueryService<LogMessage> slowSearcher =
        spy(new AstraLocalQueryService<>(chunkManagerUtil.chunkManager, Duration.ofSeconds(5)));

    // warmup to load OpenSearch plugins
    ElasticsearchApiService slowElasticsearchApiService =
        new ElasticsearchApiService(
            slowSearcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));
    slowElasticsearchApiService.multiSearch(postBody);

    slowElasticsearchApiService =
        new ElasticsearchApiService(
            slowSearcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));
    HttpResponse response = slowElasticsearchApiService.multiSearch(postBody.repeat(100));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);

    // ensure we have all 100 results
    assertThat(jsonNode.get("responses").size()).isEqualTo(100);
  }

  @Test
  public void testEmptySearchGrafana7() throws Exception {
    String postBody = readResource("elasticsearchApi/empty_search_grafana7.ndjson");
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(0);
  }

  @Test
  public void testEmptySearchGrafana8() throws Exception {
    String postBody = readResource("elasticsearchApi/empty_search_grafana8.ndjson");
    HttpResponse response = elasticsearchApiService.multiSearch(postBody);

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.findValue("hits").get("hits").size()).isEqualTo(0);
  }

  @Test
  public void testIndexMapping() throws IOException {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));

    Instant start = Instant.now();
    Instant end = start.plusSeconds(60);

    when(searcher.getSchema(
            eq(
                AstraSearch.SchemaRequest.newBuilder()
                    .setDataset("foo")
                    .setStartTimeEpochMs(start.toEpochMilli())
                    .setEndTimeEpochMs(end.toEpochMilli())
                    .build())))
        .thenReturn(AstraSearch.SchemaResult.newBuilder().build());

    HttpResponse response =
        serviceUnderTest.mapping(
            "foo", Optional.of(start.toEpochMilli()), Optional.of(end.toEpochMilli()));
    verify(searcher)
        .getSchema(
            eq(
                AstraSearch.SchemaRequest.newBuilder()
                    .setDataset("foo")
                    .setStartTimeEpochMs(start.toEpochMilli())
                    .setEndTimeEpochMs(end.toEpochMilli())
                    .build()));

    // handle response
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();
    String body = aggregatedRes.content(StandardCharsets.UTF_8);
    JsonNode jsonNode = OBJECT_MAPPER.readTree(body);

    assertThat(aggregatedRes.status().code()).isEqualTo(200);

    assertThat(jsonNode.findValue("foo")).isNotNull();
    assertThat(
            jsonNode.findValue("foo").findValue(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName))
        .isNotNull();

    when(searcher.getSchema(any()))
        .thenAnswer(
            invocationOnMock -> {
              AstraSearch.SchemaRequest request =
                  ((AstraSearch.SchemaRequest) invocationOnMock.getArguments()[0]);
              assertThat(request.getDataset()).isEqualTo("bar");
              assertThat(request.getStartTimeEpochMs())
                  .isCloseTo(
                      Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli(),
                      Offset.offset(1000L));
              assertThat(request.getEndTimeEpochMs())
                  .isCloseTo(Instant.now().toEpochMilli(), Offset.offset(1000L));
              return AstraSearch.SchemaResult.newBuilder().build();
            });
    serviceUnderTest.mapping("bar", Optional.empty(), Optional.empty());
  }

  @Test
  public void testClusterMetadataLooksLikeOpenSearch() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));

    AggregatedHttpResponse aggregatedRes = serviceUnderTest.clusterMetadata().aggregate().join();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("cluster_name").asText()).isEqualTo("astra");
    assertThat(jsonNode.get("version").get("number").asText()).isEqualTo("2.11.1");
  }

  @Test
  public void testNodesInfoReturnsSingleCompatibleNode() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));

    AggregatedHttpResponse aggregatedRes = serviceUnderTest.nodesInfo().aggregate().join();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("_nodes").get("total").asInt()).isEqualTo(1);
    assertThat(jsonNode.get("nodes").get("localhost:8081").get("version").asText())
        .isEqualTo("2.11.1");
  }

  @Test
  public void testFieldCapabilitiesUsesSchema() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));

    Instant start = Instant.now();
    Instant end = start.plusSeconds(60);
    when(searcher.getSchema(
            eq(
                AstraSearch.SchemaRequest.newBuilder()
                    .setDataset("foo")
                    .setStartTimeEpochMs(start.toEpochMilli())
                    .setEndTimeEpochMs(end.toEpochMilli())
                    .build())))
        .thenReturn(
            AstraSearch.SchemaResult.newBuilder()
                .putFieldDefinition(
                    "message",
                    AstraSearch.SchemaDefinition.newBuilder()
                        .setType(Schema.SchemaFieldType.TEXT)
                        .build())
                .putFieldDefinition(
                    "service_name",
                    AstraSearch.SchemaDefinition.newBuilder()
                        .setType(Schema.SchemaFieldType.KEYWORD)
                        .build())
                .build());

    AggregatedHttpResponse aggregatedRes =
        serviceUnderTest
            .fieldCapabilities(
                "foo",
                Optional.of(start.toEpochMilli()),
                Optional.of(end.toEpochMilli()),
                Optional.empty())
            .aggregate()
            .join();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("fields").get("message").has("text")).isTrue();
    assertThat(jsonNode.get("fields").get("message").get("text").get("aggregatable").asBoolean())
        .isFalse();
    assertThat(jsonNode.get("fields").get("service_name").has("keyword")).isTrue();
    assertThat(jsonNode.get("fields").get("@timestamp").has("date")).isTrue();
  }

  @Test
  public void testResolveIndexReturnsExactDatasetMatch() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    DatasetMetadataStore datasetMetadataStore = mock(DatasetMetadataStore.class);
    when(datasetMetadataStore.listSync())
        .thenReturn(
            List.of(datasetMetadata("bar"), datasetMetadata("foo"), datasetMetadata("foo_logs")));
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher, DEFAULT_CLUSTER_NAME, DEFAULT_HOST, DEFAULT_PORT, datasetMetadataStore);

    AggregatedHttpResponse aggregatedRes = serviceUnderTest.resolveIndex("foo").aggregate().join();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("indices")).hasSize(1);
    assertThat(jsonNode.get("indices").get(0).get("name").asText()).isEqualTo("foo");
    assertThat(jsonNode.get("indices").get(0).get("attributes")).hasSize(1);
    assertThat(jsonNode.get("indices").get(0).get("attributes").get(0).asText()).isEqualTo("open");
    assertThat(jsonNode.get("aliases")).isEmpty();
    assertThat(jsonNode.get("data_streams")).isEmpty();
  }

  @Test
  public void testResolveIndexReturnsEmptyForWildcardSelector() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    DatasetMetadataStore datasetMetadataStore = mock(DatasetMetadataStore.class);
    when(datasetMetadataStore.listSync())
        .thenReturn(
            List.of(datasetMetadata("bar"), datasetMetadata("foo"), datasetMetadata("foo_logs")));
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher, DEFAULT_CLUSTER_NAME, DEFAULT_HOST, DEFAULT_PORT, datasetMetadataStore);

    AggregatedHttpResponse aggregatedRes = serviceUnderTest.resolveIndex("foo*").aggregate().join();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("indices")).isEmpty();
    assertThat(jsonNode.get("aliases")).isEmpty();
    assertThat(jsonNode.get("data_streams")).isEmpty();
  }

  @Test
  public void testResolveIndexReturnsEmptyWhenNoDatasetMatches() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    DatasetMetadataStore datasetMetadataStore = mock(DatasetMetadataStore.class);
    when(datasetMetadataStore.listSync()).thenReturn(List.of(datasetMetadata("foo")));
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher, DEFAULT_CLUSTER_NAME, DEFAULT_HOST, DEFAULT_PORT, datasetMetadataStore);

    AggregatedHttpResponse aggregatedRes = serviceUnderTest.resolveIndex("bar").aggregate().join();
    JsonNode jsonNode = OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    assertThat(jsonNode.get("indices")).isEmpty();
    assertThat(jsonNode.get("aliases")).isEmpty();
    assertThat(jsonNode.get("data_streams")).isEmpty();
  }

  @Test
  public void testRootMappingCompatibilityEndpointIsAvailable() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    when(searcher.getSchema(any())).thenReturn(AstraSearch.SchemaResult.newBuilder().build());
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));

    try (CompatibilityServer compatibilityServer = new CompatibilityServer(serviceUnderTest)) {
      AggregatedHttpResponse aggregatedRes =
          compatibilityServer.client().get("/_mapping").aggregate().join();

      assertThat(aggregatedRes.status().code()).isEqualTo(200);
    }
  }

  @Test
  public void testRootAliasCompatibilityEndpointsAreAvailable() throws Exception {
    AstraQueryServiceBase searcher = mock(AstraQueryServiceBase.class);
    ElasticsearchApiService serviceUnderTest =
        new ElasticsearchApiService(
            searcher,
            DEFAULT_CLUSTER_NAME,
            DEFAULT_HOST,
            DEFAULT_PORT,
            mock(DatasetMetadataStore.class));

    try (CompatibilityServer compatibilityServer = new CompatibilityServer(serviceUnderTest)) {
      AggregatedHttpResponse aliasRes =
          compatibilityServer.client().get("/_alias").aggregate().join();
      AggregatedHttpResponse namedAliasRes =
          compatibilityServer.client().get("/_alias/test-alias").aggregate().join();

      assertThat(aliasRes.status().code()).isEqualTo(200);
      assertThat(namedAliasRes.status().code()).isEqualTo(200);
    }
  }

  private void addMessagesToChunkManager(List<Trace.Span> messages) throws IOException {
    IndexingChunkManager<LogMessage> chunkManager = chunkManagerUtil.chunkManager;
    int offset = 1;
    for (Trace.Span m : messages) {
      chunkManager.addMessage(m, m.toString().length(), TEST_KAFKA_PARTITION_ID, offset);
      offset++;
    }
    chunkManager.getActiveChunk().commit();
  }

  private static String readResource(String resourcePath) throws IOException {
    return Resources.toString(Resources.getResource(resourcePath), StandardCharsets.UTF_8);
  }

  private JsonNode searchJson(String postBody) throws Exception {
    HttpResponse response = elasticsearchApiService.search(TEST_DATASET_NAME, postBody);
    AggregatedHttpResponse aggregatedRes = response.aggregate().join();

    assertThat(aggregatedRes.status().code()).isEqualTo(200);
    return OBJECT_MAPPER.readTree(aggregatedRes.content(StandardCharsets.UTF_8));
  }

  private List<String> sourceValues(JsonNode searchResponse, String field) {
    return searchResponse.findValue("hits").get("hits").findValuesAsText(field);
  }

  private static DatasetMetadata datasetMetadata(String name) {
    return new DatasetMetadata(name, "test-owner", 0, List.of(), name);
  }

  private Trace.Span makeWindowSpan(
      int id,
      Instant timestamp,
      int counterId,
      int windowClientWidth,
      int windowClientHeight,
      boolean dontCountHits,
      boolean isRefresh) {
    return makeWindowSpan(
        id,
        timestamp,
        counterId,
        windowClientWidth,
        windowClientHeight,
        dontCountHits,
        isRefresh,
        "search-phrase-" + id);
  }

  private Trace.Span makeWindowSpan(
      int id,
      Instant timestamp,
      int counterId,
      int windowClientWidth,
      int windowClientHeight,
      boolean dontCountHits,
      boolean isRefresh,
      String searchPhrase) {
    return SpanUtil.makeSpan(
        id,
        "window-message-" + id,
        timestamp,
        List.of(
            Trace.KeyValue.newBuilder()
                .setKey("CounterID")
                .setFieldType(Schema.SchemaFieldType.INTEGER)
                .setVInt32(counterId)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("WindowClientWidth")
                .setFieldType(Schema.SchemaFieldType.INTEGER)
                .setVInt32(windowClientWidth)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("WindowClientHeight")
                .setFieldType(Schema.SchemaFieldType.INTEGER)
                .setVInt32(windowClientHeight)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("DontCountHits")
                .setFieldType(Schema.SchemaFieldType.BOOLEAN)
                .setVBool(dontCountHits)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("IsRefresh")
                .setFieldType(Schema.SchemaFieldType.BOOLEAN)
                .setVBool(isRefresh)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("SearchPhrase")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(searchPhrase)
                .build()));
  }

  private Trace.Span makeClickBenchHitSpan(
      int id, Instant timestamp, String url, String searchPhrase) {
    return SpanUtil.makeSpan(
        id,
        "clickbench-message-" + id,
        timestamp,
        List.of(
            Trace.KeyValue.newBuilder()
                .setKey("URL")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(url)
                .build(),
            Trace.KeyValue.newBuilder()
                .setKey("SearchPhrase")
                .setFieldType(Schema.SchemaFieldType.KEYWORD)
                .setVStr(searchPhrase)
                .build()));
  }

  private static final class CompatibilityServer implements AutoCloseable {
    private final Server server;
    private final WebClient client;

    private CompatibilityServer(ElasticsearchApiService serviceUnderTest) {
      server = Server.builder().http(0).annotatedService(serviceUnderTest).build();
      server.start().join();
      client = WebClient.of("http://127.0.0.1:" + server.activeLocalPort());
    }

    private WebClient client() {
      return client;
    }

    @Override
    public void close() {
      server.stop().join();
    }
  }
}
