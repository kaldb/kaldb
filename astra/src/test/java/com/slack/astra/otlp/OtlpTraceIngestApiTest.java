package com.slack.astra.otlp;

import static com.linecorp.armeria.common.HttpStatus.BAD_REQUEST;
import static com.linecorp.armeria.common.HttpStatus.OK;
import static com.linecorp.armeria.common.HttpStatus.TOO_MANY_REQUESTS;
import static com.linecorp.armeria.common.HttpStatus.UNSUPPORTED_MEDIA_TYPE;
import static com.slack.astra.metadata.dataset.DatasetMetadata.MATCH_ALL_SERVICE;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.slack.astra.bulkIngestApi.BulkIngestKafkaProducer;
import com.slack.astra.bulkIngestApi.BulkIngestResponse;
import com.slack.astra.bulkIngestApi.DatasetRateLimitingService;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.preprocessor.PreprocessorMetadataStore;
import com.slack.astra.preprocessor.PreprocessorRateLimiter;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.testlib.TestKafkaServer;
import com.slack.astra.util.JsonUtil;
import com.slack.astra.util.TestingZKServer;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OtlpTraceIngestApiTest {
  private static final String TRACE_DATASET = "otel_traces";
  private static final String DOWNSTREAM_TOPIC = "otlp-traces-test-topic";
  private static final byte[] TRACE_ID =
      new byte[] {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
      };
  private static final byte[] SPAN_ID = new byte[] {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};

  private static ExportTraceServiceRequest traceRequest(String serviceName, String spanName) {
    return ExportTraceServiceRequest.newBuilder()
        .addResourceSpans(
            ResourceSpans.newBuilder()
                .setResource(
                    Resource.newBuilder()
                        .addAttributes(stringAttribute("service.name", serviceName)))
                .addScopeSpans(ScopeSpans.newBuilder().addSpans(span(spanName))))
        .build();
  }

  private static Span span(String spanName) {
    return Span.newBuilder()
        .setTraceId(ByteString.copyFrom(TRACE_ID))
        .setSpanId(ByteString.copyFrom(SPAN_ID))
        .setName(spanName)
        .setStartTimeUnixNano(1_700_000_000_123_456_789L)
        .setEndTimeUnixNano(1_700_000_000_223_456_789L)
        .addAttributes(stringAttribute("http.method", "GET"))
        .build();
  }

  private static KeyValue stringAttribute(String key, String value) {
    return KeyValue.newBuilder()
        .setKey(key)
        .setValue(AnyValue.newBuilder().setStringValue(value))
        .build();
  }

  private static Trace.Span parseSpan(byte[] data) throws InvalidProtocolBufferException {
    return Trace.Span.parseFrom(data);
  }

  private static Trace.KeyValue tag(Trace.Span span, String key) {
    return span.getTagsList().stream()
        .filter(keyValue -> keyValue.getKey().equals(key))
        .findFirst()
        .orElseThrow();
  }

  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.PreprocessorConfig preprocessorConfig;
  private DatasetMetadataStore datasetMetadataStore;
  private PreprocessorMetadataStore preprocessorMetadataStore;
  private TestingServer zkServer;
  private TestKafkaServer kafkaServer;
  private DatasetRateLimitingService datasetRateLimitingService;
  private BulkIngestKafkaProducer bulkIngestKafkaProducer;
  private OtlpTraceIngestApi otlpTraceIngestApi;

  @BeforeEach
  void bootstrapCluster() throws Exception {
    System.setProperty("astra.bulkIngest.useKafkaTransactions", "false");
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();

    zkServer = TestingZKServer.createTestingServer();
    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("testZK")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    kafkaServer = new TestKafkaServer();
    kafkaServer.createTopicWithPartitions(DOWNSTREAM_TOPIC, 1);

    AstraConfigs.KafkaConfig kafkaConfig =
        AstraConfigs.KafkaConfig.newBuilder()
            .setKafkaBootStrapServers(kafkaServer.getBroker().getBrokerList().get())
            .setKafkaTopic(DOWNSTREAM_TOPIC)
            .build();
    preprocessorConfig =
        AstraConfigs.PreprocessorConfig.newBuilder()
            .setKafkaConfig(kafkaConfig)
            .setServerConfig(
                AstraConfigs.ServerConfig.newBuilder()
                    .setServerPort(8080)
                    .setServerAddress("localhost"))
            .setPreprocessorInstanceCount(1)
            .setRateLimiterMaxBurstSeconds(1)
            .setDatasetRateLimitAggregationSecs(1)
            .setDatasetRateLimitPeriodSecs(15)
            .setOtlpTraceDatasetName(TRACE_DATASET)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();
    datasetMetadataStore =
        new DatasetMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    datasetMetadataStore.createSync(datasetMetadata(1_000_000));

    preprocessorMetadataStore =
        new PreprocessorMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
    datasetRateLimitingService =
        new DatasetRateLimitingService(
            datasetMetadataStore, preprocessorMetadataStore, preprocessorConfig, meterRegistry);
    datasetRateLimitingService.startAsync();
    datasetRateLimitingService.awaitRunning(DEFAULT_START_STOP_DURATION);

    bulkIngestKafkaProducer =
        new BulkIngestKafkaProducer(datasetMetadataStore, preprocessorConfig, meterRegistry);
    bulkIngestKafkaProducer.startAsync();
    bulkIngestKafkaProducer.awaitRunning(DEFAULT_START_STOP_DURATION);

    otlpTraceIngestApi =
        new OtlpTraceIngestApi(
            bulkIngestKafkaProducer,
            datasetRateLimitingService,
            meterRegistry,
            TOO_MANY_REQUESTS.code(),
            preprocessorConfig.getOtlpTraceDatasetName(),
            Schema.IngestSchema.getDefaultInstance());
  }

  @AfterEach
  void shutdownCluster() throws Exception {
    System.clearProperty("astra.bulkIngest.useKafkaTransactions");
    if (datasetRateLimitingService != null) {
      datasetRateLimitingService.stopAsync();
      datasetRateLimitingService.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (bulkIngestKafkaProducer != null) {
      bulkIngestKafkaProducer.stopAsync();
      bulkIngestKafkaProducer.awaitTerminated(DEFAULT_START_STOP_DURATION);
    }
    if (kafkaServer != null) {
      kafkaServer.close();
    }
    if (datasetMetadataStore != null) {
      datasetMetadataStore.close();
    }
    if (preprocessorMetadataStore != null) {
      preprocessorMetadataStore.close();
    }
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }
    if (zkServer != null) {
      zkServer.close();
    }
    if (meterRegistry != null) {
      meterRegistry.close();
    }
  }

  @Test
  void acceptsProtobufAndJsonTraceRequests() throws Exception {
    KafkaConsumer<String, byte[]> kafkaConsumer = getTestKafkaConsumer();
    ExportTraceServiceRequest protobufRequest = traceRequest("frontend", "GET /api");
    ExportTraceServiceRequest jsonRequest = traceRequest("checkout", "POST /checkout");

    AggregatedHttpResponse protobufResponse =
        ingest(MediaType.X_PROTOBUF, protobufRequest.toByteArray());
    AggregatedHttpResponse jsonResponse =
        ingest(
            MediaType.JSON,
            JsonFormat.printer().print(jsonRequest).getBytes(StandardCharsets.UTF_8));

    assertSuccessfulOtlpResponse(protobufResponse, MediaType.X_PROTOBUF);
    assertSuccessfulOtlpResponse(jsonResponse, MediaType.JSON);
    validateOffset(kafkaConsumer, 2);

    ConsumerRecords<String, byte[]> records =
        kafkaConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));

    assertThat(records.count()).isEqualTo(2);
    assertThat(records).allSatisfy(record -> assertThat(record.key()).isEqualTo(TRACE_DATASET));
    assertThat(records)
        .extracting(record -> parseSpan(record.value()).getName())
        .containsExactlyInAnyOrder("GET /api", "POST /checkout");
    assertThat(records)
        .allSatisfy(
            record ->
                assertThat(
                        tag(
                                parseSpan(record.value()),
                                LogMessage.ReservedField.SERVICE_NAME.fieldName)
                            .getVStr())
                    .isEqualTo(TRACE_DATASET));
    assertThat(records)
        .extracting(record -> tag(parseSpan(record.value()), "resource.service.name").getVStr())
        .containsExactlyInAnyOrder("frontend", "checkout");

    kafkaConsumer.close();
  }

  @Test
  void rejectsMalformedTraceRequest() throws Exception {
    AggregatedHttpResponse response =
        ingest(MediaType.X_PROTOBUF, "not protobuf".getBytes(StandardCharsets.UTF_8));

    assertResponse(response, BAD_REQUEST.code(), 0, 0, "Unable to parse OTLP trace request");
  }

  @Test
  void rejectsUnsupportedContentType() throws Exception {
    AggregatedHttpResponse response =
        ingest(MediaType.PLAIN_TEXT_UTF_8, "{}".getBytes(StandardCharsets.UTF_8));

    assertResponse(response, UNSUPPORTED_MEDIA_TYPE.code(), 0, 0, "unsupported content type");
  }

  @Test
  void returnsConfiguredStatusWhenRateLimited() throws Exception {
    byte[] request = traceRequest("frontend", "GET /api").toByteArray();
    int spanBytes =
        PreprocessorRateLimiter.getSpanBytes(
            OtlpTraceRequestParser.parseProtobuf(
                    request, TRACE_DATASET, Schema.IngestSchema.getDefaultInstance())
                .get(TRACE_DATASET));
    updateDatasetThroughput(spanBytes / 2);

    AggregatedHttpResponse firstResponse = ingest(MediaType.X_PROTOBUF, request);
    AggregatedHttpResponse secondResponse = ingest(MediaType.X_PROTOBUF, request);

    assertSuccessfulOtlpResponse(firstResponse, MediaType.X_PROTOBUF);
    assertResponse(secondResponse, TOO_MANY_REQUESTS.code(), 0, 0, "rate limit exceeded");
  }

  private AggregatedHttpResponse ingest(MediaType contentType, byte[] body) {
    return otlpTraceIngestApi
        .ingestTraces(AggregatedHttpRequest.of(HttpMethod.POST, "/v1/traces", contentType, body))
        .aggregate()
        .join();
  }

  private void assertResponse(
      AggregatedHttpResponse response,
      int statusCode,
      int totalDocs,
      long failedDocs,
      String errorMessage)
      throws Exception {
    assertThat(response.status().code()).isEqualTo(statusCode);
    BulkIngestResponse responseBody =
        JsonUtil.read(response.contentUtf8(), BulkIngestResponse.class);
    assertThat(responseBody.totalDocs()).isEqualTo(totalDocs);
    assertThat(responseBody.failedDocs()).isEqualTo(failedDocs);
    assertThat(responseBody.errorMsg()).contains(errorMessage);
  }

  private void assertSuccessfulOtlpResponse(
      AggregatedHttpResponse response, MediaType expectedContentType) throws Exception {
    assertThat(response.status().code()).isEqualTo(OK.code());
    assertThat(response.headers().contentType()).isEqualTo(expectedContentType);
    if (expectedContentType.isJson()) {
      ExportTraceServiceResponse.Builder builder = ExportTraceServiceResponse.newBuilder();
      JsonFormat.parser().merge(response.contentUtf8(), builder);
      assertThat(builder.build()).isEqualTo(ExportTraceServiceResponse.getDefaultInstance());
    } else {
      assertThat(ExportTraceServiceResponse.parseFrom(response.content().array()))
          .isEqualTo(ExportTraceServiceResponse.getDefaultInstance());
    }
  }

  private void updateDatasetThroughput(int throughputBytes) {
    double timerCount =
        MetricsUtil.getTimerCount(
            DatasetRateLimitingService.RATE_LIMIT_RELOAD_TIMER, meterRegistry);

    datasetMetadataStore.updateSync(datasetMetadata(throughputBytes));

    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                        DatasetRateLimitingService.RATE_LIMIT_RELOAD_TIMER, meterRegistry)
                    > timerCount);
  }

  private DatasetMetadata datasetMetadata(int throughputBytes) {
    return new DatasetMetadata(
        TRACE_DATASET,
        "owner",
        throughputBytes,
        List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("0"))),
        MATCH_ALL_SERVICE);
  }

  private KafkaConsumer<String, byte[]> getTestKafkaConsumer() {
    Properties properties = kafkaServer.getBroker().consumerConfig();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
    properties.put("isolation.level", "read_committed");
    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(properties);
    kafkaConsumer.subscribe(List.of(DOWNSTREAM_TOPIC));
    return kafkaConsumer;
  }

  private void validateOffset(KafkaConsumer<String, byte[]> kafkaConsumer, long expectedOffset) {
    await()
        .until(
            () ->
                kafkaConsumer
                        .endOffsets(List.of(new TopicPartition(DOWNSTREAM_TOPIC, 0)))
                        .values()
                        .stream()
                        .findFirst()
                        .orElseThrow()
                    == expectedOffset);
  }
}
