package com.slack.astra.logstore.search;

import static com.slack.astra.logstore.LuceneIndexStoreImpl.COMMITS_TIMER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_FAILED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.MESSAGES_RECEIVED_COUNTER;
import static com.slack.astra.logstore.LuceneIndexStoreImpl.REFRESHES_TIMER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.testlib.MessageUtil.TEST_DATASET_NAME;
import static com.slack.astra.testlib.MessageUtil.TEST_SOURCE_LONG_PROPERTY;
import static com.slack.astra.testlib.MessageUtil.TEST_SOURCE_STRING_PROPERTY;
import static com.slack.astra.testlib.MetricsUtil.getCount;
import static com.slack.astra.testlib.MetricsUtil.getTimerCount;
import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.MAX_TIME;
import static com.slack.astra.util.AggregatorFactoriesUtil.createAverageAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createExtendedStatsAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createFiltersAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createMaxAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createMinAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createSumAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createTermsAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.google.protobuf.ByteString;
import com.slack.astra.clusterManager.RedactionUpdateService;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.schema.Schema;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.astra.writer.SpanFormatter;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;

public class LogIndexSearcherImplTest {

  private static ByteString encodedIdSortValue(String id) {
    BytesRef encodedId = Uid.encodeId(id);
    return ByteString.copyFrom(encodedId.bytes, encodedId.offset, encodedId.length);
  }

  private static ByteString encodedIpSortValue(String ip) {
    return ByteString.copyFrom(InetAddressPoint.encode(InetAddresses.forString(ip)));
  }

  private static SearchResult<LogMessage> search(
      LogIndexSearcherImpl searcher,
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    return searcher.search(
        new SearchQuery(
            dataset,
            0L,
            MAX_TIME,
            howMany,
            0,
            List.of(),
            queryBuilder,
            sourceFieldFilter,
            aggregatorFactoriesBuilder));
  }

  private static SearchResult<LogMessage> search(
      LogIndexSearcherImpl searcher,
      String dataset,
      int howMany,
      List<SearchQuery.SortFieldSpec> requestedSortFieldSpecs,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    return searcher.search(
        new SearchQuery(
            dataset,
            0L,
            MAX_TIME,
            howMany,
            0,
            requestedSortFieldSpecs,
            List.of(),
            queryBuilder,
            sourceFieldFilter,
            aggregatorFactoriesBuilder));
  }

  private static List<HitSortValue> sortValues(Object... values) {
    return Arrays.stream(values).map(HitSortValue::of).toList();
  }

  @Nested
  public class RedactionTests {
    private FieldRedactionMetadataStore fieldRedactionMetadataStore;
    private TestingServer testingServer;
    private MeterRegistry meterRegistry;
    private AsyncCuratorFramework curatorFramework;
    private AstraConfigs.RedactionUpdateServiceConfig redactionUpdateServiceConfig;
    private RedactionUpdateService redactionUpdateService;

    @BeforeEach
    public void setup() throws Exception {
      // setup ZK and redaction metadata store for field redaction testing

      testingServer = new TestingServer();
      meterRegistry = new SimpleMeterRegistry();
      AstraConfigs.MetadataStoreConfig metadataStoreConfig =
          AstraConfigs.MetadataStoreConfig.newBuilder()
              .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
              .setZookeeperConfig(
                  AstraConfigs.ZookeeperConfig.newBuilder()
                      .setZkConnectString(testingServer.getConnectString())
                      .setZkPathPrefix("test")
                      .setZkSessionTimeoutMs(Integer.MAX_VALUE)
                      .setZkConnectionTimeoutMs(Integer.MAX_VALUE)
                      .setSleepBetweenRetriesMs(1000)
                      .setZkCacheInitTimeoutMs(1000)
                      .build())
              .build();
      curatorFramework =
          CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
      fieldRedactionMetadataStore =
          new FieldRedactionMetadataStore(
              curatorFramework, metadataStoreConfig, meterRegistry, true);

      redactionUpdateServiceConfig =
          AstraConfigs.RedactionUpdateServiceConfig.newBuilder()
              .setRedactionUpdatePeriodSecs(1)
              .setRedactionUpdateInitDelaySecs(1)
              .build();
      redactionUpdateService =
          new RedactionUpdateService(fieldRedactionMetadataStore, redactionUpdateServiceConfig);
      redactionUpdateService.startAsync();
      redactionUpdateService.awaitRunning(DEFAULT_START_STOP_DURATION);
    }

    @AfterEach
    public void teardown() throws Exception {
      testingServer.close();
      fieldRedactionMetadataStore.close();
      curatorFramework.unwrap().close();
      meterRegistry.close();
      if (redactionUpdateService != null) {
        redactionUpdateService.stopAsync();
        redactionUpdateService.awaitTerminated(DEFAULT_START_STOP_DURATION);
      }
    }

    @Test
    public void testRedactionWithIncludeFilters() throws Exception {
      String redactionName = "testRedaction";
      String fieldName = "message";
      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .putIncludeFields("message", true)
              .build();

      // add redaction between log being added and searched to test that the redaction map gets
      // updated
      // a previous change passed this test when the redaction was added before the
      // DirectoryReader was created and redaction still did not work
      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(redactionName, fieldName, start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);

      // redaction service is currently set to update every <redaction_update_period_secs>
      // setRedactionUpdatePeriodSecs is set to 1 second in setup()
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      List<LogMessage> messages =
          search(
                  featureFlagEnabledStrictLogStore.logSearcher,
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .messages();
      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource()).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message")).isEqualTo("REDACTED");
      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionOutOfTimerange() throws Exception {
      String redactionName = "testRedaction";
      String fieldName = "message";
      long start = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(redactionName, fieldName, start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .putIncludeFields("message", true)
              .build();

      List<LogMessage> messages =
          search(
                  featureFlagEnabledStrictLogStore.logSearcher,
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .messages();
      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource()).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message"))
          .isEqualTo("The identifier in this message is Message1");
      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionInAndOutOfTimerange() throws Exception {
      String redactionName = "testRedaction";
      String fieldName = "message";
      long start = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(redactionName, fieldName, start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.minus(1, ChronoUnit.DAYS)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .putIncludeFields("message", true)
              .build();

      List<LogMessage> messages =
          search(
                  featureFlagEnabledStrictLogStore.logSearcher,
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "*",
                      time.minus(2, ChronoUnit.DAYS).toEpochMilli(),
                      time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .messages();
      assertThat(messages).hasSize(2);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message"))
          .isEqualTo("The identifier in this message is Message1");
      assertThat(messages.get(1).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(1).getSource().get("message")).isEqualTo("REDACTED");

      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionWithMultipleFieldsRedacted() throws Exception {

      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction1", "message", start, end));
      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction2", "binaryproperty", start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 2);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      // search
      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setIncludeAll(true).build();

      List<LogMessage> messages =
          search(
                  featureFlagEnabledStrictLogStore.logSearcher,
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .messages();

      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message")).isEqualTo("REDACTED");
      assertThat(messages.get(0).getSource().containsKey("binaryproperty")).isTrue();
      assertThat(messages.get(0).getSource().get("binaryproperty")).isEqualTo("REDACTED");

      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionWithIncludeFiltersWithWildcard() throws Exception {

      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction1", "message", start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      Instant time = Instant.now();
      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(2, time.plusSeconds(100)));
      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      assertThat(
              getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(2);
      assertThat(
              getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(0);
      assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
          .isEqualTo(1);

      AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
          AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
              .addIncludeWildcards("messag.*")
              .build();

      List<LogMessage> messages =
          search(
                  featureFlagEnabledStrictLogStore.logSearcher,
                  TEST_DATASET_NAME,
                  1000,
                  QueryBuilderUtil.generateQueryBuilder(
                      "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                  SourceFieldFilter.fromProto(sourceFieldFilter),
                  createGenericDateHistogramAggregatorFactoriesBuilder())
              .messages();
      assertThat(messages).hasSize(1);
      assertThat(messages.get(0).getSource()).hasSize(1);
      assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
      assertThat(messages.get(0).getSource().get("message")).isEqualTo("REDACTED");

      featureFlagEnabledStrictLogStore.closeAll();
    }

    @Test
    public void testRedactionWithFilterAggregations() throws Exception {
      Instant time = Instant.now();
      long start = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
      long end = Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli();

      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata("testRedaction1", "message", start, end));

      await()
          .until(
              () ->
                  AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size() == 1);
      Thread.sleep(redactionUpdateServiceConfig.getRedactionUpdatePeriodSecs() * 1000);

      TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
          new TemporaryLogStoreAndSearcherExtension(true);

      featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(3, "apple baby", time.plusSeconds(2)));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(4, "car", time.plusSeconds(3)));
      featureFlagEnabledStrictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(5, "apple baby car", time.plusSeconds(4)));

      featureFlagEnabledStrictLogStore.logStore.commit();
      featureFlagEnabledStrictLogStore.logStore.refresh();

      SearchResult<LogMessage> scriptNull =
          search(
              featureFlagEnabledStrictLogStore.logSearcher,
              TEST_DATASET_NAME,
              1000,
              QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
              null,
              createFiltersAggregatorFactoriesBuilder(
                  "1",
                  List.of(),
                  Map.of(
                      "foo",
                      QueryBuilderUtil.generateQueryBuilder(
                          String.format(
                              "%s:<=%s",
                              LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                              time.plusSeconds(2).toEpochMilli()),
                          time.toEpochMilli(),
                          time.plusSeconds(2).toEpochMilli()),
                      "bar",
                      QueryBuilderUtil.generateQueryBuilder(
                          String.format(
                              "%s:>%s",
                              LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                              time.plusSeconds(2).toEpochMilli()),
                          time.plusSeconds(2).toEpochMilli(),
                          time.plusSeconds(10).toEpochMilli()))));

      InternalFilters filters = (InternalFilters) scriptNull.internalAggregations.get("1");
      assertThat(filters.getBuckets().size()).isEqualTo(2);
      assertThat(filters.getBuckets().get(0).getDocCount()).isEqualTo(2);
      assertThat(filters.getBuckets().get(0).getKey()).isIn(List.of("foo", "bar"));
      assertThat(filters.getBuckets().get(1).getDocCount()).isEqualTo(2);
      assertThat(filters.getBuckets().get(1).getKey()).isIn(List.of("foo", "bar"));
      assertThat(filters.getBuckets().get(0).getKey())
          .isNotEqualTo(filters.getBuckets().get(1).getKey());

      featureFlagEnabledStrictLogStore.closeAll();
    }
  }

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStore =
      new TemporaryLogStoreAndSearcherExtension(true);

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension strictLogStoreWithoutFts =
      new TemporaryLogStoreAndSearcherExtension(false);

  public LogIndexSearcherImplTest() throws IOException {}

  @BeforeAll
  public static void beforeClass() {
    Tracing.newBuilder().build();
  }

  private void loadTestData(Instant time) {
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "apple baby", time.plusSeconds(2)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, "car", time.plusSeconds(3)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(5, "apple baby car", time.plusSeconds(4)));
    // when we enable multi-tenancy, we can add messages to different indices

    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
  }

  @Test
  public void testSearchWithIncludeFilters() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .putIncludeFields("message", true)
            .build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource()).hasSize(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
    assertThat(messages.get(0).getSource().get("message"))
        .isEqualTo("The identifier in this message is Message1");
  }

  @Test
  public void testSearchWithIncludeFiltersWithWildcardAfter() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addIncludeWildcards("messag.*")
            .build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource()).hasSize(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
    assertThat(messages.get(0).getSource().get("message"))
        .isEqualTo("The identifier in this message is Message1");
  }

  @Test
  public void testSearchWithIncludeFiltersWithWildcardBefore() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addIncludeWildcards(".*ssage")
            .build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource()).hasSize(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isTrue();
    assertThat(messages.get(0).getSource().get("message"))
        .isEqualTo("The identifier in this message is Message1");
  }

  @Test
  public void testSearchWithIncludeFilterAllTrue() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setIncludeAll(true).build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
  }

  @Test
  public void testSearchWithIncludeFilterAllFalse() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setIncludeAll(false).build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isEqualTo(0);
  }

  @Test
  public void testSearchWithExcludeFilters() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .putExcludeFields("message", true)
            .build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isFalse();
  }

  @Test
  public void testSearchWithExcludeFiltersWithWildcardAfter() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addExcludeWildcards(".*ssage")
            .build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isFalse();
  }

  @Test
  public void testSearchWithExcludeFiltersWithWildcardBefore() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder()
            .addExcludeWildcards(".*ssage")
            .build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(1);
    assertThat(messages.get(0).getSource().containsKey("message")).isFalse();
  }

  @Test
  public void testSearchWithExcludeFilterAllTrue() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setExcludeAll(true).build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isEqualTo(0);
  }

  @Test
  public void testSearchWithExcludeFilterAllFalse() throws IOException {
    TemporaryLogStoreAndSearcherExtension featureFlagEnabledStrictLogStore =
        new TemporaryLogStoreAndSearcherExtension(true);

    Instant time = Instant.now();
    featureFlagEnabledStrictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    featureFlagEnabledStrictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, time.plusSeconds(100)));
    featureFlagEnabledStrictLogStore.logStore.commit();
    featureFlagEnabledStrictLogStore.logStore.refresh();

    assertThat(
            getCount(MESSAGES_RECEIVED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, featureFlagEnabledStrictLogStore.metricsRegistry))
        .isEqualTo(1);

    AstraSearch.SearchRequest.SourceFieldFilter sourceFieldFilter =
        AstraSearch.SearchRequest.SourceFieldFilter.newBuilder().setExcludeAll(false).build();

    List<LogMessage> messages =
        search(
                featureFlagEnabledStrictLogStore.logSearcher,
                TEST_DATASET_NAME,
                1000,
                QueryBuilderUtil.generateQueryBuilder(
                    "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                SourceFieldFilter.fromProto(sourceFieldFilter),
                createGenericDateHistogramAggregatorFactoriesBuilder())
            .messages();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getSource().size()).isGreaterThan(0);
  }

  @Test
  public void testTimeBoundSearch() throws IOException {
    Instant time = Instant.now();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time.plusSeconds(100)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Start inclusive.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "Message1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    // Extended range still only picking one element.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "Message1",
                        time.minusSeconds(1).toEpochMilli(),
                        time.plusSeconds(90).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    // Both ranges are inclusive.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "_id:Message1 OR Message2",
                        time.toEpochMilli(),
                        time.plusSeconds(100).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    // Extended range to pick up both events
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder(
                        "_id:Message1 OR Message2",
                        time.minusSeconds(1).toEpochMilli(),
                        time.plusSeconds(100).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
  }

  @Test
  @Disabled // todo - re-enable when multi-tenancy is supported - slackhq/astra/issues/223
  public void testIndexBoundSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, time));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    assertThat(
            search(
                    strictLogStore.logSearcher,
                    "idx",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test1",
                        time.minusSeconds(1).toEpochMilli(),
                        time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            search(
                    strictLogStore.logSearcher,
                    "idx1",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    assertThat(
            search(
                    strictLogStore.logSearcher,
                    "idx12",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test1", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);

    assertThat(
            search(
                    strictLogStore.logSearcher,
                    "idx1",
                    100,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testSearchMultipleItemsAndIndices() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> babies =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "Message1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(babies.hits.size()).isEqualTo(1);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(babies.internalAggregations.get("1"));
    assertThat(histogram.getBuckets().size()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAllQueryWithFullTextSearchEnabled() throws IOException {
    Instant time = Instant.now();

    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> termQuery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "customField:value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermStrQuery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(noTermStrQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> noTermNumericQuery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "Message1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(noTermNumericQuery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testAllQueryWithFullTextSearchDisabled() throws IOException {
    Instant time = Instant.now();
    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    SearchResult<LogMessage> termQuery =
        search(
            strictLogStoreWithoutFts.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "customField:value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(termQuery.hits.size()).isEqualTo(1);
  }

  @Test
  public void testExistsQuery() throws IOException {
    Instant time = Instant.now();
    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    Trace.KeyValue customField1 =
        Trace.KeyValue.newBuilder()
            .setVStr("value")
            .setKey("customField1")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "apple", time, List.of(customField1)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> exists =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "_exists_:customField", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(exists.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> termQuery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "customField:value", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(termQuery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> notExists =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "_exists_:foo", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(notExists.hits.size()).isEqualTo(0);
  }

  @Test
  public void testRangeQuery() throws IOException {
    Instant time = Instant.now();

    Trace.KeyValue valTag1 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(1)
            .build();
    Trace.KeyValue valTag2 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(2)
            .build();
    Trace.KeyValue valTag3 =
        Trace.KeyValue.newBuilder()
            .setKey("val")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .setVInt32(3)
            .build();
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(valTag1)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "bear", time, List.of(valTag2)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "car", time, List.of(valTag3)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> rangeBoundInclusive =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "val:[1 TO 3]", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(rangeBoundInclusive.hits.size()).isEqualTo(3);

    SearchResult<LogMessage> rangeBoundExclusive =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "val:{1 TO 3}", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(rangeBoundExclusive.hits.size()).isEqualTo(1);
  }

  @Test
  public void testQueryParsingFieldTypes() throws IOException {
    Instant time = Instant.now();

    Trace.KeyValue boolTag =
        Trace.KeyValue.newBuilder()
            .setVBool(true)
            .setKey("boolval")
            .setFieldType(Schema.SchemaFieldType.BOOLEAN)
            .build();

    Trace.KeyValue intTag =
        Trace.KeyValue.newBuilder()
            .setVInt32(1)
            .setKey("intval")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    Trace.KeyValue longTag =
        Trace.KeyValue.newBuilder()
            .setVInt64(2L)
            .setKey("longval")
            .setFieldType(Schema.SchemaFieldType.LONG)
            .build();

    Trace.KeyValue floatTag =
        Trace.KeyValue.newBuilder()
            .setVFloat32(3F)
            .setKey("floatval")
            .setFieldType(Schema.SchemaFieldType.FLOAT)
            .build();

    Trace.KeyValue doubleTag =
        Trace.KeyValue.newBuilder()
            .setVFloat64(4D)
            .setKey("doubleval")
            .setFieldType(Schema.SchemaFieldType.DOUBLE)
            .build();

    Trace.Span span =
        SpanUtil.makeSpan(1, "apple", time, List.of(boolTag, intTag, longTag, floatTag, doubleTag));
    strictLogStore.logStore.addMessage(span);
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> boolquery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "boolval:true", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(boolquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> intquery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "intval:1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(intquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> longquery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "longval:2", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(longquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> floatquery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "floatval:3", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(floatquery.hits.size()).isEqualTo(1);

    SearchResult<LogMessage> doublequery =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "doubleval:4", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(doublequery.hits.size()).isEqualTo(1);
  }

  /** Verifies descending numeric sort places missing numeric values after present values. */
  @Test
  void testDescendingSortPlacesMissingNumericValuesLast() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue rankTen =
        Trace.KeyValue.newBuilder()
            .setVInt32(10)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue rankTwenty =
        Trace.KeyValue.newBuilder()
            .setVInt32(20)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "rank-10", time, List.of(rankTen)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "rank-missing", time.plusSeconds(1)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(3, "rank-20", time.plusSeconds(2), List.of(rankTwenty)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> results =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            3,
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.DESC)),
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            null);

    assertThat(results.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message3", "Message1", "Message2");
    assertThat(results.hits.stream().map(hit -> hit.sortValues().get(0)).toList())
        .containsExactly(
            HitSortValue.intValue(20),
            HitSortValue.intValue(10),
            HitSortValue.intValue(Integer.MIN_VALUE));
  }

  /** Verifies worker string sort values represent missing keyword fields as null values. */
  @Test
  void testStringSortCarriesMissingKeywordValuesAsNullSortValues() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue serviceZeta =
        Trace.KeyValue.newBuilder()
            .setVStr("zeta")
            .setKey("service")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();

    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(1, "service-zeta", time, List.of(serviceZeta)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, "service-missing", time.plusSeconds(1)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> results =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            2,
            List.of(new SearchQuery.SortFieldSpec("service", SearchQuery.SortDirection.DESC)),
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            null);

    assertThat(results.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message1", "Message2");
    assertThat(results.hits.stream().map(hit -> hit.sortValues().get(0)).toList())
        .containsExactly(HitSortValue.stringValue("zeta"), HitSortValue.nullValue());
  }

  /** Verifies IP field sorting carries encoded IP sort values from the worker. */
  @Test
  void testSortingByIpFieldUsesEncodedIpSortValues() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue ip192 =
        Trace.KeyValue.newBuilder()
            .setVStr("192.168.0.1")
            .setKey("client.ip")
            .setFieldType(Schema.SchemaFieldType.IP)
            .build();
    Trace.KeyValue ip10Dot2 =
        Trace.KeyValue.newBuilder()
            .setVStr("10.0.0.2")
            .setKey("client.ip")
            .setFieldType(Schema.SchemaFieldType.IP)
            .build();
    Trace.KeyValue ip10Dot1 =
        Trace.KeyValue.newBuilder()
            .setVStr("10.0.0.1")
            .setKey("client.ip")
            .setFieldType(Schema.SchemaFieldType.IP)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "ip-192", time, List.of(ip192)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, "ip-10-dot-2", time.plusSeconds(1), List.of(ip10Dot2)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(3, "ip-10-dot-1", time.plusSeconds(2), List.of(ip10Dot1)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> results =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            3,
            List.of(new SearchQuery.SortFieldSpec("client.ip", SearchQuery.SortDirection.ASC)),
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            null);

    assertThat(results.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message3", "Message2", "Message1");
    assertThat(results.hits.stream().map(hit -> hit.sortValues().get(0)).toList())
        .containsExactly(
            HitSortValue.ipAddress(encodedIpSortValue("10.0.0.1")),
            HitSortValue.ipAddress(encodedIpSortValue("10.0.0.2")),
            HitSortValue.ipAddress(encodedIpSortValue("192.168.0.1")));
  }

  /** Verifies worker and coordinator tie-breakers produce the same order for equal sort values. */
  @Test
  void testWorkerAndCoordinatorUseSameTieBreakerOrderForEqualSortValues() throws IOException {
    Instant time = Instant.now();
    Trace.KeyValue rank =
        Trace.KeyValue.newBuilder()
            .setVInt32(100)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    List<SearchQuery.SortFieldSpec> sortFieldSpecs =
        List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC));

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "rank-100", time, List.of(rank)));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "rank-100", time, List.of(rank)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(3, "rank-100", time.plusSeconds(1), List.of(rank)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> workerResult =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            3,
            sortFieldSpecs,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            null);

    assertThat(workerResult.hits.stream().map(SearchResultHit::sortValues).toList())
        .containsExactly(
            sortValues(100, time.plusSeconds(1).toEpochMilli(), encodedIdSortValue("Message3")),
            sortValues(100, time.toEpochMilli(), encodedIdSortValue("Message1")),
            sortValues(100, time.toEpochMilli(), encodedIdSortValue("Message2")));
    assertThat(workerResult.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message3", "Message1", "Message2");

    SearchQuery searchQuery =
        new SearchQuery(
            TEST_DATASET_NAME, 0L, MAX_TIME, 3, 0, sortFieldSpecs, List.of(), null, null, null);
    SearchResult<LogMessage> firstPartialResult =
        new SearchResult<>(
            List.of(workerResult.hits.get(0), workerResult.hits.get(2)), 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> secondPartialResult =
        new SearchResult<>(List.of(workerResult.hits.get(1)), 11, 0, 1, 1, 0, null);

    SearchResult<LogMessage> coordinatorResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(firstPartialResult, secondPartialResult), true);

    assertThat(coordinatorResult.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message3", "Message1", "Message2");
  }

  /** Verifies worker sort values survive proto round trips with requested sort fields. */
  @Test
  void testWorkerSortValuesRoundTripWithRequestedSortFields() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue lowRank =
        Trace.KeyValue.newBuilder()
            .setVInt32(10)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue highRank =
        Trace.KeyValue.newBuilder()
            .setVInt32(20)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue lowScore =
        Trace.KeyValue.newBuilder()
            .setVInt32(100)
            .setKey("score")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue highScore =
        Trace.KeyValue.newBuilder()
            .setVInt32(200)
            .setKey("score")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    List<SearchQuery.SortFieldSpec> sortFieldSpecs =
        List.of(
            new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC),
            new SearchQuery.SortFieldSpec("score", SearchQuery.SortDirection.DESC));

    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(1, "rank-10-score-100", time, List.of(lowRank, lowScore)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, "rank-10-score-200", time, List.of(lowRank, highScore)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(3, "rank-20-score-200", time, List.of(highRank, highScore)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> workerResult =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            3,
            sortFieldSpecs,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            null);

    assertThat(workerResult.hits.stream().map(hit -> hit.sortValues().subList(0, 2)).toList())
        .containsExactly(sortValues(10, 200), sortValues(10, 100), sortValues(20, 200));
    assertThat(workerResult.hits)
        .allSatisfy(
            hit -> {
              assertThat(hit.sortValues()).hasSize(4);
              assertThat(hit.sortValues().get(2))
                  .isEqualTo(HitSortValue.of(hit.message().getTimestamp().toEpochMilli()));
              assertThat(hit.sortValues().get(3))
                  .isEqualTo(HitSortValue.bytes(encodedIdSortValue(hit.message().getId())));
            });

    SearchResult<LogMessage> roundTrippedResult =
        SearchResultUtils.fromSearchResultProto(
            SearchResultUtils.toSearchResultProto(workerResult));

    assertThat(roundTrippedResult.hits.stream().map(SearchResultHit::sortValues).toList())
        .containsExactlyElementsOf(
            workerResult.hits.stream().map(SearchResultHit::sortValues).toList());
  }

  /** Verifies requested sort is applied after a query filter chooses matching hits. */
  @Test
  void testSortWithBooleanQueryFiltersMatchingHits() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue rankThree =
        Trace.KeyValue.newBuilder()
            .setVInt32(3)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue rankOne =
        Trace.KeyValue.newBuilder()
            .setVInt32(1)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue rankTwo =
        Trace.KeyValue.newBuilder()
            .setVInt32(2)
            .setKey("rank")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();
    Trace.KeyValue includeGroup =
        Trace.KeyValue.newBuilder()
            .setVStr("include")
            .setKey("group")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();
    Trace.KeyValue excludeGroup =
        Trace.KeyValue.newBuilder()
            .setVStr("exclude")
            .setKey("group")
            .setFieldType(Schema.SchemaFieldType.KEYWORD)
            .build();

    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(1, "include-rank-3", time, List.of(rankThree, includeGroup)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(
            2, "exclude-rank-1", time.plusSeconds(1), List.of(rankOne, excludeGroup)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(
            3, "include-rank-1", time.plusSeconds(2), List.of(rankOne, includeGroup)));
    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(
            4, "include-rank-2", time.plusSeconds(3), List.of(rankTwo, includeGroup)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> results =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC)),
            new TermQueryBuilder("group", "include"),
            null,
            null);

    assertThat(results.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message3", "Message4", "Message1");
    assertThat(results.hits.stream().map(hit -> hit.sortValues().get(0)).toList())
        .containsExactly(
            HitSortValue.intValue(1), HitSortValue.intValue(2), HitSortValue.intValue(3));
  }

  @Test
  public void testTopKQuery() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> apples =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            2,
            QueryBuilderUtil.generateQueryBuilder(
                "apple", time.toEpochMilli(), time.plusSeconds(100).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(
            apples.hits.stream()
                .map(SearchResultHit::message)
                .map(m -> m.getId())
                .collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("Message5", "Message3"));
    assertThat(apples.hits.size()).isEqualTo(2);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(apples.internalAggregations.get("1"));

    assertThat(histogram.getBuckets().size()).isEqualTo(3);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testSearchMultipleCommits() throws IOException {
    Instant time = Instant.now();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time));
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(2)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    SearchResult<LogMessage> baby =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            2,
            QueryBuilderUtil.generateQueryBuilder(
                "baby", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(baby.hits.size()).isEqualTo(1);
    assertThat(baby.hits.get(0).message().getId()).isEqualTo("Message2");
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    // Add car but don't commit. So, no results for car.
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(3, "car", time.plusSeconds(3)));

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);

    search(
        strictLogStore.logSearcher,
        TEST_DATASET_NAME,
        2,
        QueryBuilderUtil.generateQueryBuilder(
            "car", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
        null,
        createGenericDateHistogramAggregatorFactoriesBuilder());

    // Commit but no refresh. Item is still not available for search.
    strictLogStore.logStore.commit();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(1);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    search(
        strictLogStore.logSearcher,
        TEST_DATASET_NAME,
        2,
        QueryBuilderUtil.generateQueryBuilder(
            "car", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
        null,
        createGenericDateHistogramAggregatorFactoriesBuilder());

    // Car can be searched after refresh.
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    search(
        strictLogStore.logSearcher,
        TEST_DATASET_NAME,
        2,
        QueryBuilderUtil.generateQueryBuilder(
            "car", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
        null,
        createGenericDateHistogramAggregatorFactoriesBuilder());

    // Add another message to search, refresh but don't commit.
    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(4, "apple baby car", time.plusSeconds(4)));
    strictLogStore.logStore.refresh();

    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(2);

    // Item shows up in search without commit.
    SearchResult<LogMessage> babies =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            2,
            QueryBuilderUtil.generateQueryBuilder(
                "baby", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(babies.hits.size()).isEqualTo(2);
    assertThat(
            babies.hits.stream()
                .map(SearchResultHit::message)
                .map(m -> m.getId())
                .collect(Collectors.toList()))
        .isEqualTo(Arrays.asList("Message4", "Message2"));

    // Commit now
    strictLogStore.logStore.commit();
    assertThat(getCount(MESSAGES_RECEIVED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(4);
    assertThat(getCount(MESSAGES_FAILED_COUNTER, strictLogStore.metricsRegistry)).isEqualTo(0);
    assertThat(getTimerCount(REFRESHES_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
    assertThat(getTimerCount(COMMITS_TIMER, strictLogStore.metricsRegistry)).isEqualTo(3);
  }

  @Test
  public void testFullIndexSearch() throws IOException {
    loadTestData(Instant.now());

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(allIndexItems.internalAggregations.get("1"));
    // assertThat(histogram.getTargetBuckets()).isEqualTo(1);

    assertThat(histogram.getBuckets().size()).isEqualTo(4);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(3).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testAggregationWithScripting() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> scriptNull =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createAverageAggregatorFactoriesBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, null));
    assertThat(((InternalAvg) scriptNull.internalAggregations.get("1")).value()).isEqualTo(3.25);

    SearchResult<LogMessage> scriptEmpty =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createAverageAggregatorFactoriesBuilder("1", TEST_SOURCE_LONG_PROPERTY, 0, ""));
    assertThat(((InternalAvg) scriptEmpty.internalAggregations.get("1")).value()).isEqualTo(3.25);

    SearchResult<LogMessage> scripted =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createAverageAggregatorFactoriesBuilder(
                "1", TEST_SOURCE_LONG_PROPERTY, 0, "return 9;"));
    assertThat(((InternalAvg) scripted.internalAggregations.get("1")).value()).isEqualTo(9);
  }

  @Test
  public void testFilterAggregations() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> scriptNull =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createFiltersAggregatorFactoriesBuilder(
                "1",
                List.of(),
                Map.of(
                    "foo",
                    QueryBuilderUtil.generateQueryBuilder(
                        String.format(
                            "%s:<=%s",
                            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                            time.plusSeconds(2).toEpochMilli()),
                        time.toEpochMilli(),
                        time.plusSeconds(2).toEpochMilli()),
                    "bar",
                    QueryBuilderUtil.generateQueryBuilder(
                        String.format(
                            "%s:>%s",
                            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                            time.plusSeconds(2).toEpochMilli()),
                        time.plusSeconds(2).toEpochMilli(),
                        time.plusSeconds(10).toEpochMilli()))));

    InternalFilters filters = (InternalFilters) scriptNull.internalAggregations.get("1");
    assertThat(filters.getBuckets().size()).isEqualTo(2);
    assertThat(filters.getBuckets().get(0).getDocCount()).isEqualTo(2);
    assertThat(filters.getBuckets().get(0).getKey()).isIn(List.of("foo", "bar"));
    assertThat(filters.getBuckets().get(1).getDocCount()).isEqualTo(2);
    assertThat(filters.getBuckets().get(1).getKey()).isIn(List.of("foo", "bar"));
    assertThat(filters.getBuckets().get(0).getKey())
        .isNotEqualTo(filters.getBuckets().get(1).getKey());
  }

  @Test
  public void testFullIndexSearchForMinAgg() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createMinAggregatorFactoriesBuilder(
                "test", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalMin internalMin =
        (InternalMin) Objects.requireNonNull(allIndexItems.internalAggregations.get("test"));

    assertThat(Double.valueOf(internalMin.getValue()).longValue()).isEqualTo(time.toEpochMilli());
  }

  @Test
  public void testFullIndexSearchForMaxAgg() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createMaxAggregatorFactoriesBuilder(
                "test", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalMax internalMax =
        (InternalMax) Objects.requireNonNull(allIndexItems.internalAggregations.get("test"));

    // 4 seconds because of test data
    assertThat(Double.valueOf(internalMax.getValue()).longValue())
        .isEqualTo(time.plus(4, ChronoUnit.SECONDS).toEpochMilli());
  }

  @Test
  public void testFullIndexSearchForSumAgg() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createSumAggregatorFactoriesBuilder("test", TEST_SOURCE_LONG_PROPERTY, "0", null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalSum internalSum =
        (InternalSum) Objects.requireNonNull(allIndexItems.internalAggregations.get("test"));

    // 1, 3, 4, 5
    assertThat(internalSum.getValue()).isEqualTo(13);
  }

  @Test
  public void testFullIndexSearchForExtendedStatsAgg() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createExtendedStatsAggregatorFactoriesBuilder(
                "test", TEST_SOURCE_LONG_PROPERTY, "0", null, null));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    InternalExtendedStats internalExtendedStats =
        (InternalExtendedStats)
            Objects.requireNonNull(allIndexItems.internalAggregations.get("test"));

    // 1, 3, 4, 5
    assertThat(internalExtendedStats).isNotNull();
    assertThat(internalExtendedStats.getCount()).isEqualTo(4);
    assertThat(internalExtendedStats.getMax()).isEqualTo(5);
    assertThat(internalExtendedStats.getMin()).isEqualTo(1);
    assertThat(internalExtendedStats.getSum()).isEqualTo(13);
    assertThat(internalExtendedStats.getAvg()).isEqualTo(3.25);
    assertThat(internalExtendedStats.getSumOfSquares()).isEqualTo(51);
    assertThat(internalExtendedStats.getVariance()).isEqualTo(2.1875);
  }

  @Test
  public void testTermsAggregation() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createTermsAggregatorFactoriesBuilder(
                "1",
                List.of(),
                TEST_SOURCE_STRING_PROPERTY,
                "foo",
                10,
                0,
                Map.of("_count", "asc")));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    StringTerms stringTerms = (StringTerms) allIndexItems.internalAggregations.get("1");
    assertThat(stringTerms.getBuckets().size()).isEqualTo(4);

    List<String> bucketKeys =
        stringTerms.getBuckets().stream().map(bucket -> (String) bucket.getKey()).toList();
    assertThat(bucketKeys.contains("String-1")).isTrue();
    assertThat(bucketKeys.contains("String-3")).isTrue();
    assertThat(bucketKeys.contains("String-4")).isTrue();
    assertThat(bucketKeys.contains("String-5")).isTrue();
  }

  @Test
  public void testTermsAggregationMissingValues() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
            null,
            createTermsAggregatorFactoriesBuilder(
                "1", List.of(), "thisFieldDoesNotExist", "foo", 10, 0, Map.of("_count", "asc")));

    assertThat(allIndexItems.hits.size()).isEqualTo(4);

    StringTerms stringTerms = (StringTerms) allIndexItems.internalAggregations.get("1");
    assertThat(stringTerms.getBuckets().size()).isEqualTo(1);
    assertThat(stringTerms.getBuckets().get(0).getKey()).isEqualTo("foo");
  }

  @Test
  public void testEmptyStringKeywordIsQueryableLikeOpenSearch() throws IOException {
    // Ingest THROUGH SpanFormatter.convertKVtoProto (the ingest path the fix lives in) so this
    // actually exercises the fix: with it, SearchPhrase "" is indexed as a real keyword value;
    // without it, convertKVtoProto returns null and the "" doc carries no SearchPhrase at all.
    Instant time = Instant.ofEpochSecond(1593365471);
    Schema.IngestSchema schema = Schema.IngestSchema.getDefaultInstance();
    List<String> phrases = List.of("alpha", "beta", "");
    for (int i = 0; i < phrases.size(); i++) {
      List<Trace.KeyValue> tags =
          SpanFormatter.convertKVtoProto("SearchPhrase", phrases.get(i), schema);
      strictLogStore.logStore.addMessage(
          SpanUtil.makeSpan(i + 1, "msg", time, tags == null ? List.of() : tags));
    }
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();

    // terms(SearchPhrase): "" is a real bucket alongside the others (Q16/Q17).
    StringTerms terms =
        (StringTerms)
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    0,
                    QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
                    null,
                    createTermsAggregatorFactoriesBuilder(
                        "1", List.of(), "SearchPhrase", null, 10, 1, Map.of("_count", "asc")))
                .internalAggregations
                .get("1");
    assertThat(terms.getBuckets().stream().map(b -> (String) b.getKey()).toList())
        .containsExactlyInAnyOrder("alpha", "beta", "");

    // cardinality(SearchPhrase): "" counts as one distinct value (Q5).
    InternalCardinality cardinality =
        (InternalCardinality)
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    0,
                    QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
                    null,
                    new AggregatorFactories.Builder()
                        .addAggregator(
                            new CardinalityAggregationBuilder("1").field("SearchPhrase")))
                .internalAggregations
                .get("1");
    assertThat(cardinality.getValue()).isEqualTo(3L);

    // must_not term "" excludes the empty-string doc, matching OpenSearch `field <> ''` (Q30/Q31).
    BoolQueryBuilder nonEmptyQuery =
        (BoolQueryBuilder) QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME);
    nonEmptyQuery.mustNot(new TermQueryBuilder("SearchPhrase", ""));
    SearchResult<LogMessage> nonEmpty =
        search(strictLogStore.logSearcher, TEST_DATASET_NAME, 1000, nonEmptyQuery, null, null);
    assertThat(nonEmpty.hits.size()).isEqualTo(2);
  }

  @Test
  public void testFullTextSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);

    Trace.KeyValue customField =
        Trace.KeyValue.newBuilder()
            .setVInt32(1234)
            .setKey("field1")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(1, "apple", time, List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:apple", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);
    // Default all field search.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("Message1", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);

    strictLogStore.logStore.addMessage(
        SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(4), List.of(customField)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
    // Default all field search.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(1);
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    strictLogStore.logStore.addMessage(SpanUtil.makeSpan(2, "baby car 1234", time.plusSeconds(4)));
    strictLogStore.logStore.commit();
    strictLogStore.logStore.refresh();
    // Search using _all field.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);
    // Default all field search.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    // empty string
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("app*", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    // Returns baby or car, 2 messages.
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby car", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(2);

    // Test numbers
    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("apple 1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("123", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);
  }

  @Test
  public void testDisabledFullTextSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    Trace.KeyValue field1Tag =
        Trace.KeyValue.newBuilder()
            .setVInt32(1234)
            .setKey("field1")
            .setFieldType(Schema.SchemaFieldType.INTEGER)
            .build();

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(1, "apple", time.plusSeconds(4), List.of(field1Tag)));

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(2, "apple baby", time.plusSeconds(4), List.of(field1Tag)));

    strictLogStoreWithoutFts.logStore.addMessage(
        SpanUtil.makeSpan(3, "baby car 1234", time.plusSeconds(4)));
    strictLogStoreWithoutFts.logStore.commit();
    strictLogStoreWithoutFts.logStore.refresh();

    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("_all:1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(0);

    // empty string
    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isEqualTo(3);

    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("app*", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    // Returns baby or car, 2 messages.
    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("baby car", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    // Test numbers
    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("apple 1234", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();

    assertThat(
            search(
                    strictLogStoreWithoutFts.logSearcher,
                    TEST_DATASET_NAME,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("123", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder())
                .hits
                .size())
        .isZero();
  }

  @Test
  @Disabled // todo - re-enable when multi-tenancy is supported - slackhq/astra/issues/223
  public void testMissingIndexSearch() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> allIndexItems =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME + "miss",
            1000,
            QueryBuilderUtil.generateQueryBuilder("apple", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());

    assertThat(allIndexItems.hits.size()).isEqualTo(0);

    InternalAutoDateHistogram histogram =
        (InternalAutoDateHistogram)
            Objects.requireNonNull(allIndexItems.internalAggregations.get("1"));
    assertThat(histogram.getTargetBuckets()).isEqualTo(1);

    assertThat(histogram.getBuckets().size()).isEqualTo(4);
    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(2).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(3).getDocCount()).isEqualTo(1);
  }

  @Test
  public void testNoResultQuery() throws IOException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    SearchResult<LogMessage> elephants =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1000,
            QueryBuilderUtil.generateQueryBuilder("elephant", 0L, MAX_TIME),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(elephants.hits.size()).isEqualTo(0);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(elephants.internalAggregations.get("1"));
    assertThat(histogram.getBuckets().size()).isEqualTo(0);
  }

  @Test
  public void testSearchAndNoStats() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> results =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            100,
            QueryBuilderUtil.generateQueryBuilder(
                "_id:Message3 OR _id:Message4",
                time.toEpochMilli(),
                time.plusSeconds(10).toEpochMilli()),
            null,
            null);
    assertThat(results.hits.size()).isEqualTo(2);
    assertThat(results.internalAggregations).isNull();
  }

  @Test
  public void testSearchOnlyHistogram() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> babies =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            0,
            QueryBuilderUtil.generateQueryBuilder(
                "_id:Message3 OR _id:Message4",
                time.toEpochMilli(),
                time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(babies.hits.size()).isEqualTo(0);

    InternalDateHistogram histogram =
        (InternalDateHistogram) Objects.requireNonNull(babies.internalAggregations.get("1"));
    assertThat(histogram.getBuckets().size()).isEqualTo(2);

    assertThat(histogram.getBuckets().get(0).getDocCount()).isEqualTo(1);
    assertThat(histogram.getBuckets().get(1).getDocCount()).isEqualTo(1);

    assertThat(
            Long.parseLong(histogram.getBuckets().get(0).getKeyAsString()) >= time.toEpochMilli())
        .isTrue();
    assertThat(
            Long.parseLong(histogram.getBuckets().get(1).getKeyAsString())
                <= time.plusSeconds(10).toEpochMilli())
        .isTrue();
  }

  @Test
  public void testEmptyIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                search(
                    strictLogStore.logSearcher,
                    "",
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("test", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testNullIndexName() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                search(
                    strictLogStore.logSearcher,
                    null,
                    1000,
                    QueryBuilderUtil.generateQueryBuilder("test", 0L, MAX_TIME),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testSearchOrHistogramQuery() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    0,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli()),
                    null,
                    null));
  }

  /** Verifies omitted requested sort still returns stable internal sort values. */
  @Test
  void testSearchUsesStableInternalSortFieldsWhenRequestOmitsSort() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);

    SearchResult<LogMessage> results =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            1,
            List.of(),
            QueryBuilderUtil.generateQueryBuilder(
                "apple", time.toEpochMilli(), time.plusSeconds(10).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());

    assertThat(results.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("Message5");
    assertThat(results.hits.get(0).sortValues())
        .containsExactly(
            HitSortValue.of(time.plusSeconds(4).toEpochMilli()),
            HitSortValue.bytes(encodedIdSortValue(results.hits.get(0).message().getId())));
  }

  @Test
  public void testNegativeHitCount() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    -1,
                    QueryBuilderUtil.generateQueryBuilder(
                        "test", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testQueryParseError() {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                search(
                    strictLogStore.logSearcher,
                    TEST_DATASET_NAME,
                    1,
                    QueryBuilderUtil.generateQueryBuilder(
                        "/", time.toEpochMilli(), time.plusSeconds(1).toEpochMilli()),
                    null,
                    createGenericDateHistogramAggregatorFactoriesBuilder()));
  }

  @Test
  public void testConcurrentSearches() throws InterruptedException {
    Instant time = Instant.ofEpochSecond(1593365471);
    loadTestData(time);

    AtomicInteger searchFailures = new AtomicInteger(0);
    AtomicInteger statsFailures = new AtomicInteger(0);
    AtomicInteger searchExceptions = new AtomicInteger(0);
    AtomicInteger successfulRuns = new AtomicInteger(0);

    Runnable searchRun =
        () -> {
          for (int i = 0; i < 100; i++) {
            try {
              SearchResult<LogMessage> babies =
                  search(
                      strictLogStore.logSearcher,
                      TEST_DATASET_NAME,
                      100,
                      QueryBuilderUtil.generateQueryBuilder(
                          "_id:Message3 OR _id:Message4", 0L, MAX_TIME),
                      null,
                      createGenericDateHistogramAggregatorFactoriesBuilder());
              if (babies.hits.size() != 2) {
                searchFailures.addAndGet(1);
              } else {
                successfulRuns.addAndGet(1);
              }
            } catch (Exception e) {
              searchExceptions.addAndGet(1);
            }
          }
        };

    Thread t1 = new Thread(searchRun);
    Thread t2 = new Thread(searchRun);
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    assertThat(searchExceptions.get()).isEqualTo(0);
    assertThat(statsFailures.get()).isEqualTo(0);
    assertThat(searchFailures.get()).isEqualTo(0);
    assertThat(successfulRuns.get()).isEqualTo(200);
  }

  @Test
  public void testSearchById() throws IOException {
    Instant time = Instant.now();
    loadTestData(time);
    SearchResult<LogMessage> index =
        search(
            strictLogStore.logSearcher,
            TEST_DATASET_NAME,
            10,
            QueryBuilderUtil.generateQueryBuilder(
                "_id:Message1", time.toEpochMilli(), time.plusSeconds(2).toEpochMilli()),
            null,
            createGenericDateHistogramAggregatorFactoriesBuilder());
    assertThat(index.hits.size()).isEqualTo(1);
  }
}
