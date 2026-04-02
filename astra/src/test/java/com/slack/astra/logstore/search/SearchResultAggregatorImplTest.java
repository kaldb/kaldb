package com.slack.astra.logstore.search;

import static com.slack.astra.util.AggregatorFactoriesUtil.createDateHistogramAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createDetailedDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.google.common.io.Files;
import com.slack.astra.logstore.DocumentBuilder;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.logstore.LuceneIndexStoreConfig;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.util.QueryBuilderUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;

public class SearchResultAggregatorImplTest {
  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
  }

  @Test
  public void testSimpleSearchResultsAggWithOneResult() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 1;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);

    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    LogMessage hit = aggSearchResult.hits.get(0);
    assertThat(hit.getId()).contains("Message20");
    assertThat(hit.getTimestamp()).isEqualTo(startTime2.plus(9, ChronoUnit.MINUTES));

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSimpleSearchResultsAggWithMultipleResults() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSearchResultAggregatorOn4Results() throws IOException {
    long tookMs = 10;
    int bucketCount = 25;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    Instant startTime3 = startTime1.plus(2, ChronoUnit.HOURS);
    Instant startTime4 = startTime1.plus(3, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(4, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);
    List<LogMessage> messages3 =
        MessageUtil.makeMessagesWithTimeDifference(21, 30, 1000 * 60, startTime3);
    List<LogMessage> messages4 =
        MessageUtil.makeMessagesWithTimeDifference(31, 40, 1000 * 60, startTime4);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));
    InternalAggregation histogram3 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(21, 30, 1000 * 60, startTime3));
    InternalAggregation histogram4 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(31, 40, 1000 * 60, startTime4));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 1, 1, 1, 1, histogram2);
    SearchResult<LogMessage> searchResult3 =
        new SearchResult<>(messages3, tookMs + 2, 0, 1, 1, 0, histogram3);
    SearchResult<LogMessage> searchResult4 =
        new SearchResult<>(messages4, tookMs + 3, 0, 1, 1, 1, histogram4);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults =
        List.of(searchResult1, searchResult4, searchResult3, searchResult2);
    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 3);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(4);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages4.contains(m)).isTrue();
    }

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size() + messages3.size() + messages4.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSimpleSearchResultsAggWithNoHistograms() throws IOException {
    long tookMs = 10;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long searchStartMs = startTime1.toEpochMilli();
    long searchEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            searchStartMs,
            searchEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", searchStartMs, searchEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    assertThat(aggSearchResult.internalAggregation).isNull();
  }

  @Test
  public void testSearchResultAggregatorRespectsDescendingNumericSortAcrossResults()
      throws IOException {
    Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
    long startTimeMs = startTime.toEpochMilli();
    long endTimeMs = startTime.plus(1, ChronoUnit.DAYS).toEpochMilli();

    LogMessage shard1HighestTip =
        makeLogMessage("shard-1-highest-tip", startTime.plus(5, ChronoUnit.MINUTES), 9.5);
    LogMessage shard1LowerTip =
        makeLogMessage("shard-1-lower-tip", startTime.plus(40, ChronoUnit.MINUTES), 1.5);
    LogMessage shard2SecondHighestTip =
        makeLogMessage("shard-2-second-highest-tip", startTime.plus(10, ChronoUnit.MINUTES), 8.0);
    LogMessage shard2LowestTip =
        makeLogMessage("shard-2-lowest-tip", startTime.plus(50, ChronoUnit.MINUTES), 0.5);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(List.of(shard1HighestTip, shard1LowerTip), 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(List.of(shard2SecondHighestTip, shard2LowestTip), 11, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTimeMs,
            endTimeMs,
            3,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("*:*", startTimeMs, endTimeMs),
            null,
            null,
            "[{\"tip_amount\":\"desc\"}]");

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggSearchResult.hits.stream().map(LogMessage::getId).collect(Collectors.toList()))
        .containsExactly("shard-1-highest-tip", "shard-2-second-highest-tip", "shard-1-lower-tip");
  }

  @Test
  public void testSearchResultAggregatorRespectsAscendingNumericSortAcrossResults()
      throws IOException {
    Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
    long startTimeMs = startTime.toEpochMilli();
    long endTimeMs = startTime.plus(1, ChronoUnit.DAYS).toEpochMilli();

    LogMessage shard1LowestTip =
        makeLogMessage("shard-1-lowest-tip", startTime.plus(45, ChronoUnit.MINUTES), 0.5);
    LogMessage shard1HighestTip =
        makeLogMessage("shard-1-highest-tip", startTime.plus(5, ChronoUnit.MINUTES), 5.0);
    LogMessage shard2SecondLowestTip =
        makeLogMessage("shard-2-second-lowest-tip", startTime.plus(35, ChronoUnit.MINUTES), 1.0);
    LogMessage shard2ThirdLowestTip =
        makeLogMessage("shard-2-third-lowest-tip", startTime.plus(15, ChronoUnit.MINUTES), 2.0);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(List.of(shard1LowestTip, shard1HighestTip), 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            List.of(shard2SecondLowestTip, shard2ThirdLowestTip), 11, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTimeMs,
            endTimeMs,
            3,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("*:*", startTimeMs, endTimeMs),
            null,
            null,
            "[{\"tip_amount\":\"asc\"}]");

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggSearchResult.hits.stream().map(LogMessage::getId).collect(Collectors.toList()))
        .containsExactly(
            "shard-1-lowest-tip", "shard-2-second-lowest-tip", "shard-2-third-lowest-tip");
  }

  @Test
  public void testSearchResultAggregatorRespectsSecondarySortAcrossResults() throws IOException {
    Instant startTime = Instant.parse("2024-01-01T00:00:00Z");
    long startTimeMs = startTime.toEpochMilli();
    long endTimeMs = startTime.plus(1, ChronoUnit.DAYS).toEpochMilli();

    LogMessage shard1First =
        makeLogMessage("shard-1-first", startTime.plus(5, ChronoUnit.MINUTES), 10.0, 400.0);
    LogMessage shard1Second =
        makeLogMessage("shard-1-second", startTime.plus(10, ChronoUnit.MINUTES), 10.0, 300.0);
    LogMessage shard2First =
        makeLogMessage("shard-2-first", startTime.plus(15, ChronoUnit.MINUTES), 10.0, 500.0);
    LogMessage shard2Second =
        makeLogMessage("shard-2-second", startTime.plus(20, ChronoUnit.MINUTES), 9.0, 900.0);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(List.of(shard1First, shard1Second), 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(List.of(shard2First, shard2Second), 11, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTimeMs,
            endTimeMs,
            4,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("*:*", startTimeMs, endTimeMs),
            null,
            null,
            "[{\"tip_amount\":\"desc\"},{\"total_amount\":\"desc\"}]");

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), false);

    assertThat(aggSearchResult.hits.stream().map(LogMessage::getId).collect(Collectors.toList()))
        .containsExactly("shard-2-first", "shard-1-first", "shard-1-second", "shard-2-second");
  }

  @Test
  public void testSimpleSearchResultsAggNoHits() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 0;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(Collections.emptyList(), tookMs, 0, 2, 2, 2, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(Collections.emptyList(), tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSearchResultsAggIgnoresBucketsInSearchResultsSafely() throws IOException {
    long tookMs = 10;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long startTimeMs = startTime1.toEpochMilli();
    long endTimeMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            startTimeMs,
            endTimeMs,
            "6m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 1, 1, 1, 0, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTimeMs,
            endTimeMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", startTimeMs, endTimeMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, false);

    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.hits.size()).isEqualTo(howMany);
    assertThat(aggSearchResult.failedNodes).isEqualTo(1);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(0);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(2);

    for (LogMessage m : aggSearchResult.hits) {
      assertThat(messages2.contains(m)).isTrue();
    }

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(internalDateHistogram).isEqualTo(histogram1);
  }

  @Test
  public void testSimpleSearchResultsAggIgnoreHitsSafely() throws IOException {
    long tookMs = 10;
    int bucketCount = 13;
    int howMany = 0;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    InternalAggregation histogram1 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregation histogram2 =
        makeHistogram(
            histogramStartMs,
            histogramEndMs,
            "10m",
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 2, 2, 2, histogram1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(Collections.emptyList(), tookMs + 1, 0, 1, 1, 0, histogram2);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", histogramStartMs, histogramEndMs),
            null,
            createDateHistogramAggregatorFactoriesBuilder(
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1),
            null);
    List<SearchResult<LogMessage>> searchResults = new ArrayList<>(2);
    searchResults.add(searchResult1);
    searchResults.add(searchResult2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(searchResults, true);

    assertThat(aggSearchResult.hits.size()).isZero();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isEqualTo(0);
    assertThat(aggSearchResult.snapshotsWithReplicas).isEqualTo(2);
    assertThat(aggSearchResult.totalSnapshots).isEqualTo(3);

    InternalDateHistogram internalDateHistogram =
        Objects.requireNonNull((InternalDateHistogram) aggSearchResult.internalAggregation);
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testAutoDateHistogramPartialReductionNeedsFinalPass() throws Exception {
    Instant startTime = Instant.parse("2024-01-01T00:00:00Z");

    SearchQuery searchQuery =
        SearchResultUtils.fromSearchRequest(
            AstraSearch.SearchRequest.newBuilder()
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setHowMany(0)
                .setQuery("{\"match_all\":{}}")
                .setAggregationJson(
                    """
                    {
                      "dropoffs_over_time": {
                        "auto_date_histogram": {
                          "field": "_timesinceepoch",
                          "buckets": 1
                        }
                      }
                    }
                    """)
                .build());

    SearchResult<LogMessage> partiallyReduced =
        makeAutoDateHistogramSearchResult(
            searchQuery,
            SpanUtil.makeSpansWithTimeDifference(
                1, 40, ChronoUnit.DAYS.getDuration().toMillis(), startTime));
    SearchResult<LogMessage> finalized =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(List.of(partiallyReduced), true);

    InternalAutoDateHistogram partiallyReducedHistogram =
        (InternalAutoDateHistogram) Objects.requireNonNull(partiallyReduced.internalAggregation);
    InternalAutoDateHistogram finalizedHistogram =
        (InternalAutoDateHistogram) Objects.requireNonNull(finalized.internalAggregation);

    assertThat(partiallyReducedHistogram.getBuckets().size())
        .isGreaterThan(finalizedHistogram.getBuckets().size());
  }

  /**
   * Makes an InternalDateHistogram given the provided configuration. Since the
   * InternalDateHistogram has private constructors this uses a temporary LogSearcher to index,
   * search, and then collect the results into an appropriate aggregation.
   */
  private InternalAggregation makeHistogram(
      long histogramStartMs, long histogramEndMs, String interval, List<Trace.Span> logMessages)
      throws IOException {
    File tempFolder = Files.createTempDir();
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            Duration.of(1, ChronoUnit.MINUTES),
            Duration.of(1, ChronoUnit.MINUTES),
            tempFolder.getCanonicalPath(),
            false);
    MeterRegistry metricsRegistry = new SimpleMeterRegistry();
    DocumentBuilder documentBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD,
            true,
            metricsRegistry);

    LogStore logStore = new LuceneIndexStoreImpl(indexStoreCfg, documentBuilder, metricsRegistry);
    LogIndexSearcherImpl logSearcher =
        new LogIndexSearcherImpl(logStore.getSearcherManager(), logStore.getSchema());

    for (Trace.Span logMessage : logMessages) {
      logStore.addMessage(logMessage);
    }
    logStore.commit();
    logStore.refresh();

    SearchResult<LogMessage> messageSearchResult =
        logSearcher.search(
            "testDataSet",
            0,
            QueryBuilderUtil.generateQueryBuilder("*:*", histogramStartMs, histogramEndMs),
            null,
            createDetailedDateHistogramAggregatorFactoriesBuilder(
                "1",
                LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                interval,
                0,
                histogramStartMs,
                histogramEndMs),
            null);

    try {
      return messageSearchResult.internalAggregation;
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }

  private LogMessage makeLogMessage(String id, Instant timestamp, double tipAmount) {
    return makeLogMessage(id, timestamp, tipAmount, tipAmount);
  }

  private LogMessage makeLogMessage(
      String id, Instant timestamp, double tipAmount, double totalAmount) {
    return new LogMessage(
        MessageUtil.TEST_DATASET_NAME,
        "testType",
        id,
        timestamp,
        Map.of("tip_amount", tipAmount, "total_amount", totalAmount));
  }

  private SearchResult<LogMessage> makeAutoDateHistogramSearchResult(
      SearchQuery searchQuery, List<Trace.Span> logMessages) throws IOException {
    File tempFolder = Files.createTempDir();
    LuceneIndexStoreConfig indexStoreCfg =
        new LuceneIndexStoreConfig(
            Duration.of(1, ChronoUnit.MINUTES),
            Duration.of(1, ChronoUnit.MINUTES),
            tempFolder.getCanonicalPath(),
            false);
    MeterRegistry metricsRegistry = new SimpleMeterRegistry();
    DocumentBuilder documentBuilder =
        SchemaAwareLogDocumentBuilderImpl.build(
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.DROP_FIELD,
            true,
            metricsRegistry);

    LogStore logStore = new LuceneIndexStoreImpl(indexStoreCfg, documentBuilder, metricsRegistry);
    LogIndexSearcherImpl logSearcher =
        new LogIndexSearcherImpl(logStore.getSearcherManager(), logStore.getSchema());

    for (Trace.Span logMessage : logMessages) {
      logStore.addMessage(logMessage);
    }
    logStore.commit();
    logStore.refresh();

    try {
      return logSearcher.search(
          searchQuery.dataset,
          searchQuery.howMany,
          searchQuery.queryBuilder,
          null,
          searchQuery.aggregatorFactoriesBuilder,
          searchQuery.sortJson);
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }
}
