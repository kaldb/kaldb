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
import com.slack.astra.proto.schema.Schema;
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
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;

public class SearchResultAggregatorImplTest {
  private static Trace.Span makeDimensionSpan(
      int id, Instant timestamp, String country, String browser) {
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
                .build()));
  }

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
        new SearchResult<>(
            messages1, tookMs, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            messages2, tookMs + 1, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram2)));

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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1));
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
        Objects.requireNonNull(
            (InternalDateHistogram) aggSearchResult.internalAggregations.get("1"));
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
        new SearchResult<>(
            messages1, tookMs, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            messages2, tookMs + 1, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram2)));

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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1));
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
        Objects.requireNonNull(
            (InternalDateHistogram) aggSearchResult.internalAggregations.get("1"));
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSearchResultAggregatorHonorsRequestedHitSort() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(
            List.of(
                new LogMessage(
                    MessageUtil.TEST_DATASET_NAME,
                    "_doc",
                    "message-1",
                    baseTime.plusSeconds(1),
                    Map.of("WindowClientWidth", 1024)),
                new LogMessage(
                    MessageUtil.TEST_DATASET_NAME,
                    "_doc",
                    "message-2",
                    baseTime.plusSeconds(2),
                    Map.of("WindowClientWidth", 1440))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            List.of(
                new LogMessage(
                    MessageUtil.TEST_DATASET_NAME,
                    "_doc",
                    "message-3",
                    baseTime.plusSeconds(3),
                    Map.of("WindowClientWidth", 800))),
            11,
            0,
            1,
            1,
            0,
            null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            baseTime.toEpochMilli(),
            baseTime.plusSeconds(10).toEpochMilli(),
            3,
            0,
            List.of(
                new SearchQuery.SortFieldSpec("WindowClientWidth", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    assertThat(aggregatedResult.hits.stream().map(LogMessage::getId).toList())
        .containsExactly("message-3", "message-1", "message-2");

    SearchQuery pagedSearchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            baseTime.toEpochMilli(),
            baseTime.plusSeconds(10).toEpochMilli(),
            1,
            1,
            List.of(
                new SearchQuery.SortFieldSpec("WindowClientWidth", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);
    SearchResult<LogMessage> pagedAggregatedResult =
        new SearchResultAggregatorImpl<>(pagedSearchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    assertThat(pagedAggregatedResult.hits.stream().map(LogMessage::getId).toList())
        .containsExactly("message-1");
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
        new SearchResult<>(
            messages1, tookMs, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            messages2, tookMs + 1, 1, 1, 1, 1, InternalAggregations.from(List.of(histogram2)));
    SearchResult<LogMessage> searchResult3 =
        new SearchResult<>(
            messages3, tookMs + 2, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram3)));
    SearchResult<LogMessage> searchResult4 =
        new SearchResult<>(
            messages4, tookMs + 3, 0, 1, 1, 1, InternalAggregations.from(List.of(histogram4)));

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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1));
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
        Objects.requireNonNull(
            (InternalDateHistogram) aggSearchResult.internalAggregations.get("1"));
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size() + messages3.size() + messages4.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
  }

  @Test
  public void testSearchResultAggregatorWithMultipleSiblingAggregations() throws IOException {
    long tookMs = 10;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    SearchQuery searchQuery =
        buildSiblingAggregationQuery(histogramStartMs, histogramEndMs, howMany, "10m");

    InternalAggregations aggregations1 =
        makeAggregations(
            searchQuery,
            histogramStartMs,
            histogramEndMs,
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregations aggregations2 =
        makeAggregations(
            searchQuery,
            histogramStartMs,
            histogramEndMs,
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, aggregations1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, aggregations2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    InternalAggregations combined = Objects.requireNonNull(aggSearchResult.internalAggregations);
    InternalDateHistogram overTime =
        (InternalDateHistogram) Objects.requireNonNull(combined.get("over_time"));
    assertThat(
            overTime.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());

    StringTerms services = (StringTerms) Objects.requireNonNull(combined.get("services"));
    assertThat(services.getBuckets()).hasSize(1);
    assertThat(services.getBuckets().getFirst().getDocCount())
        .isEqualTo(messages1.size() + messages2.size());
  }

  @Test
  public void testSearchResultAggregatorWithNestedAndSiblingAggregations() throws IOException {
    long tookMs = 10;
    int howMany = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long histogramStartMs = startTime1.toEpochMilli();
    long histogramEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    List<LogMessage> messages1 =
        MessageUtil.makeMessagesWithTimeDifference(1, 10, 1000 * 60, startTime1);
    List<LogMessage> messages2 =
        MessageUtil.makeMessagesWithTimeDifference(11, 20, 1000 * 60, startTime2);

    SearchQuery searchQuery =
        buildNestedAndSiblingAggregationQuery(histogramStartMs, histogramEndMs, howMany, "10m");

    InternalAggregations aggregations1 =
        makeAggregations(
            searchQuery,
            histogramStartMs,
            histogramEndMs,
            SpanUtil.makeSpansWithTimeDifference(1, 10, 1000 * 60, startTime1));
    InternalAggregations aggregations2 =
        makeAggregations(
            searchQuery,
            histogramStartMs,
            histogramEndMs,
            SpanUtil.makeSpansWithTimeDifference(11, 20, 1000 * 60, startTime2));

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(messages1, tookMs, 0, 1, 1, 0, aggregations1);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(messages2, tookMs + 1, 0, 1, 1, 0, aggregations2);

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    InternalAggregations combined = Objects.requireNonNull(aggSearchResult.internalAggregations);
    InternalDateHistogram overTime =
        (InternalDateHistogram) Objects.requireNonNull(combined.get("over_time"));
    assertThat(
            overTime.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());

    long nestedServicesDocCount =
        overTime.getBuckets().stream()
            .map(bucket -> (StringTerms) bucket.getAggregations().get("bucket_services"))
            .filter(Objects::nonNull)
            .mapToLong(
                bucketServices ->
                    bucketServices.getBuckets().stream()
                        .mapToLong(bucket -> bucket.getDocCount())
                        .sum())
            .sum();
    assertThat(nestedServicesDocCount).isEqualTo(messages1.size() + messages2.size());

    StringTerms allServices = (StringTerms) Objects.requireNonNull(combined.get("all_services"));
    assertThat(allServices.getBuckets()).hasSize(1);
    assertThat(allServices.getBuckets().getFirst().getDocCount())
        .isEqualTo(messages1.size() + messages2.size());
  }

  /** Verifies compound multi_terms buckets are reduced across shard search results. */
  @Test
  public void testSearchResultAggregatorReducesMultiTermsBuckets() throws IOException {
    Instant startTime = Instant.now().minusSeconds(10);
    long searchStartMs = startTime.toEpochMilli();
    long searchEndMs = startTime.plusSeconds(1).toEpochMilli();
    List<Trace.Span> shard1Rows =
        List.of(
            makeDimensionSpan(1, startTime.plusMillis(1), "US", "Chrome"),
            makeDimensionSpan(2, startTime.plusMillis(2), "CA", "Safari"));
    List<Trace.Span> shard2Rows =
        List.of(
            makeDimensionSpan(3, startTime.plusMillis(3), "US", "Chrome"),
            makeDimensionSpan(4, startTime.plusMillis(4), "US", "Safari"));
    Map<List<Object>, Long> expectedBuckets =
        Map.of(
            List.of("US", "Chrome"), 2L,
            List.of("CA", "Safari"), 1L,
            List.of("US", "Safari"), 1L);
    SearchQuery searchQuery =
        SearchResultUtils.fromSearchRequest(
            AstraSearch.SearchRequest.newBuilder()
                .setDataset(MessageUtil.TEST_DATASET_NAME)
                .setStartTimeEpochMs(searchStartMs)
                .setEndTimeEpochMs(searchEndMs)
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
                        }
                      }
                    }
                    """)
                .build());

    InternalAggregations shard1Aggregations =
        makeAggregations(searchQuery, searchStartMs, searchEndMs, shard1Rows);
    InternalAggregations shard2Aggregations =
        makeAggregations(searchQuery, searchStartMs, searchEndMs, shard2Rows);

    SearchResult<LogMessage> result =
        new SearchResultAggregatorImpl<LogMessage>(searchQuery)
            .aggregate(
                List.of(
                    new SearchResult<>(List.of(), 0, 0, 1, 1, 0, shard1Aggregations),
                    new SearchResult<>(List.of(), 0, 0, 1, 1, 0, shard2Aggregations)),
                true);

    InternalMultiTerms dimensions =
        (InternalMultiTerms) Objects.requireNonNull(result.internalAggregations).get("dimensions");
    Map<List<Object>, Long> actualBuckets =
        dimensions.getBuckets().stream()
            .collect(
                Collectors.toMap(
                    InternalMultiTerms.Bucket::getKey, InternalMultiTerms.Bucket::getDocCount));
    assertThat(actualBuckets).isEqualTo(expectedBuckets);
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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1));
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

    assertThat(aggSearchResult.internalAggregations).isNull();
  }

  @Test
  public void testSearchResultAggregatorMergesTotalHits() throws IOException {
    SearchResult<LogMessage> exactResult =
        new SearchResult<>(
            Collections.emptyList(),
            10,
            0,
            1,
            1,
            1,
            7,
            SearchResult.TotalHitsRelation.EQUAL_TO,
            null);
    SearchResult<LogMessage> lowerBoundResult =
        new SearchResult<>(
            Collections.emptyList(),
            11,
            0,
            1,
            1,
            1,
            5,
            SearchResult.TotalHitsRelation.GREATER_THAN_OR_EQUAL_TO,
            null);
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0,
            1,
            0,
            Collections.emptyList(),
            QueryBuilderUtil.generateQueryBuilder("Message1", 0L, 1L),
            null,
            null,
            SearchQuery.TotalHitsPolicy.threshold(5));

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(exactResult, lowerBoundResult), true);

    assertThat(aggSearchResult.totalHits).isEqualTo(12);
    assertThat(aggSearchResult.totalHitsRelation)
        .isEqualTo(SearchResult.TotalHitsRelation.GREATER_THAN_OR_EQUAL_TO);
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
        new SearchResult<>(
            Collections.emptyList(),
            tookMs,
            0,
            2,
            2,
            2,
            InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            Collections.emptyList(),
            tookMs + 1,
            0,
            1,
            1,
            0,
            InternalAggregations.from(List.of(histogram2)));

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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1));
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
        Objects.requireNonNull(
            (InternalDateHistogram) aggSearchResult.internalAggregations.get("1"));
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
        new SearchResult<>(
            messages1, tookMs, 1, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "6m", 1));
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
        Objects.requireNonNull(
            (InternalDateHistogram) aggSearchResult.internalAggregations.get("1"));
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
        new SearchResult<>(
            messages1, tookMs, 0, 2, 2, 2, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            Collections.emptyList(),
            tookMs + 1,
            0,
            1,
            1,
            0,
            InternalAggregations.from(List.of(histogram2)));

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
                "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1));
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
        Objects.requireNonNull(
            (InternalDateHistogram) aggSearchResult.internalAggregations.get("1"));
    assertThat(
            internalDateHistogram.getBuckets().stream()
                .collect(Collectors.summarizingLong(InternalDateHistogram.Bucket::getDocCount))
                .getSum())
        .isEqualTo(messages1.size() + messages2.size());
    assertThat(internalDateHistogram.getBuckets().size()).isEqualTo(bucketCount);
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
        new LogIndexSearcherImpl(logStore.getAstraSearcherManager(), logStore.getSchema());

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
                histogramEndMs));

    try {
      return messageSearchResult.internalAggregations.get("1");
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }

  private SearchQuery buildSiblingAggregationQuery(
      long histogramStartMs, long histogramEndMs, int howMany, String interval) {
    return SearchResultUtils.fromSearchRequest(
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setStartTimeEpochMs(histogramStartMs)
            .setEndTimeEpochMs(histogramEndMs)
            .setHowMany(howMany)
            .setAggregationJson(
                """
                {
                  "over_time": {
                    "date_histogram": {
                      "field": "_timesinceepoch",
                      "interval": "%s",
                      "min_doc_count": 0,
                      "extended_bounds": {
                        "min": %d,
                        "max": %d
                      },
                      "format": "epoch_millis"
                    },
                    "aggs": {}
                  },
                  "services": {
                    "terms": {
                      "field": "service_name",
                      "size": 10,
                      "min_doc_count": 1
                    }
                  }
                }
                """
                    .formatted(interval, histogramStartMs, histogramEndMs))
            .build());
  }

  private SearchQuery buildNestedAndSiblingAggregationQuery(
      long histogramStartMs, long histogramEndMs, int howMany, String interval) {
    return SearchResultUtils.fromSearchRequest(
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setStartTimeEpochMs(histogramStartMs)
            .setEndTimeEpochMs(histogramEndMs)
            .setHowMany(howMany)
            .setAggregationJson(
                """
                {
                  "over_time": {
                    "date_histogram": {
                      "field": "_timesinceepoch",
                      "interval": "%s",
                      "min_doc_count": 0,
                      "extended_bounds": {
                        "min": %d,
                        "max": %d
                      },
                      "format": "epoch_millis"
                    },
                    "aggs": {
                      "bucket_services": {
                        "terms": {
                          "field": "service_name",
                          "size": 10,
                          "min_doc_count": 1
                        }
                      }
                    }
                  },
                  "all_services": {
                    "terms": {
                      "field": "service_name",
                      "size": 10,
                      "min_doc_count": 1
                    }
                  }
                }
                """
                    .formatted(interval, histogramStartMs, histogramEndMs))
            .build());
  }

  private InternalAggregations makeAggregations(
      SearchQuery searchQuery,
      long histogramStartMs,
      long histogramEndMs,
      List<Trace.Span> logMessages)
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
        new LogIndexSearcherImpl(logStore.getAstraSearcherManager(), logStore.getSchema());

    for (Trace.Span logMessage : logMessages) {
      logStore.addMessage(logMessage);
    }
    logStore.commit();
    logStore.refresh();

    SearchResult<LogMessage> messageSearchResult =
        logSearcher.search(
            MessageUtil.TEST_DATASET_NAME,
            0,
            QueryBuilderUtil.generateQueryBuilder("*:*", histogramStartMs, histogramEndMs),
            null,
            searchQuery.aggregatorFactoriesBuilder);

    try {
      return messageSearchResult.internalAggregations;
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }
}
