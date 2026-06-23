package com.slack.astra.logstore.search;

import static com.slack.astra.util.AggregatorFactoriesUtil.createDateHistogramAggregatorFactoriesBuilder;
import static com.slack.astra.util.AggregatorFactoriesUtil.createDetailedDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
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
import com.slack.service.murron.trace.Trace.KeyValue;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.index.mapper.Uid;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;

public class SearchResultAggregatorImplTest {
  private static List<HitSortValue> sortValues(Object... values) {
    return Arrays.stream(values).map(HitSortValue::of).toList();
  }

  private static List<HitSortValue> missingStringSortValues() {
    return List.of(HitSortValue.nullValue());
  }

  private static ByteString encodedIdSortValue(String id) {
    BytesRef encodedId = Uid.encodeId(id);
    return ByteString.copyFrom(encodedId.bytes, encodedId.offset, encodedId.length);
  }

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

  // The coordinator synthesizes missingQueryableSnapshotCoverage / failedDistributedSubrequest
  // results and feeds them back through this aggregator. Verify they compose correctly with a
  // real per-node result so the requested/fulfilled gap survives aggregation.
  @Test
  public void testAggregatePreservesSyntheticMissingCoverageResults() {
    SearchResult<LogMessage> realNodeResult =
        new SearchResult<>(Collections.emptyList(), 5, 0, 1, 3, 3, null);
    SearchResult<LogMessage> failedSubrequest = SearchResult.failedDistributedSubrequest(2);
    SearchResult<LogMessage> missingMetadata = SearchResult.missingQueryableSnapshotCoverage(4);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            0,
            1,
            10,
            0,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregated =
        new SearchResultAggregatorImpl<LogMessage>(searchQuery)
            .aggregate(List.of(realNodeResult, failedSubrequest, missingMetadata), true);

    // requested = 3 + 2 + 4; fulfilled = 3 + 0 + 0 → 6 missing
    assertThat(aggregated.requestedSnapshots).isEqualTo(9);
    assertThat(aggregated.fulfilledSnapshots).isEqualTo(3);
    assertThat(aggregated.failedSnapshots()).isEqualTo(6);

    // failedNodes/totalNodes only get bumped by the subrequest case (1/1), not by the
    // coordinator-only missing-metadata case.
    assertThat(aggregated.failedNodes).isEqualTo(1);
    assertThat(aggregated.totalNodes).isEqualTo(2);
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
        searchResult(messages1, tookMs, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        searchResult(
            messages2, tookMs + 1, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram2)));

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(0);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(2);

    LogMessage hit = aggSearchResult.hits.get(0).message();
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
        searchResult(messages1, tookMs, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        searchResult(
            messages2, tookMs + 1, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram2)));

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(0);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(2);

    for (SearchResultHit<LogMessage> hit : aggSearchResult.hits) {
      assertThat(messages2.contains(hit.message())).isTrue();
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

  /** Verifies the coordinator merges worker hits using requested sort field values. */
  @Test
  void testSearchResultAggregatorHonorsRequestedHitSort() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage firstMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-1",
            baseTime.plusSeconds(1),
            Map.of("WindowClientWidth", 1024));
    LogMessage secondMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-2",
            baseTime.plusSeconds(2),
            Map.of("WindowClientWidth", 1440));
    LogMessage thirdMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-3",
            baseTime.plusSeconds(3),
            Map.of("WindowClientWidth", 800));
    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(firstMessage, sortValues(1024)),
                new SearchResultHit<>(secondMessage, sortValues(1440))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            List.of(new SearchResultHit<>(thirdMessage, sortValues(800))), 11, 0, 1, 1, 0, null);

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

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
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

    assertThat(
            pagedAggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("message-1");
  }

  /** Verifies numeric sort values compare correctly across Java numeric wrappers. */
  @Test
  void testSearchResultAggregatorComparesNumericSortValuesAcrossJavaTypes() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage integerMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME, "_doc", "integer-rank", baseTime, Map.of("rank", 1));
    LogMessage floatMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "float-rank",
            baseTime.plusSeconds(1),
            Map.of("rank", 1.5f));
    LogMessage longMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "long-rank",
            baseTime.plusSeconds(2),
            Map.of("rank", 2L));

    SearchResult<LogMessage> integerResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    integerMessage,
                    sortValues(
                        1, baseTime.toEpochMilli(), encodedIdSortValue(integerMessage.getId())))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> floatResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    floatMessage,
                    sortValues(
                        1.5f,
                        baseTime.plusSeconds(1).toEpochMilli(),
                        encodedIdSortValue(floatMessage.getId())))),
            11,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> longResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    longMessage,
                    sortValues(
                        2L,
                        baseTime.plusSeconds(2).toEpochMilli(),
                        encodedIdSortValue(longMessage.getId())))),
            12,
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
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(longResult, floatResult, integerResult), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("integer-rank", "float-rank", "long-rank");
  }

  /** Verifies large numeric sort values remain distinct across Java numeric wrappers. */
  @Test
  void testSearchResultAggregatorKeepsLargeNumericSortValuesDistinctAcrossJavaTypes() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage smallerRankMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "smaller-rank",
            baseTime,
            Map.of("rank", 9_007_199_254_740_992D));
    LogMessage largerRankMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "larger-rank",
            baseTime.plusSeconds(1),
            Map.of("rank", 9_007_199_254_740_993L));

    SearchResult<LogMessage> smallerRankResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    smallerRankMessage,
                    sortValues(
                        9_007_199_254_740_992D,
                        baseTime.toEpochMilli(),
                        encodedIdSortValue(smallerRankMessage.getId())))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> largerRankResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    largerRankMessage,
                    sortValues(
                        9_007_199_254_740_993L,
                        baseTime.plusSeconds(1).toEpochMilli(),
                        encodedIdSortValue(largerRankMessage.getId())))),
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
            2,
            0,
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(largerRankResult, smallerRankResult), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("smaller-rank", "larger-rank");
  }

  /** Verifies string sort values use Lucene-compatible UTF-8 byte ordering. */
  @Test
  void testSearchResultAggregatorComparesStringSortValuesByUtf8Bytes() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    String privateUseValue = "\uE000";
    String supplementaryValue = new String(Character.toChars(0x1F600));
    LogMessage privateUseMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "private-use",
            baseTime,
            Map.of("service", privateUseValue));
    LogMessage supplementaryMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "supplementary",
            baseTime,
            Map.of("service", supplementaryValue));

    SearchResult<LogMessage> privateUseResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    privateUseMessage,
                    sortValues(
                        privateUseValue,
                        baseTime.toEpochMilli(),
                        encodedIdSortValue(privateUseMessage.getId())))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> supplementaryResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    supplementaryMessage,
                    sortValues(
                        supplementaryValue,
                        baseTime.toEpochMilli(),
                        encodedIdSortValue(supplementaryMessage.getId())))),
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
            baseTime.plusSeconds(1).toEpochMilli(),
            2,
            0,
            List.of(new SearchQuery.SortFieldSpec("service", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(supplementaryResult, privateUseResult), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("private-use", "supplementary");
  }

  /** Verifies the coordinator honors carried tie-breaker values rather than message fields. */
  @Test
  void testSearchResultAggregatorUsesCarriedTieBreakerSortValues() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage messageTimestampWinner =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-timestamp-winner",
            baseTime.plusSeconds(10),
            Map.of("rank", 100));
    LogMessage carriedSortWinner =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "carried-sort-winner",
            baseTime,
            Map.of("rank", 100));
    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    messageTimestampWinner,
                    sortValues(
                        100,
                        baseTime.toEpochMilli(),
                        encodedIdSortValue(messageTimestampWinner.getId())))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    carriedSortWinner,
                    sortValues(
                        100,
                        baseTime.plusSeconds(1).toEpochMilli(),
                        encodedIdSortValue(carriedSortWinner.getId())))),
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
            baseTime.plusSeconds(20).toEpochMilli(),
            2,
            0,
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("carried-sort-winner", "message-timestamp-winner");
  }

  /** Verifies byte-backed _id tie-breaker values are compared unsigned. */
  @Test
  void testSearchResultAggregatorComparesIdTieBreakerBytesUnsigned() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage zeroByteId =
        new LogMessage(MessageUtil.TEST_DATASET_NAME, "_doc", "zero-byte-id", baseTime, Map.of());
    LogMessage highByteId =
        new LogMessage(MessageUtil.TEST_DATASET_NAME, "_doc", "high-byte-id", baseTime, Map.of());

    SearchResult<LogMessage> highByteResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    highByteId,
                    sortValues(baseTime.toEpochMilli(), ByteString.copyFrom(new byte[] {-1})))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> zeroByteResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    zeroByteId,
                    sortValues(baseTime.toEpochMilli(), ByteString.copyFrom(new byte[] {0})))),
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
            baseTime.plusSeconds(1).toEpochMilli(),
            2,
            0,
            List.of(),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(highByteResult, zeroByteResult), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("zero-byte-id", "high-byte-id");
  }

  /** Verifies descending string sort keeps missing values after present values. */
  @Test
  void testDescendingSortPlacesMissingStringValuesLast() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage presentMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-present",
            baseTime.plusSeconds(1),
            Map.of("service", "zeta"));
    LogMessage missingMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-missing",
            baseTime.plusSeconds(2),
            Map.of());
    SearchResult<LogMessage> presentResult =
        new SearchResult<>(
            List.of(new SearchResultHit<>(presentMessage, sortValues("zeta"))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> missingResult =
        new SearchResult<>(
            List.of(new SearchResultHit<>(missingMessage, missingStringSortValues())),
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
            2,
            0,
            List.of(new SearchQuery.SortFieldSpec("service", SearchQuery.SortDirection.DESC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(missingResult, presentResult), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("message-present", "message-missing");
    assertThat(aggregatedResult.hits.stream().map(SearchResultHit::sortValues).toList())
        .containsExactly(sortValues("zeta"), missingStringSortValues());
  }

  /** Verifies the coordinator does not fail when a hit has a shorter legacy sort tuple. */
  @Test
  void testSearchResultAggregatorHandlesShortSortValueLists() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    LogMessage completeMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "complete-hit",
            baseTime.plusSeconds(1),
            Map.of("rank", 1));
    LogMessage shortMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME, "_doc", "short-hit", baseTime, Map.of("rank", 1));
    SearchResult<LogMessage> completeResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(
                    completeMessage,
                    sortValues(
                        1,
                        baseTime.plusSeconds(1).toEpochMilli(),
                        encodedIdSortValue(completeMessage.getId())))),
            10,
            0,
            1,
            1,
            0,
            null);
    SearchResult<LogMessage> shortResult =
        new SearchResult<>(
            List.of(new SearchResultHit<>(shortMessage, sortValues(1))), 11, 0, 1, 1, 0, null);
    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            baseTime.toEpochMilli(),
            baseTime.plusSeconds(10).toEpochMilli(),
            2,
            0,
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(shortResult, completeResult), true);

    assertThat(aggregatedResult.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("complete-hit", "short-hit");
  }

  /** Verifies each hit keeps its own sort values during aggregation. */
  @Test
  void testSearchResultHitKeepsSortValuesWithMessageDuringAggregation() {
    Instant baseTime = Instant.parse("2024-01-01T00:00:00Z");
    LogMessage firstMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-1",
            baseTime.plusSeconds(1),
            Map.of("WindowClientWidth", 999));
    LogMessage secondMessage =
        new LogMessage(
            MessageUtil.TEST_DATASET_NAME,
            "_doc",
            "message-2",
            baseTime.plusSeconds(2),
            Map.of("WindowClientWidth", 111));
    SearchResult<LogMessage> searchResult =
        new SearchResult<>(
            List.of(
                new SearchResultHit<>(firstMessage, sortValues(10)),
                new SearchResultHit<>(secondMessage, sortValues(20))),
            10,
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
            2,
            0,
            List.of(
                new SearchQuery.SortFieldSpec("WindowClientWidth", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery).aggregate(List.of(searchResult), true);

    assertThat(aggregatedResult.hits.stream().map(SearchResultHit::message).map(LogMessage::getId))
        .containsExactly("message-1", "message-2");
    assertThat(aggregatedResult.hits.stream().map(SearchResultHit::sortValues).toList())
        .containsExactly(sortValues(10), sortValues(20));
  }

  /** Verifies pagination returns only the requested prefix from sorted worker results. */
  @Test
  void testSearchResultAggregatorDoesNotReadUnneededSortedResultTail() {
    Instant baseTime = Instant.parse("2026-05-18T05:00:00Z");
    SearchResultHit<LogMessage> secondBestHit =
        new SearchResultHit<>(
            new LogMessage(
                MessageUtil.TEST_DATASET_NAME,
                "_doc",
                "rank-1",
                baseTime.plusSeconds(1),
                Map.of("rank", 1)),
            sortValues(1));
    SearchResultHit<LogMessage> bestHit =
        new SearchResultHit<>(
            new LogMessage(
                MessageUtil.TEST_DATASET_NAME, "_doc", "rank-0", baseTime, Map.of("rank", 0)),
            sortValues(0));
    SearchResult<LogMessage> resultWithTail =
        new SearchResult<>(listThatFailsIfTailIsRead(secondBestHit), 10, 0, 1, 1, 0, null);
    SearchResult<LogMessage> resultWithBestHit =
        new SearchResult<>(List.of(bestHit), 11, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            baseTime.toEpochMilli(),
            baseTime.plusSeconds(10).toEpochMilli(),
            1,
            0,
            List.of(new SearchQuery.SortFieldSpec("rank", SearchQuery.SortDirection.ASC)),
            Collections.emptyList(),
            null,
            null,
            null);

    SearchResult<LogMessage> aggregatedResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(resultWithTail, resultWithBestHit), true);

    assertThat(
            aggregatedResult.hits.stream()
                .map(SearchResultHit::message)
                .map(LogMessage::getId)
                .toList())
        .containsExactly("rank-0");
  }

  private List<SearchResultHit<LogMessage>> listThatFailsIfTailIsRead(
      SearchResultHit<LogMessage> firstHit) {
    return new AbstractList<>() {
      @Override
      public SearchResultHit<LogMessage> get(int index) {
        if (index == 0) {
          return firstHit;
        }
        throw new AssertionError("Reducer read an unneeded tail hit");
      }

      @Override
      public int size() {
        return 2;
      }
    };
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
        searchResult(messages1, tookMs, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        searchResult(
            messages2, tookMs + 1, 1, 1, 1, 1, InternalAggregations.from(List.of(histogram2)));
    SearchResult<LogMessage> searchResult3 =
        searchResult(
            messages3, tookMs + 2, 0, 1, 1, 0, InternalAggregations.from(List.of(histogram3)));
    SearchResult<LogMessage> searchResult4 =
        searchResult(
            messages4, tookMs + 3, 0, 1, 1, 1, InternalAggregations.from(List.of(histogram4)));

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            histogramStartMs,
            histogramEndMs,
            howMany,
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(2);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(4);

    for (SearchResultHit<LogMessage> hit : aggSearchResult.hits) {
      assertThat(messages4.contains(hit.message())).isTrue();
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
        searchResult(messages1, tookMs, 0, 1, 1, 0, aggregations1);
    SearchResult<LogMessage> searchResult2 =
        searchResult(messages2, tookMs + 1, 0, 1, 1, 0, aggregations2);

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
        searchResult(messages1, tookMs, 0, 1, 1, 0, aggregations1);
    SearchResult<LogMessage> searchResult2 =
        searchResult(messages2, tookMs + 1, 0, 1, 1, 0, aggregations2);

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
  void testBucketSortPipelineAggregation() throws IOException {
    // Commit d428686b358f7b40d382f41b76475bc899fc7cd8 moved distributed
    // aggregation merging to OpenSearch's topLevelReduce API, which has the full aggregation tree
    // needed to run pipeline aggregations during final reduction.
    //
    // SQL equivalent:
    // SELECT bucket_field, COUNT(*) AS doc_count
    // FROM spans
    // GROUP BY bucket_field
    // ORDER BY doc_count DESC
    // LIMIT 1 OFFSET 1
    long tookMs = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long searchStartMs = startTime1.toEpochMilli();
    long searchEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    SearchQuery searchQuery = buildBucketSortAggregationQuery(searchStartMs, searchEndMs);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(
            Collections.emptyList(),
            tookMs,
            0,
            1,
            1,
            0,
            makeAggregations(
                searchQuery,
                searchStartMs,
                searchEndMs,
                makeBucketFieldSpans(startTime1, "A", "A", "B")));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            Collections.emptyList(),
            tookMs + 1,
            0,
            1,
            1,
            0,
            makeAggregations(
                searchQuery,
                searchStartMs,
                searchEndMs,
                makeBucketFieldSpans(startTime2, "A", "C", "C")));

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    assertThat(aggSearchResult.hits).isEmpty();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isZero();
    assertThat(aggSearchResult.fulfilledSnapshots).isZero();
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(2);

    StringTerms combined =
        (StringTerms) Objects.requireNonNull(aggSearchResult.internalAggregations.get("by_bucket"));
    assertThat(combined.getBuckets()).hasSize(1);
    assertThat(combined.getBuckets().getFirst().getKeyAsString()).isEqualTo("C");
    assertThat(combined.getBuckets().getFirst().getDocCount()).isEqualTo(2);
  }

  @Test
  void testBucketSelectorPipelineAggregation() throws IOException {
    // Commit d428686b358f7b40d382f41b76475bc899fc7cd8 moved distributed
    // aggregation merging to OpenSearch's topLevelReduce API, which has the full aggregation tree
    // needed to run pipeline aggregations during final reduction.
    //
    // SQL equivalent:
    // SELECT bucket_field, COUNT(*) AS doc_count
    // FROM spans
    // GROUP BY bucket_field
    // HAVING COUNT(*) > 1
    long tookMs = 10;
    Instant startTime1 = Instant.now();
    Instant startTime2 = startTime1.plus(1, ChronoUnit.HOURS);
    long searchStartMs = startTime1.toEpochMilli();
    long searchEndMs = startTime1.plus(2, ChronoUnit.HOURS).toEpochMilli();

    SearchQuery searchQuery = buildBucketSelectorAggregationQuery(searchStartMs, searchEndMs);

    SearchResult<LogMessage> searchResult1 =
        new SearchResult<>(
            Collections.emptyList(),
            tookMs,
            0,
            1,
            1,
            0,
            makeAggregations(
                searchQuery,
                searchStartMs,
                searchEndMs,
                makeBucketFieldSpans(startTime1, "A", "B")));
    SearchResult<LogMessage> searchResult2 =
        new SearchResult<>(
            Collections.emptyList(),
            tookMs + 1,
            0,
            1,
            1,
            0,
            makeAggregations(
                searchQuery,
                searchStartMs,
                searchEndMs,
                makeBucketFieldSpans(startTime2, "A", "C")));

    SearchResult<LogMessage> aggSearchResult =
        new SearchResultAggregatorImpl<>(searchQuery)
            .aggregate(List.of(searchResult1, searchResult2), true);

    assertThat(aggSearchResult.hits).isEmpty();
    assertThat(aggSearchResult.tookMicros).isEqualTo(tookMs + 1);
    assertThat(aggSearchResult.failedNodes).isZero();
    assertThat(aggSearchResult.fulfilledSnapshots).isZero();
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(2);

    StringTerms combined =
        (StringTerms) Objects.requireNonNull(aggSearchResult.internalAggregations.get("by_bucket"));
    assertThat(combined.getBuckets()).hasSize(1);
    assertThat(combined.getBuckets().getFirst().getKeyAsString()).isEqualTo("A");
    assertThat(combined.getBuckets().getFirst().getDocCount()).isEqualTo(2);
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

    SearchResult<LogMessage> searchResult1 = searchResult(messages1, tookMs, 0, 1, 1, 0, null);
    SearchResult<LogMessage> searchResult2 = searchResult(messages2, tookMs + 1, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            searchStartMs,
            searchEndMs,
            howMany,
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(0);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(2);

    for (SearchResultHit<LogMessage> hit : aggSearchResult.hits) {
      assertThat(messages2.contains(hit.message())).isTrue();
    }

    assertThat(aggSearchResult.internalAggregations).isNull();
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
        searchResult(
            Collections.emptyList(),
            tookMs,
            0,
            2,
            2,
            2,
            InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        searchResult(
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
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(2);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(3);

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
        searchResult(messages1, tookMs, 1, 1, 1, 0, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 = searchResult(messages2, tookMs + 1, 0, 1, 1, 0, null);

    SearchQuery searchQuery =
        new SearchQuery(
            MessageUtil.TEST_DATASET_NAME,
            startTimeMs,
            endTimeMs,
            howMany,
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(0);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(2);

    for (SearchResultHit<LogMessage> hit : aggSearchResult.hits) {
      assertThat(messages2.contains(hit.message())).isTrue();
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
        searchResult(messages1, tookMs, 0, 2, 2, 2, InternalAggregations.from(List.of(histogram1)));
    SearchResult<LogMessage> searchResult2 =
        searchResult(
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
            0,
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
    assertThat(aggSearchResult.fulfilledSnapshots).isEqualTo(2);
    assertThat(aggSearchResult.requestedSnapshots).isEqualTo(3);

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

  private SearchResult<LogMessage> searchResult(
      List<LogMessage> messages,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      InternalAggregations internalAggregations) {
    return new SearchResult<>(
        messages.stream()
            .sorted(
                Comparator.comparing(LogMessage::getTimestamp)
                    .reversed()
                    .thenComparing(LogMessage::getId))
            .map(
                message ->
                    new SearchResultHit<>(
                        message,
                        sortValues(
                            message.getTimestamp().toEpochMilli(),
                            encodedIdSortValue(message.getId()))))
            .toList(),
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas,
        internalAggregations);
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
            new SearchQuery(
                "testDataSet",
                histogramStartMs,
                histogramEndMs,
                0,
                0,
                List.of(),
                QueryBuilderUtil.generateQueryBuilder("*:*", histogramStartMs, histogramEndMs),
                null,
                createDetailedDateHistogramAggregatorFactoriesBuilder(
                    "1",
                    LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
                    interval,
                    0,
                    histogramStartMs,
                    histogramEndMs)));

    try {
      return messageSearchResult.internalAggregations.get("1");
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }

  private List<Trace.Span> makeBucketFieldSpans(Instant startTime, String... bucketValues) {
    List<Trace.Span> result = new ArrayList<>();
    for (int i = 0; i < bucketValues.length; i++) {
      KeyValue bucketTag =
          KeyValue.newBuilder()
              .setKey("bucket_field")
              .setFieldType(Schema.SchemaFieldType.KEYWORD)
              .setVStr(bucketValues[i])
              .build();
      result.add(
          SpanUtil.makeSpan(
              i + 1,
              "bucket-field-" + (i + 1),
              startTime.plus(i, ChronoUnit.MINUTES),
              List.of(bucketTag)));
    }
    return result;
  }

  private SearchQuery buildBucketSortAggregationQuery(long searchStartMs, long searchEndMs) {
    return SearchResultUtils.fromSearchRequest(
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setStartTimeEpochMs(searchStartMs)
            .setEndTimeEpochMs(searchEndMs)
            .setHowMany(0)
            .setAggregationJson(
                """
                {
                  "by_bucket": {
                    "terms": {
                      "field": "bucket_field",
                      "size": 10,
                      "min_doc_count": 1
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
                          "from": 1,
                          "size": 1
                        }
                      }
                    }
                  }
                }
                """)
            .build());
  }

  private SearchQuery buildBucketSelectorAggregationQuery(long searchStartMs, long searchEndMs) {
    return SearchResultUtils.fromSearchRequest(
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setStartTimeEpochMs(searchStartMs)
            .setEndTimeEpochMs(searchEndMs)
            .setHowMany(0)
            .setAggregationJson(
                """
                {
                  "by_bucket": {
                    "terms": {
                      "field": "bucket_field",
                      "size": 10,
                      "min_doc_count": 1
                    },
                    "aggs": {
                      "having": {
                        "bucket_selector": {
                          "buckets_path": {
                            "c": "_count"
                          },
                          "script": "params.c > 1"
                        }
                      }
                    }
                  }
                }
                """)
            .build());
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
            new SearchQuery(
                MessageUtil.TEST_DATASET_NAME,
                histogramStartMs,
                histogramEndMs,
                0,
                0,
                List.of(),
                QueryBuilderUtil.generateQueryBuilder("*:*", histogramStartMs, histogramEndMs),
                null,
                searchQuery.aggregatorFactoriesBuilder));

    try {
      return messageSearchResult.internalAggregations;
    } finally {
      logSearcher.close();
      logStore.close();
      logStore.cleanup();
    }
  }
}
