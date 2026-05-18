package com.slack.astra.server;

import static com.slack.astra.util.AggregatorFactoriesUtil.createGenericDateHistogramAggregatorFactoriesBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.logstore.search.HitSortValue;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.logstore.search.SearchResultHit;
import com.slack.astra.logstore.search.SearchResultUtils;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.testlib.MessageUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.lucene.document.InetAddressPoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;

public class SearchResultTest {

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherExtension(false);

  public SearchResultTest() throws IOException {}

  private static AggregatorFactories.Builder createTwoAverageAggregations() {
    AvgAggregationBuilder foo = new AvgAggregationBuilder("foo");
    foo.field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    foo.missing("2");

    AvgAggregationBuilder bar = new AvgAggregationBuilder("bar");
    bar.field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    bar.missing("2");

    AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
    builder.addAggregator(foo);
    builder.addAggregator(bar);
    return builder;
  }

  @Test
  public void testSearchResultObjectConversions() throws Exception {
    Tracing.newBuilder().build();
    List<LogMessage> logMessages = new ArrayList<>();
    Random random = new Random();

    int numDocs = random.nextInt(10);
    // if we ever fail easy to repro - ideally we want to use a test framework like lucene which
    // gives us a test seed
    System.out.println("numDocs=" + numDocs);
    for (int i = 0; i < numDocs; i++) {
      LogMessage logMessage = MessageUtil.makeMessage(i);
      logMessages.add(logMessage);
    }
    OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(Map.of());

    OpenSearchAdapter.AggregationExecution aggregationExecution =
        openSearchAdapter.createAggregationExecution(
            createGenericDateHistogramAggregatorFactoriesBuilder(),
            logStoreAndSearcherRule
                .logStore
                .getAstraSearcherManager()
                .getLuceneSearcherManager()
                .acquire(),
            null);
    InternalAggregations internalAggregations = aggregationExecution.finish();
    SearchResult<LogMessage> searchResult =
        searchResult(logMessages, 1, 1, 5, 7, 7, internalAggregations);
    AstraSearch.SearchResult protoSearchResult =
        SearchResultUtils.toSearchResultProto(searchResult);

    assertThat(protoSearchResult.getHitsCount()).isEqualTo(numDocs);
    assertThat(protoSearchResult.getTookMicros()).isEqualTo(1);
    assertThat(protoSearchResult.getFailedNodes()).isEqualTo(1);
    assertThat(protoSearchResult.getTotalNodes()).isEqualTo(5);
    assertThat(protoSearchResult.getRequestedSnapshots()).isEqualTo(7);
    assertThat(protoSearchResult.getFulfilledSnapshots()).isEqualTo(7);
    assertThat(protoSearchResult.getInternalAggregations().toByteArray())
        .isEqualTo(OpenSearchInternalAggregation.toByteArray(internalAggregations));

    SearchResult<LogMessage> convertedSearchResult =
        SearchResultUtils.fromSearchResultProto(protoSearchResult);

    assertThat(convertedSearchResult).isEqualTo(searchResult);
  }

  /** Verifies equality and hashCode when both results have null aggregations. */
  @Test
  public void testSearchResultEqualsAndHashCodeHandleNullAggregation() {
    SearchResult<LogMessage> searchResultA = new SearchResult<>(List.of(), 1, 0, 0, 1, 0, null);
    SearchResult<LogMessage> searchResultB = new SearchResult<>(List.of(), 1, 0, 0, 1, 0, null);

    assertThat(searchResultA).isEqualTo(searchResultB);
    assertThat(searchResultA.hashCode()).isEqualTo(searchResultB.hashCode());
  }

  // The (requested, fulfilled, failedNodes, totalNodes) tuple of each missing-coverage factory
  // is a load-bearing invariant: it determines _shards.failed and node-health counters in the
  // OpenSearch response. Any drift here would silently regress the user-visible response.
  /** Verifies the local hard-failure factory tuple used for shard accounting. */
  @Test
  public void testLocalHardFailureFactoryInvariant() {
    SearchResult<LogMessage> r = SearchResult.localHardFailure();
    assertThat(r.requestedSnapshots).isEqualTo(1);
    assertThat(r.fulfilledSnapshots).isEqualTo(0);
    assertThat(r.failedNodes).isEqualTo(1);
    assertThat(r.totalNodes).isEqualTo(1);
    assertThat(r.failedSnapshots()).isEqualTo(1);
  }

  /** Verifies the local soft-failure factory tuple used for shard accounting. */
  @Test
  public void testLocalSoftFailureFactoryInvariant() {
    SearchResult<LogMessage> r = SearchResult.localSoftFailure();
    assertThat(r.requestedSnapshots).isEqualTo(1);
    assertThat(r.fulfilledSnapshots).isEqualTo(0);
    assertThat(r.failedNodes).isEqualTo(0);
    assertThat(r.totalNodes).isEqualTo(0);
    assertThat(r.failedSnapshots()).isEqualTo(1);
  }

  /** Verifies the failed distributed-subrequest factory tuple for N assigned snapshots. */
  @Test
  public void testFailedDistributedSubrequestFactoryInvariant() {
    SearchResult<LogMessage> r = SearchResult.failedDistributedSubrequest(7);
    assertThat(r.requestedSnapshots).isEqualTo(7);
    assertThat(r.fulfilledSnapshots).isEqualTo(0);
    assertThat(r.failedNodes).isEqualTo(1);
    assertThat(r.totalNodes).isEqualTo(1);
    assertThat(r.failedSnapshots()).isEqualTo(7);
  }

  /** Verifies the missing-queryable-coverage factory tuple when no node was contacted. */
  @Test
  public void testMissingQueryableSnapshotCoverageFactoryInvariant() {
    SearchResult<LogMessage> r = SearchResult.missingQueryableSnapshotCoverage(4);
    assertThat(r.requestedSnapshots).isEqualTo(4);
    assertThat(r.fulfilledSnapshots).isEqualTo(0);
    assertThat(r.failedNodes).isEqualTo(0);
    assertThat(r.totalNodes).isEqualTo(0);
    assertThat(r.failedSnapshots()).isEqualTo(4);
  }

  @Test
  public void testSearchResultProtoRoundTripWithSiblingAggregations() throws Exception {
    OpenSearchAdapter openSearchAdapter = new OpenSearchAdapter(Map.of());
    OpenSearchAdapter.AggregationExecution aggregationExecution =
        openSearchAdapter.createAggregationExecution(
            createTwoAverageAggregations(),
            logStoreAndSearcherRule
                .logStore
                .getAstraSearcherManager()
                .getLuceneSearcherManager()
                .acquire(),
            null);
    InternalAggregations internalAggregations = aggregationExecution.finish();
    SearchResult<LogMessage> searchResult =
        searchResult(List.of(), 1, 0, 1, 1, 1, internalAggregations);

    AstraSearch.SearchResult protoSearchResult =
        SearchResultUtils.toSearchResultProto(searchResult);
    SearchResult<LogMessage> convertedSearchResult =
        SearchResultUtils.fromSearchResultProto(protoSearchResult);

    InternalAvg foo = (InternalAvg) convertedSearchResult.internalAggregations.get("foo");
    InternalAvg bar = (InternalAvg) convertedSearchResult.internalAggregations.get("bar");
    assertThat(foo).isNotNull();
    assertThat(bar).isNotNull();
    assertThat(foo.getName()).isEqualTo("foo");
    assertThat(bar.getName()).isEqualTo("bar");
  }

  @Test
  void testSearchResultProtoRoundTripPreservesHitSortValues() throws Exception {
    Tracing.newBuilder().build();
    LogMessage message = MessageUtil.makeMessage(1);
    List<HitSortValue> sortValues =
        Arrays.asList(
            HitSortValue.bytes(ByteString.copyFromUtf8("encoded-id")),
            HitSortValue.ipAddress(
                ByteString.copyFrom(InetAddressPoint.encode(InetAddresses.forString("10.0.0.1")))),
            HitSortValue.of(10),
            HitSortValue.of(20L),
            HitSortValue.of(25F),
            HitSortValue.of(30D),
            HitSortValue.of("forty"),
            HitSortValue.of(false),
            HitSortValue.of(null));
    SearchResult<LogMessage> searchResult =
        new SearchResult<>(
            List.of(new SearchResultHit<>(message, sortValues)), 1, 0, 1, 1, 1, null);

    AstraSearch.SearchResult protoSearchResult =
        SearchResultUtils.toSearchResultProto(searchResult);
    assertThat(protoSearchResult.getHits(0).getSortValues(0).hasBytesValue()).isTrue();
    assertThat(protoSearchResult.getHits(0).getSortValues(1).hasIpValue()).isTrue();
    assertThat(protoSearchResult.getHits(0).getSortValues(4).hasFloatValue()).isTrue();
    SearchResult<LogMessage> convertedSearchResult =
        SearchResultUtils.fromSearchResultProto(protoSearchResult);

    assertThat(convertedSearchResult.hits).hasSize(1);
    assertThat(convertedSearchResult.hits.get(0).message()).isEqualTo(message);
    assertThat(convertedSearchResult.hits.get(0).sortValues())
        .containsExactlyElementsOf(sortValues);
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
            .map(
                message ->
                    new SearchResultHit<>(
                        message, List.of(HitSortValue.of(message.getTimestamp().toEpochMilli()))))
            .toList(),
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas,
        internalAggregations);
  }
}
