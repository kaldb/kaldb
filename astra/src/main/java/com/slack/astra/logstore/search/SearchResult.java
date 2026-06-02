package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.aggregations.InternalAggregations;

public class SearchResult<T> {

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, 1, 0, 0, null);

  private static final SearchResult LOCAL_HARD_FAILURE =
      new SearchResult<>(Collections.emptyList(), 0, 1, 1, 1, 0, null);

  private static final SearchResult LOCAL_SOFT_FAILURE =
      new SearchResult<>(Collections.emptyList(), 0, 0, 0, 1, 0, null);

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  public final int failedNodes;
  public final int totalNodes;
  // Coverage is represented as requested vs fulfilled logical snapshots.
  // Failed coverage is derived as requestedSnapshots - fulfilledSnapshots.
  public final int requestedSnapshots;
  public final int fulfilledSnapshots;

  public final InternalAggregations internalAggregations;

  /** Creates a search result that carries the full top-level OpenSearch aggregation collection. */
  public SearchResult(
      List<T> hits,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int requestedSnapshots,
      int fulfilledSnapshots,
      InternalAggregations internalAggregations) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.requestedSnapshots = requestedSnapshots;
    this.fulfilledSnapshots = fulfilledSnapshots;
    this.internalAggregations = internalAggregations;
  }

  @Override
  public String toString() {
    return "SearchResult{"
        + "hits="
        + hits
        + ", tookMicros="
        + tookMicros
        + ", failedNodes="
        + failedNodes
        + ", totalNodes="
        + totalNodes
        + ", requestedSnapshots="
        + requestedSnapshots
        + ", fulfilledSnapshots="
        + fulfilledSnapshots
        + ", internalAggregations="
        + aggregationString(internalAggregations)
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SearchResult<?> that = (SearchResult<?>) o;

    if (tookMicros != that.tookMicros) return false;
    if (failedNodes != that.failedNodes) return false;
    if (totalNodes != that.totalNodes) return false;
    if (requestedSnapshots != that.requestedSnapshots) return false;
    if (fulfilledSnapshots != that.fulfilledSnapshots) return false;
    if (!hits.equals(that.hits)) return false;

    // todo - this is pending a PR to OpenSearch to address
    // https://github.com/opensearch-project/OpenSearch/pull/6357
    // this is because DocValueFormat.DateTime in OpenSearch does not implement a proper equals
    // method
    // As such the DocValueFormat.parser are never equal to each other
    return Objects.equals(
        aggregationString(internalAggregations), aggregationString(that.internalAggregations));
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        hits,
        tookMicros,
        failedNodes,
        totalNodes,
        requestedSnapshots,
        fulfilledSnapshots,
        aggregationString(internalAggregations));
  }

  public static SearchResult<LogMessage> empty() {
    return EMPTY;
  }

  /**
   * Logical shard coverage that was requested but not fulfilled. Drives OpenSearch _shards.failed.
   */
  public int failedSnapshots() {
    return Math.max(0, requestedSnapshots - fulfilledSnapshots);
  }

  /** Same invariant as {@link #failedSnapshots()}, on the proto wire form. */
  public static int failedSnapshots(
      com.slack.astra.proto.service.AstraSearch.SearchResult protoSearchResult) {
    return Math.max(
        0, protoSearchResult.getRequestedSnapshots() - protoSearchResult.getFulfilledSnapshots());
  }

  // Catalog of "missing coverage" SearchResults. Each case fixes the
  // (requested, fulfilled, failedNodes, totalNodes) tuple:
  //   localHardFailure              : 1 / 0, 1 / 1   (Astra-side chunk failure)
  //   localSoftFailure              : 1 / 0, 0 / 0   (user-attributable chunk failure)
  //   failedDistributedSubrequest(N): N / 0, 1 / 1   (a remote node was contacted and failed)
  //   missingQueryableSnapshotCoverage(N): N / 0, 0 / 0 (no node was contacted at all)

  /**
   * Astra-side chunk failure after a node was contacted. Tuple: requested=1, fulfilled=0,
   * failedNodes=1, totalNodes=1.
   */
  public static SearchResult<LogMessage> localHardFailure() {
    return LOCAL_HARD_FAILURE;
  }

  /**
   * User-attributable chunk failure that does not count against node health. Tuple: requested=1,
   * fulfilled=0, failedNodes=0, totalNodes=0.
   */
  public static SearchResult<LogMessage> localSoftFailure() {
    return LOCAL_SOFT_FAILURE;
  }

  /**
   * A distributed subrequest covering {@code requestedSnapshots} logical snapshots failed. Tuple:
   * requested=N, fulfilled=0, failedNodes=1, totalNodes=1.
   *
   * @param requestedSnapshots logical snapshot count the failed subrequest was responsible for
   */
  public static SearchResult<LogMessage> failedDistributedSubrequest(int requestedSnapshots) {
    return new SearchResult<>(Collections.emptyList(), 0, 1, 1, requestedSnapshots, 0, null);
  }

  /**
   * Matching logical snapshots had no queryable SearchMetadata, so no node was contacted. Tuple:
   * requested=N, fulfilled=0, failedNodes=0, totalNodes=0.
   *
   * @param requestedSnapshots logical snapshot count missing queryable coverage
   */
  public static SearchResult<LogMessage> missingQueryableSnapshotCoverage(int requestedSnapshots) {
    return new SearchResult<>(Collections.emptyList(), 0, 0, 0, requestedSnapshots, 0, null);
  }

  private static String aggregationString(InternalAggregations internalAggregations) {
    return internalAggregations == null
        ? null
        : Strings.toString(MediaTypeRegistry.JSON, internalAggregations);
  }
}
