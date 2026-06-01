package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.aggregations.InternalAggregations;

public class SearchResult<T> {
  /** Describes whether totalHits is an exact value or an OpenSearch-compatible lower bound. */
  public enum TotalHitsRelation {
    EQUAL_TO("eq"),
    GREATER_THAN_OR_EQUAL_TO("gte");

    /** OpenSearch response value for hits.total.relation. */
    public final String openSearchName;

    TotalHitsRelation(String openSearchName) {
      this.openSearchName = openSearchName;
    }
  }

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, 1, 0, 0, null);

  // Astra problem (instead of a user-caused issue)
  private static final SearchResult ASTRA_ERROR =
      new SearchResult<>(Collections.emptyList(), 0, 1, 1, 0, 0, null);

  private static final SearchResult USER_ERROR =
      new SearchResult<>(Collections.emptyList(), 0, 0, 0, 1, 0, null);

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  public final int failedNodes;
  public final int totalNodes;
  public final int totalSnapshots;
  public final int snapshotsWithReplicas;

  public final long totalHits;
  public final TotalHitsRelation totalHitsRelation;

  public final InternalAggregations internalAggregations;

  /** Creates a search result that carries the full top-level OpenSearch aggregation collection. */
  public SearchResult(
      List<T> hits,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      InternalAggregations internalAggregations) {
    this(
        hits,
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshotsWithReplicas,
        hits.size(),
        TotalHitsRelation.EQUAL_TO,
        internalAggregations);
  }

  /** Creates a search result with explicit OpenSearch hits.total metadata. */
  public SearchResult(
      List<T> hits,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      long totalHits,
      TotalHitsRelation totalHitsRelation,
      InternalAggregations internalAggregations) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.totalSnapshots = totalSnapshots;
    this.snapshotsWithReplicas = snapshotsWithReplicas;
    this.totalHits = totalHits;
    this.totalHitsRelation = totalHitsRelation;
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
        + ", totalSnapshots="
        + totalSnapshots
        + ", snapshotsWithReplicas="
        + snapshotsWithReplicas
        + ", totalHits="
        + totalHits
        + ", totalHitsRelation="
        + totalHitsRelation
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
    if (totalSnapshots != that.totalSnapshots) return false;
    if (snapshotsWithReplicas != that.snapshotsWithReplicas) return false;
    if (totalHits != that.totalHits) return false;
    if (totalHitsRelation != that.totalHitsRelation) return false;
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
        totalSnapshots,
        snapshotsWithReplicas,
        totalHits,
        totalHitsRelation,
        aggregationString(internalAggregations));
  }

  public static SearchResult<LogMessage> empty() {
    return EMPTY;
  }

  public static SearchResult<LogMessage> error() {
    return ASTRA_ERROR;
  }

  public static SearchResult<LogMessage> soft_error() {
    return USER_ERROR;
  }

  private static String aggregationString(InternalAggregations internalAggregations) {
    return internalAggregations == null
        ? null
        : Strings.toString(MediaTypeRegistry.JSON, internalAggregations);
  }
}
