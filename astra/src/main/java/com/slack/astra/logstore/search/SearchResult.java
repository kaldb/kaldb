package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import com.slack.astra.logstore.LogMessage;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.aggregations.InternalAggregations;

public class SearchResult<T> {
  /** OpenSearch-compatible total-hit metadata. */
  public sealed interface TotalHits
      permits TotalHits.Untracked, TotalHits.EqualTo, TotalHits.GreaterThanOrEqualTo {
    /** Returns total-hit metadata for a search that did not track totals. */
    static TotalHits untracked() {
      return new Untracked();
    }

    /** Returns exact total-hit metadata. */
    static TotalHits equalTo(long value) {
      return new EqualTo(value);
    }

    /** Returns lower-bound total-hit metadata. */
    static TotalHits greaterThanOrEqualTo(long value) {
      return new GreaterThanOrEqualTo(value);
    }

    /** Total-hit tracking was disabled or unavailable. */
    record Untracked() implements TotalHits {}

    /** The value is the exact matched-document count. */
    record EqualTo(long value) implements TotalHits {
      public EqualTo {
        ensureTrue(value >= 0, "total hits should not be negative.");
      }
    }

    /** The value is a lower bound for the matched-document count. */
    record GreaterThanOrEqualTo(long value) implements TotalHits {
      public GreaterThanOrEqualTo {
        ensureTrue(value >= 0, "total hits should not be negative.");
      }
    }
  }

  private static final SearchResult EMPTY =
      new SearchResult<>(Collections.emptyList(), 0, 0, 1, 0, 0, TotalHits.equalTo(0), null);

  // Astra problem (instead of a user-caused issue)
  private static final SearchResult ASTRA_ERROR =
      new SearchResult<>(Collections.emptyList(), 0, 1, 1, 0, 0, TotalHits.equalTo(0), null);

  private static final SearchResult USER_ERROR =
      new SearchResult<>(Collections.emptyList(), 0, 0, 0, 1, 0, TotalHits.equalTo(0), null);

  // TODO: Make hits an iterator.
  // An iterator helps with the early termination of a search and may be efficient in some cases.
  public final List<T> hits;
  public final long tookMicros;

  public final int failedNodes;
  public final int totalNodes;
  public final int totalSnapshots;
  public final int snapshotsWithReplicas;

  public final TotalHits totalHits;

  public final InternalAggregations internalAggregations;

  /** Creates a search result with explicit OpenSearch hits.total metadata. */
  public SearchResult(
      List<T> hits,
      long tookMicros,
      int failedNodes,
      int totalNodes,
      int totalSnapshots,
      int snapshotsWithReplicas,
      TotalHits totalHits,
      InternalAggregations internalAggregations) {
    this.hits = hits;
    this.tookMicros = tookMicros;
    this.failedNodes = failedNodes;
    this.totalNodes = totalNodes;
    this.totalSnapshots = totalSnapshots;
    this.snapshotsWithReplicas = snapshotsWithReplicas;
    this.totalHits = Objects.requireNonNull(totalHits, "totalHits");
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
    if (!totalHits.equals(that.totalHits)) return false;
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
