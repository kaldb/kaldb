package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import java.util.List;
import java.util.Objects;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  /**
   * Controls whether result metadata should include OpenSearch-compatible hits.total metadata and
   * what accuracy/cost policy should be used.
   */
  public sealed interface TotalHitsPolicy
      permits TotalHitsPolicy.Disabled, TotalHitsPolicy.Exact, TotalHitsPolicy.Threshold {
    int DEFAULT_THRESHOLD = 10_000;
    int EXACT_THRESHOLD = Integer.MAX_VALUE;

    /** Returns whether total-hit tracking is enabled. */
    boolean enabled();

    /** Returns the Lucene total-hit threshold for this policy. */
    int threshold();

    /** Total-hit tracking is disabled and response metadata should omit hits.total. */
    record Disabled() implements TotalHitsPolicy {
      @Override
      public boolean enabled() {
        return false;
      }

      @Override
      public int threshold() {
        return 0;
      }
    }

    /** Total-hit tracking should count every matching document exactly. */
    record Exact() implements TotalHitsPolicy {
      @Override
      public boolean enabled() {
        return true;
      }

      @Override
      public int threshold() {
        return EXACT_THRESHOLD;
      }
    }

    /** Total-hit tracking should count accurately up to the provided threshold. */
    record Threshold(int threshold) implements TotalHitsPolicy {
      public Threshold {
        ensureTrue(threshold >= 0, "total hits threshold should not be negative.");
      }

      @Override
      public boolean enabled() {
        return true;
      }
    }

    /** Returns the OpenSearch default total-hit counting policy. */
    public static TotalHitsPolicy defaultPolicy() {
      return threshold(DEFAULT_THRESHOLD);
    }

    /** Returns a policy that asks Lucene to count every matching document exactly. */
    public static TotalHitsPolicy exact() {
      return new Exact();
    }

    /** Returns a policy that disables hits.total tracking in the compatibility response. */
    public static TotalHitsPolicy disabled() {
      return new Disabled();
    }

    /** Returns a policy that counts accurately up to the provided threshold. */
    public static TotalHitsPolicy threshold(int threshold) {
      return new Threshold(threshold);
    }

    /** Returns whether this policy requests exact total-hit counting. */
    default boolean isExact() {
      return this instanceof Exact;
    }
  }

  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final AggregatorFactories.Builder aggregatorFactoriesBuilder;
  public final QueryBuilder queryBuilder;
  public final int howMany;
  public final List<String> chunkIds;
  public final SourceFieldFilter sourceFieldFilter;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final TotalHitsPolicy totalHitsPolicy;

  public SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      TotalHitsPolicy totalHitsPolicy) {
    this.dataset = dataset;
    this.howMany = howMany;
    this.chunkIds = chunkIds;
    this.queryBuilder = queryBuilder;
    this.sourceFieldFilter = sourceFieldFilter;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.aggregatorFactoriesBuilder = aggregatorFactoriesBuilder;
    this.totalHitsPolicy = Objects.requireNonNull(totalHitsPolicy, "totalHitsPolicy");

    ensureTrue(howMany >= 0, "hits requested should not be negative.");
  }

  @Override
  public String toString() {
    return "SearchQuery{"
        + "dataset='"
        + dataset
        + '\''
        + ", howMany="
        + howMany
        + ", chunkIds="
        + chunkIds
        + ", queryBuilder="
        + queryBuilder
        + ", sourceFieldFilter="
        + sourceFieldFilter
        + ", totalHitsPolicy="
        + totalHitsPolicy
        + ", aggregatorFactoriesBuilder="
        + aggregatorFactoriesBuilder
        + '}';
  }
}
