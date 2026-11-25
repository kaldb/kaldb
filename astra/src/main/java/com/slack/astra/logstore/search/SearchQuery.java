package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import com.slack.astra.logstore.LogMessage;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  enum SortDirection {
    ASC,
    DESC
  }

  record SortFieldSpec(String field, SortDirection direction) {
    SortFieldSpec {
      Objects.requireNonNull(field, "field");
      Objects.requireNonNull(direction, "direction");
    }

    /** Returns true when this field should be sorted in descending order. */
    boolean descending() {
      return direction == SortDirection.DESC;
    }
  }

  static final List<SortFieldSpec> INTERNAL_STABLE_SORT_FIELD_SPECS =
      List.of(
          new SortFieldSpec(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, SortDirection.DESC),
          new SortFieldSpec(LogMessage.SystemField.ID.fieldName, SortDirection.ASC));
  static final int MAX_RESULT_WINDOW = 10_000;

  /**
   * Separates the requested hit sort fields from the effective sort fields used internally for hit
   * ordering. Requested fields are exactly the parsed request sort fields; effective fields add the
   * internal stable ordering fields needed by Lucene leaf search and distributed merge.
   */
  record HitSortPlan(
      List<SortFieldSpec> requestedSortFields, List<SortFieldSpec> effectiveSortFields) {
    HitSortPlan {
      Objects.requireNonNull(requestedSortFields, "requestedSortFields");
      Objects.requireNonNull(effectiveSortFields, "effectiveSortFields");
    }

    /** Builds the internal hit sort plan from request sort fields. Empty means omitted sort. */
    static HitSortPlan fromRequested(List<SortFieldSpec> requestedSortFieldSpecs) {
      Objects.requireNonNull(requestedSortFieldSpecs, "requestedSortFieldSpecs");
      List<SortFieldSpec> effectiveSortFields =
          new ArrayList<>(requestedSortFieldSpecs.size() + INTERNAL_STABLE_SORT_FIELD_SPECS.size());
      Set<String> effectiveSortFieldNames = new HashSet<>();
      for (SortFieldSpec requestedSortField : requestedSortFieldSpecs) {
        effectiveSortFields.add(requestedSortField);
        effectiveSortFieldNames.add(requestedSortField.field());
      }
      for (SortFieldSpec stableSortField : INTERNAL_STABLE_SORT_FIELD_SPECS) {
        if (effectiveSortFieldNames.contains(stableSortField.field())) {
          continue;
        }
        effectiveSortFields.add(stableSortField);
        effectiveSortFieldNames.add(stableSortField.field());
      }
      return new HitSortPlan(requestedSortFieldSpecs, effectiveSortFields);
    }
  }

  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final AggregatorFactories.Builder aggregatorFactoriesBuilder;
  public final QueryBuilder queryBuilder;
  public final int howMany;
  public final int startFrom;
  final List<SortFieldSpec> requestedSortFieldSpecs;
  final HitSortPlan hitSortPlan;
  public final List<String> chunkIds;
  public final SourceFieldFilter sourceFieldFilter;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;

  /** Creates a search query that uses KalDB's default hit ordering. */
  public SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      int startFrom,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    this(
        dataset,
        startTimeEpochMs,
        endTimeEpochMs,
        howMany,
        startFrom,
        List.of(),
        chunkIds,
        queryBuilder,
        sourceFieldFilter,
        aggregatorFactoriesBuilder);
  }

  SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      int startFrom,
      List<SortFieldSpec> requestedSortFieldSpecs,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    this.dataset = dataset;
    this.howMany = howMany;
    this.startFrom = startFrom;
    this.hitSortPlan = HitSortPlan.fromRequested(requestedSortFieldSpecs);
    this.requestedSortFieldSpecs = hitSortPlan.requestedSortFields();
    this.chunkIds = chunkIds;
    this.queryBuilder = queryBuilder;
    this.sourceFieldFilter = sourceFieldFilter;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.aggregatorFactoriesBuilder = aggregatorFactoriesBuilder;

    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(startFrom >= 0, "from should not be negative.");
    ensureTrue(startFrom <= Integer.MAX_VALUE - howMany, "from plus size is too large.");
    ensureTrue(
        startFrom + howMany <= MAX_RESULT_WINDOW,
        "from plus size must be less than or equal to " + MAX_RESULT_WINDOW + ".");
    // Reject unsupported no-op requests before they fan out to every chunk.
    ensureTrue(
        howMany > 0 || aggregatorFactoriesBuilder != null,
        "Hits or aggregation should be requested.");
  }

  /** Returns the number of leaf hits needed before the final global offset is applied. */
  public int leafHowMany() {
    if (howMany == 0) {
      return 0;
    }
    return startFrom + howMany;
  }

  @Override
  public String toString() {
    return "SearchQuery{"
        + "dataset='"
        + dataset
        + '\''
        + ", howMany="
        + howMany
        + ", startFrom="
        + startFrom
        + ", requestedSortFieldSpecs="
        + requestedSortFieldSpecs
        + ", chunkIds="
        + chunkIds
        + ", queryBuilder="
        + queryBuilder
        + ", sourceFieldFilter="
        + sourceFieldFilter
        + ", aggregatorFactoriesBuilder="
        + aggregatorFactoriesBuilder
        + '}';
  }
}
