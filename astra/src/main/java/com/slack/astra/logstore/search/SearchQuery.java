package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import com.slack.astra.logstore.LogMessage;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  public enum SortDirection {
    ASC,
    DESC
  }

  public record SortFieldSpec(String field, SortDirection direction) {
    public SortFieldSpec {
      Objects.requireNonNull(field, "field");
      Objects.requireNonNull(direction, "direction");
    }

    /** Returns true when this field should be sorted descending. */
    public boolean descending() {
      return direction == SortDirection.DESC;
    }
  }

  private static final SortFieldSpec DEFAULT_SORT_FIELD =
      new SortFieldSpec(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, SortDirection.DESC);

  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final AggregatorFactories.Builder aggregatorFactoriesBuilder;
  public final QueryBuilder queryBuilder;
  public final int howMany;
  public final int startFrom;
  public final List<SortFieldSpec> sortFieldSpecs;
  public final List<String> chunkIds;
  public final SourceFieldFilter sourceFieldFilter;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;

  public SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    this(
        dataset,
        startTimeEpochMs,
        endTimeEpochMs,
        howMany,
        0,
        List.of(),
        chunkIds,
        queryBuilder,
        sourceFieldFilter,
        aggregatorFactoriesBuilder);
  }

  public SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      int startFrom,
      List<SortFieldSpec> sortFieldSpecs,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    this.dataset = dataset;
    this.howMany = howMany;
    this.startFrom = startFrom;
    this.sortFieldSpecs =
        sortFieldSpecs == null || sortFieldSpecs.isEmpty()
            ? List.of(DEFAULT_SORT_FIELD)
            : List.copyOf(sortFieldSpecs);
    this.chunkIds = chunkIds;
    this.queryBuilder = queryBuilder;
    this.sourceFieldFilter = sourceFieldFilter;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.aggregatorFactoriesBuilder = aggregatorFactoriesBuilder;

    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(startFrom >= 0, "from should not be negative.");
    ensureTrue(startFrom <= Integer.MAX_VALUE - howMany, "from plus size is too large.");
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

  /** Returns the comparator used to merge hits across chunks or nodes. */
  public Comparator<LogMessage> hitComparator() {
    return hitComparator(sortFieldSpecs);
  }

  /** Returns the comparator for the provided hit sort fields. */
  public static Comparator<LogMessage> hitComparator(List<SortFieldSpec> sortFieldSpecs) {
    List<SortFieldSpec> effectiveSortFields =
        sortFieldSpecs == null || sortFieldSpecs.isEmpty()
            ? List.of(DEFAULT_SORT_FIELD)
            : sortFieldSpecs;

    return (left, right) -> {
      for (SortFieldSpec sortFieldSpec : effectiveSortFields) {
        int comparison =
            compareSortValues(sortValue(left, sortFieldSpec), sortValue(right, sortFieldSpec));
        if (comparison != 0) {
          return sortFieldSpec.descending() ? -comparison : comparison;
        }
      }

      int timestampComparison = right.getTimestamp().compareTo(left.getTimestamp());
      if (timestampComparison != 0) {
        return timestampComparison;
      }
      return left.getId().compareTo(right.getId());
    };
  }

  /** Returns the response sort value for a hit and sort field. */
  public static Object sortValue(LogMessage message, SortFieldSpec sortFieldSpec) {
    if (LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName.equals(sortFieldSpec.field())) {
      return message.getTimestamp().toEpochMilli();
    }
    return message.getSource().get(sortFieldSpec.field());
  }

  @SuppressWarnings("unchecked")
  private static int compareSortValues(Object left, Object right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return 1;
    }
    if (right == null) {
      return -1;
    }
    if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
      if (left instanceof Float
          || left instanceof Double
          || right instanceof Float
          || right instanceof Double) {
        return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
      }
      return Long.compare(leftNumber.longValue(), rightNumber.longValue());
    }
    if (left instanceof Boolean leftBoolean && right instanceof Boolean rightBoolean) {
      return Boolean.compare(leftBoolean, rightBoolean);
    }
    if (left instanceof Comparable<?> && left.getClass().isInstance(right)) {
      return ((Comparable<Object>) left).compareTo(right);
    }
    return left.toString().compareTo(right.toString());
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
        + ", sortFieldSpecs="
        + sortFieldSpecs
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
