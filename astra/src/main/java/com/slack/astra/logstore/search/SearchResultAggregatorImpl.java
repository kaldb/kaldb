package com.slack.astra.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.opensearch.AstraBigArrays;
import com.slack.astra.logstore.opensearch.ScriptServiceProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;

/**
 * This class will merge multiple search results into a single search result. It returns the topK
 * hits according to the query sort (or timestamp descending when no sort is provided). The
 * histogram will be merged using the histogram merge function.
 */
public class SearchResultAggregatorImpl<T extends LogMessage> implements SearchResultAggregator<T> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Set<String> LONG_SORT_FIELDS =
      Set.of(
          LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
          "@timestamp",
          "dropoff_datetime",
          "pickup_datetime");
  private static final SortSpec DEFAULT_SORT_SPEC =
      new SortSpec(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, SortValueType.LONG, true);

  private final SearchQuery searchQuery;

  public SearchResultAggregatorImpl(SearchQuery searchQuery) {
    this.searchQuery = searchQuery;
  }

  @Override
  public SearchResult<T> aggregate(List<SearchResult<T>> searchResults, boolean finalAggregation) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultAggregatorImpl.aggregate");
    long tookMicros = 0;
    int failedNodes = 0;
    int totalNodes = 0;
    int totalSnapshots = 0;
    int snapshpotReplicas = 0;
    List<InternalAggregation> internalAggregationList = new ArrayList<>();

    for (SearchResult<T> searchResult : searchResults) {
      tookMicros = Math.max(tookMicros, searchResult.tookMicros);
      failedNodes += searchResult.failedNodes;
      totalNodes += searchResult.totalNodes;
      totalSnapshots += searchResult.totalSnapshots;
      snapshpotReplicas += searchResult.snapshotsWithReplicas;
      if (searchResult.internalAggregation != null) {
        internalAggregationList.add(searchResult.internalAggregation);
      }
    }

    InternalAggregation internalAggregation = null;
    if (internalAggregationList.size() > 0) {
      InternalAggregation.ReduceContext reduceContext;
      PipelineAggregator.PipelineTree pipelineTree = null;
      // The last aggregation should be indicated using the final aggregation boolean. This performs
      // some final pass "destructive" actions, such as applying min doc count or extended bounds.
      if (finalAggregation) {
        if (searchQuery.aggregatorFactoriesBuilder != null) {
          Collection<AggregationBuilder> aggregationBuilders =
              searchQuery.aggregatorFactoriesBuilder.getAggregatorFactories();
          pipelineTree = aggregationBuilders.iterator().next().buildPipelineTree();
        }

        reduceContext =
            InternalAggregation.ReduceContext.forFinalReduction(
                AstraBigArrays.getInstance(),
                ScriptServiceProvider.getInstance(),
                (s) -> {},
                pipelineTree);
      } else {
        reduceContext =
            InternalAggregation.ReduceContext.forPartialReduction(
                AstraBigArrays.getInstance(),
                ScriptServiceProvider.getInstance(),
                () -> PipelineAggregator.PipelineTree.EMPTY);
      }
      // Using the first element on the list as the basis for the reduce method is per OpenSearch
      // recommendations: "For best efficiency, when implementing, try reusing an existing instance
      // (typically the first in the given list) to save on redundant object construction."
      internalAggregation =
          internalAggregationList.get(0).reduce(internalAggregationList, reduceContext);

      if (finalAggregation) {
        // materialize any parent pipelines
        internalAggregation =
            internalAggregation.reducePipelines(internalAggregation, reduceContext, pipelineTree);
        // materialize any sibling pipelines at top level
        for (PipelineAggregator pipelineAggregator : pipelineTree.aggregators()) {
          internalAggregation = pipelineAggregator.reduce(internalAggregation, reduceContext);
        }
      }
    }

    List<T> resultHits = mergeTopHits(searchResults);

    span.tag("resultHits", String.valueOf(resultHits.size()));
    span.tag("finalAggregation", String.valueOf(finalAggregation));
    span.finish();

    return new SearchResult<>(
        resultHits,
        tookMicros,
        failedNodes,
        totalNodes,
        totalSnapshots,
        snapshpotReplicas,
        internalAggregation);
  }

  private List<T> mergeTopHits(List<SearchResult<T>> searchResults) {
    if (searchQuery.howMany <= 0) {
      return Collections.emptyList();
    }

    Comparator<T> hitComparator = buildHitComparator();
    PriorityQueue<SearchResultCursor<T>> queue =
        new PriorityQueue<>(
            (left, right) -> {
              int comparison = hitComparator.compare(left.currentHit(), right.currentHit());
              if (comparison != 0) {
                return comparison;
              }
              comparison = Integer.compare(left.searchResultIndex(), right.searchResultIndex());
              if (comparison != 0) {
                return comparison;
              }
              return Integer.compare(left.hitIndex(), right.hitIndex());
            });

    for (int searchResultIndex = 0; searchResultIndex < searchResults.size(); searchResultIndex++) {
      List<T> hits = ensureSortedHits(searchResults.get(searchResultIndex).hits, hitComparator);
      if (!hits.isEmpty()) {
        queue.add(new SearchResultCursor<>(hits, searchResultIndex, 0));
      }
    }

    List<T> mergedHits = new ArrayList<>(searchQuery.howMany);
    while (!queue.isEmpty() && mergedHits.size() < searchQuery.howMany) {
      SearchResultCursor<T> cursor = queue.poll();
      mergedHits.add(cursor.currentHit());
      if (cursor.hasNext()) {
        queue.add(cursor.next());
      }
    }
    return mergedHits;
  }

  private List<T> ensureSortedHits(List<T> hits, Comparator<T> hitComparator) {
    for (int i = 1; i < hits.size(); i++) {
      if (hitComparator.compare(hits.get(i - 1), hits.get(i)) > 0) {
        List<T> sortedHits = new ArrayList<>(hits);
        sortedHits.sort(hitComparator);
        return sortedHits;
      }
    }
    return hits;
  }

  private Comparator<T> buildHitComparator() {
    SortSpec sortSpec = parseSortSpec(searchQuery.sortJson);
    return (left, right) -> {
      int comparison =
          switch (sortSpec.valueType()) {
            case LONG ->
                compareNullableLongs(
                    coerceLong(getSortFieldValue(left, sortSpec.fieldName())),
                    coerceLong(getSortFieldValue(right, sortSpec.fieldName())));
            case DOUBLE ->
                compareNullableDoubles(
                    coerceDouble(getSortFieldValue(left, sortSpec.fieldName())),
                    coerceDouble(getSortFieldValue(right, sortSpec.fieldName())));
          };
      return sortSpec.reverse() ? -comparison : comparison;
    };
  }

  private Object getSortFieldValue(T message, String fieldName) {
    if (LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName.equals(fieldName)
        || "@timestamp".equals(fieldName)) {
      return message.getTimestamp();
    }
    return message.getSource().get(fieldName);
  }

  private SortSpec parseSortSpec(String sortJson) {
    if (sortJson == null || sortJson.isEmpty()) {
      return DEFAULT_SORT_SPEC;
    }
    try {
      JsonNode sortArray = OBJECT_MAPPER.readTree(sortJson);
      if (sortArray.isArray() && !sortArray.isEmpty()) {
        JsonNode firstSort = sortArray.get(0);
        if (firstSort.isObject()) {
          String fieldName = firstSort.fieldNames().next();
          boolean reverse = true;
          JsonNode sortNode = firstSort.get(fieldName);
          if (sortNode.isTextual()) {
            reverse = !"asc".equalsIgnoreCase(sortNode.asText());
          } else if (sortNode.isObject() && sortNode.has("order")) {
            reverse = !"asc".equalsIgnoreCase(sortNode.get("order").asText());
          }
          return new SortSpec(
              fieldName,
              LONG_SORT_FIELDS.contains(fieldName) ? SortValueType.LONG : SortValueType.DOUBLE,
              reverse);
        }
      }
    } catch (Exception e) {
      // Fall back to the default timestamp-desc sort if parsing fails.
    }
    return DEFAULT_SORT_SPEC;
  }

  private Long coerceLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof java.time.Instant instant) {
      return instant.toEpochMilli();
    }
    if (value instanceof String stringValue) {
      try {
        return Long.valueOf(stringValue);
      } catch (NumberFormatException ignored) {
        try {
          return java.time.Instant.parse(stringValue).toEpochMilli();
        } catch (Exception ignoredAgain) {
          return null;
        }
      }
    }
    return null;
  }

  private Double coerceDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    if (value instanceof String stringValue) {
      try {
        return Double.valueOf(stringValue);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private int compareNullableLongs(Long left, Long right) {
    if (left == null && right == null) {
      return 0;
    }
    if (left == null) {
      return 1;
    }
    if (right == null) {
      return -1;
    }
    return Long.compare(left, right);
  }

  private int compareNullableDoubles(Double left, Double right) {
    if (left == null && right == null) {
      return 0;
    }
    if (left == null) {
      return 1;
    }
    if (right == null) {
      return -1;
    }
    return Double.compare(left, right);
  }

  private enum SortValueType {
    LONG,
    DOUBLE
  }

  private record SortSpec(String fieldName, SortValueType valueType, boolean reverse) {}

  private record SearchResultCursor<T extends LogMessage>(
      List<T> hits, int searchResultIndex, int hitIndex) {
    private T currentHit() {
      return hits.get(hitIndex);
    }

    private boolean hasNext() {
      return hitIndex + 1 < hits.size();
    }

    private SearchResultCursor<T> next() {
      return new SearchResultCursor<>(hits, searchResultIndex, hitIndex + 1);
    }
  }
}
