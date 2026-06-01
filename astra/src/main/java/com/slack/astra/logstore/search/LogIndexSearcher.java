package com.slack.astra.logstore.search;

import java.io.Closeable;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

public interface LogIndexSearcher<T> extends Closeable {
  /**
   * Searches a single index and returns matching hits, aggregations, and total-hit metadata
   * according to the provided policy.
   */
  SearchResult<T> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      SearchQuery.TotalHitsPolicy totalHitsPolicy);
}
