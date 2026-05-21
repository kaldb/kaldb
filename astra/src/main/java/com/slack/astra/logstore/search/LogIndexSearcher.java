package com.slack.astra.logstore.search;

import java.io.Closeable;
import java.util.List;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

public interface LogIndexSearcher<T> extends Closeable {
  /** Searches using the default descending timestamp hit sort. */
  default SearchResult<T> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    return search(
        dataset, howMany, List.of(), queryBuilder, sourceFieldFilter, aggregatorFactoriesBuilder);
  }

  /** Searches using the provided top-level hit sort fields. */
  SearchResult<T> search(
      String dataset,
      int howMany,
      List<SearchQuery.SortFieldSpec> sortFieldSpecs,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder);
}
