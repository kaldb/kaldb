package com.slack.astra.logstore.search;

import java.io.Closeable;
import java.util.List;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

public interface LogIndexSearcher<T> extends Closeable {
  /** Searches with default hit sort and Astra's default total-hit counting policy. */
  default SearchResult<T> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    return search(
        dataset,
        howMany,
        List.of(),
        queryBuilder,
        sourceFieldFilter,
        aggregatorFactoriesBuilder,
        SearchQuery.TotalHitsPolicy.defaultPolicy());
  }

  /** Searches with default hit sort and the provided total-hit counting policy. */
  default SearchResult<T> search(
      String dataset,
      int howMany,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      SearchQuery.TotalHitsPolicy totalHitsPolicy) {
    return search(
        dataset,
        howMany,
        List.of(),
        queryBuilder,
        sourceFieldFilter,
        aggregatorFactoriesBuilder,
        totalHitsPolicy);
  }

  /** Searches using the provided top-level hit sort fields. */
  default SearchResult<T> search(
      String dataset,
      int howMany,
      List<SearchQuery.SortFieldSpec> sortFieldSpecs,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    return search(
        dataset,
        howMany,
        sortFieldSpecs,
        queryBuilder,
        sourceFieldFilter,
        aggregatorFactoriesBuilder,
        SearchQuery.TotalHitsPolicy.defaultPolicy());
  }

  /**
   * Searches a single index and returns matching hits, aggregations, and total-hit metadata
   * according to the provided policy.
   */
  SearchResult<T> search(
      String dataset,
      int howMany,
      List<SearchQuery.SortFieldSpec> sortFieldSpecs,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      SearchQuery.TotalHitsPolicy totalHitsPolicy);
}
