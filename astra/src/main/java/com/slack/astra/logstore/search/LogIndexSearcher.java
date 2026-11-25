package com.slack.astra.logstore.search;

import java.io.Closeable;

public interface LogIndexSearcher<T> extends Closeable {
  /**
   * Executes a search for the supplied query, applying the query's requested sort fields when
   * present and the deterministic internal ordering otherwise.
   */
  SearchResult<T> search(SearchQuery query);
}
