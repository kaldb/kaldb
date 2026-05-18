package com.slack.astra.logstore.search;

import java.io.Closeable;

public interface LogIndexSearcher<T> extends Closeable {
  SearchResult<T> search(SearchQuery query);
}
