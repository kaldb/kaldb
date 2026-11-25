package com.slack.astra.logstore.search;

import com.slack.astra.logstore.LogMessage;
import org.apache.lucene.store.AlreadyClosedException;

public class AlreadyClosedLogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  @Override
  public SearchResult<LogMessage> search(SearchQuery query) {
    throw new AlreadyClosedException("Failed to acquire an index searcher");
  }

  @Override
  public void close() {
    // do nothing
  }
}
