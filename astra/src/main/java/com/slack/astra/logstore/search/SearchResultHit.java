package com.slack.astra.logstore.search;

import java.util.List;
import java.util.Objects;

/** One returned search result row and the typed scalar sort values collected with it. */
public record SearchResultHit<T>(T message, List<HitSortValue> sortValues) {
  public SearchResultHit {
    Objects.requireNonNull(message, "message");
    Objects.requireNonNull(sortValues, "sortValues");
  }
}
