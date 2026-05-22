# ADR 0005: Support OpenSearch Hits Total And Track Total Hits

## Status

Current state: `Accepted`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

Before this change, KalDB's OpenSearch-compatible response built
`hits.total.value` from the number of hits returned in the current page. That
is not the OpenSearch contract.

`size` controls how many hit documents are returned in `hits.hits`. It does not
control how many documents matched the query. For example, a request with
`size: 0` should return no hit documents, but `hits.total` should still report
the number of matching documents, subject to `track_total_hits`.

That behavior made count-only, aggregation-only, and paginated searches look
like they matched fewer documents than they actually matched. The returned
documents and aggregation values could be correct while the response metadata
was still wrong.

## Questions

- Question: What should `hits.total.value` mean?
  Answer: It should mean the number of documents that matched the query, not the
  number of hit documents returned in the current page.

- Question: Should `size: 0` force `hits.total.value` to `0`?
  Answer: No. `size: 0` means "return no hit documents." It does not mean
  "match zero documents."

- Question: Should aggregation queries be treated differently?
  Answer: The response contract is the same, but the implementation cost is
  different. Exact aggregations already visit matching documents, so counting
  those matched documents is usually cheap relative to the aggregation work.

- Question: Should plain top-N hit queries always compute exact totals?
  Answer: No. Exact totals can defeat Lucene pruning on broad searches. KalDB
  should support OpenSearch-style `track_total_hits` semantics so callers can
  trade accuracy for cost.

- Question: What should KalDB do by default?
  Answer: Match OpenSearch's default behavior: count accurately up to a
  threshold of 10,000 and return `relation: "gte"` when the threshold is
  exceeded.

## Public Interfaces

- OpenSearch-compatible search responses include:

```json
{
  "hits": {
    "total": {
      "value": 123,
      "relation": "eq"
    }
  }
}
```

- `hits.total.value` is the matched-document count or thresholded lower bound.
- `hits.total.relation` is `eq` when the value is exact and `gte` when it is a
  lower bound.
- OpenSearch-compatible search requests may include `track_total_hits`.
- `track_total_hits: true` requests an exact count.
- `track_total_hits: false` disables total-hit tracking for the compatibility
  response. KalDB omits `hits.total` in that case.
- `track_total_hits: <non-negative integer>` requests exact counting up to that
  threshold.
- If `track_total_hits` is omitted, KalDB uses OpenSearch's default threshold of
  10,000.
- The internal request protobuf carries total-hit tracking as one explicit
  policy: disabled, exact, or threshold.
- The internal search result representation carries one explicit total-hit
  outcome: untracked, exact value, or lower-bound value.

## Proposed Changes

### Summary

Carry total-hit information from Lucene through KalDB's internal search result
path and serialize it into the OpenSearch-compatible response. Do not derive
`hits.total` from the number of returned hits.

### Detailed Design

`SearchQuery` should carry the requested total-hit tracking policy:

- disabled;
- exact;
- threshold with a non-negative threshold value.

`SearchResult` should carry the observed total-hit outcome:

- untracked, when totals were disabled or unavailable;
- exact value;
- lower-bound value.

The internal `AstraSearch.SearchRequest` protobuf should carry the request
policy without relying on nullable fields or side-channel sentinel values. The
internal `AstraSearch.SearchResult` protobuf should carry exact and lower-bound
result values as distinct alternatives. If result totals are untracked, the
protobuf omits `total_hits`, and the OpenSearch-compatible response omits
`hits.total`.

`LogIndexSearcherImpl` should capture total-hit information from the Lucene
search path:

- For top-N hit searches, use the `TopDocs.totalHits` value and relation from
  Lucene's top-hit collector.
- For `size: 0` count-only searches without aggregations, use Lucene's count
  path where possible.
- For aggregation searches, collect total hits in the same document visitation
  pass that computes the aggregation.

`SearchResultAggregatorImpl` should merge total-hit values across partial
results:

- sum the per-result values;
- return untracked if tracking is disabled or any contributing result is
  untracked;
- for exact tracking, return `relation: "eq"` only when every contributing
  result is exact, otherwise return `relation: "gte"` with the summed lower
  bound;
- for thresholded tracking, apply the threshold after summing partial results.
  If the summed value exceeds the threshold, return `relation: "gte"` with the
  threshold value even if every partial result was exact. Otherwise, return
  `relation: "eq"` only when every contributing result is exact.

`ElasticsearchApiService` should serialize the reduced total-hit value and
relation into the final OpenSearch response. It should not use
`responseHits.size()` for `hits.total`.

### Cost Model

Aggregation queries and plain top-N hit queries have different costs.

For aggregation queries, the aggregation already visits matching documents to
compute exact bucket or metric values. Counting matched documents in that same
pass is cheap relative to the work already required.

For plain top-N hit queries, exact total counting can be expensive because
Lucene may otherwise skip documents that cannot enter the top-N result set.
Exact counting requires knowing every match. Thresholded `track_total_hits`
allows KalDB to count accurately up to a configured threshold and then preserve
pruning for the rest of the search.

## Rejected Alternatives

- Keep deriving `hits.total` from returned hit count.
  Rejected because it is not OpenSearch-compatible and breaks `size: 0` and
  paginated queries.

- Always compute exact totals for every search.
  Rejected because broad top-N hit queries can become substantially more
  expensive when exact counting disables useful Lucene pruning.

## Consequences

Benefits:

- `size: 0` responses report correct match counts.
- Paginated hit responses report total matches instead of page length.
- Dashboards and clients that use `hits.total` for result counts or pagination
  receive OpenSearch-compatible metadata.
- KalDB can expose the same accuracy/cost tradeoff as OpenSearch through
  `track_total_hits`.

Costs:

- The internal result model and protobuf need additional total-hit fields.
- Top-N hit searches may do more work when callers request exact or high
  threshold total counts.
- Tests must distinguish returned hit count from matched-document count.

Risks:

- Incorrect threshold reduction across workers could report `eq` when the
  distributed total is only a lower bound.
