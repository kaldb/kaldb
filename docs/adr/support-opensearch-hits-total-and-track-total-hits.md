# ADR 0005: Support OpenSearch Hits Total And Track Total Hits

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB's OpenSearch-compatible response currently builds `hits.total.value` from
the number of hits returned in the current page. That is not the OpenSearch
contract.

`size` controls how many hit documents are returned in `hits.hits`. It does not
control how many documents matched the query. For example, a request with
`size: 0` should return no hit documents, but `hits.total` should still report
the number of matching documents, subject to `track_total_hits`.

The current behavior makes count-only, aggregation-only, and paginated searches
look like they matched fewer documents than they actually matched. The returned
documents and aggregation values can be correct while the response metadata is
still wrong.

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
  threshold and return `relation: "gte"` when the threshold is exceeded.

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
- `track_total_hits: false` allows the cheapest response and may omit or avoid
  accurate total counting where OpenSearch permits it.
- `track_total_hits: <integer>` requests exact counting up to that threshold.
- The internal search result representation must carry both the total-hit value
  and whether the value is exact.

## Proposed Changes

### Summary

Carry total-hit information from Lucene through KalDB's internal search result
path and serialize it into the OpenSearch-compatible response. Do not derive
`hits.total` from the number of returned hits.

### Detailed Design

`SearchResult` should carry:

- total hit value;
- total hit relation, or an equivalent exact/not-exact boolean.

The internal `AstraSearch.SearchResult` protobuf should carry the same
information so worker-node results can be serialized, sent to the query node,
deserialized, and reduced without losing total-hit metadata.

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
- return `relation: "eq"` only when every contributing result is exact;
- return `relation: "gte"` if any contributing result is a lower bound.

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
