# ADR 0004: Support OpenSearch Hit Sorting And Pagination

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB's OpenSearch-compatible search API accepts the root `size` field, but
we do not preserve the full OpenSearch hit pagination contract.

- `OpenSearchRequest` maps root `size` to the internal `how_many` field.
- Root `from` is not parsed into the internal search request.
- Root `sort` is not parsed into the internal search request.
- `LogIndexSearcherImpl` always collects hits using `_timesinceepoch`
  descending order.
- `SearchResultAggregatorImpl` merges hits from workers by timestamp descending
  and returns the first `how_many` hits.

That behavior is enough for "latest N logs" queries, but it is not enough for
OpenSearch-compatible clients that request a specific result window or an
explicit hit sort.

For example:

```json
{
  "from": 20,
  "size": 10,
  "sort": [
    {
      "@timestamp": {
        "order": "desc"
      }
    }
  ],
  "query": {
    "match_all": {}
  }
}
```

This request asks for hits `20..29` after sorting by `@timestamp` descending.
KalDB currently has no internal representation for the offset or the requested
sort list, so it cannot implement this response window correctly.

## Questions

- Question: Should KalDB support root `from` and `size` for returned hits?
  Answer: Yes. Root hit pagination is part of the standard OpenSearch search
  request shape.

- Question: Should KalDB preserve explicit root `sort` requests?
  Answer: Yes. The internal search request should carry the requested hit sort
  instead of assuming timestamp descending for every query.

- Question: What should the default sort be when clients omit `sort`?
  Answer: Keep the current behavior: timestamp descending.

- Question: Should the first implementation support every OpenSearch sort
  option?
  Answer: No. The first implementation should support the sort forms required by clickbench
  compatibility tests.

- Question: Where should `from` be applied in distributed search?
  Answer: At the final merge point. Workers should return enough sorted
  candidates for the final node to apply the global offset correctly.

## Public Interfaces

- OpenSearch-compatible search requests may include root `from`, `size`, and
  `sort` fields.
- `from` is a zero-based hit offset. If omitted, it defaults to `0`.
- `size` is the number of hits to return. If omitted, it keeps the current
  default of `10`.
- If `sort` is omitted, hits are returned by timestamp descending.
- Supported explicit sort fields include `@timestamp`, `_timesinceepoch`, and
  schema fields that are stored with Lucene doc values and map cleanly to a
  Lucene `SortField` type.
- Unsupported sort fields or sort options should fail with a clear user-facing
  error instead of being silently ignored.
- The internal gRPC `SearchRequest` and `SearchQuery` representations carry hit
  offset and hit sort information.

## Proposed Changes

### Summary

Represent root hit pagination and hit sorting explicitly in KalDB's internal
search request, collect enough sorted hits on each worker, merge hits globally
using the same sort comparator, and apply the final offset and limit after
distributed hit merge.

### Detailed Design

`OpenSearchRequest` parses root `from`, `size`, and `sort` from the request
body. `size` continues to map to the existing `how_many` request field. `from`
is carried as `start_from`, and the requested sort is carried as serialized
sort JSON that is parsed into `SearchQuery.SortFieldSpec` records.

`SearchQuery` carries the parsed offset, size, and ordered sort list. It
validates that `from >= 0`, `size >= 0`, and `from + size` fits in an integer.
If the client omits `sort`, `SearchQuery` preserves KalDB's existing default:
`_timesinceepoch` descending.

`LogIndexSearcherImpl` builds the Lucene `Sort` from the requested sort list
instead of always constructing:

```java
new SortField("_timesinceepoch", SortField.Type.LONG, true)
```

Workers collect `from + size` hits, not just `size` hits. That is
required for distributed correctness. A hit that belongs on the final page may
be ranked after the first `size` hits on its worker.

For example, with:

```json
{
  "from": 20,
  "size": 10
}
```

each worker should return up to `30` sorted candidates. The final query node
then merges all worker candidates, skips the first `20`, and returns the next
`10`.

`SearchResultAggregatorImpl` merges hits using the requested sort order, then
applies:

```text
skip(from)
limit(size)
```

The final merge uses a deterministic tie-breaker when multiple hits have the
same requested sort values. The implemented comparator falls back to timestamp
descending and then log message ID.

`SearchResponseHit` serializes the sort values corresponding to the requested
sort fields. This mirrors OpenSearch's response shape for sorted hit queries
and lets clients inspect the values that determined each hit's position.

### Supported Sort Scope

The implementation supports an ordered list of sort fields. Multi-key sort is
lexicographic: KalDB compares the first requested field, then the second if the
first is equal, and so on before applying the internal tie-breaker.

Supported sort fields:

- `@timestamp` ascending and descending.
- `_timesinceepoch` ascending and descending.
- Schema fields stored with doc values whose field type maps to a Lucene
  `SortField` type:
  - date, long, and scaled long as `LONG`;
  - boolean, integer, short, and byte as `INT`;
  - float as `FLOAT`;
  - double as `DOUBLE`;
  - keyword, string, ID, and IP as `STRING`.

Unsupported fields are rejected instead of silently ignored. Fields without doc
values are rejected because Lucene cannot sort hits by them through this path.

### Offset Cost

This design intentionally applies `from` after the final distributed merge. That
is the correct OpenSearch-style global offset behavior, but it has a cost:
workers need to return up to `from + size` candidates.

For example, `from = 1_000_000` and `size = 2` may require each worker to
collect and return up to `1_000_002` sorted candidates. That can increase heap
usage, CPU time, and network transfer. This ADR does not introduce a different
deep-pagination mechanism such as `search_after`; it preserves the standard
`from`/`size` semantics and leaves deep-pagination optimization for future work.

## Rejected Alternatives

- Apply `from` independently on each worker.
  Rejected because distributed offsets are global. Applying the offset before
  the final merge can drop hits that belong in the final page.

- Fetch only `size` hits from each worker.
  Rejected because it is not enough to construct pages with a non-zero `from`.

- Support every OpenSearch sort option in the first implementation.
  Rejected because many sort options require additional field handling,
  scoring, scripting, or nested-document semantics. The first implementation
  should cover the compatibility workload and fail for unsupported
  options.

## Consequences

Benefits:

- KalDB can honor standard OpenSearch hit pagination requests.
- Clients can request deterministic windows over sorted hit results.
- Timestamp ascending scans become possible through the OpenSearch-compatible
  request shape.

Costs:

- Workers may need to return more hit candidates for non-zero offsets.
- Distributed hit merging must use the requested sort order rather than the
  current hard-coded timestamp descending comparator.
- The internal request contract must grow to carry offset and sort information.
- Deep offsets can be expensive because this design implements `from`/`size`
  literally and does not add `search_after`.

Risks:

- Large `from` values can increase per-worker memory and network cost because
  workers must return `from + size` candidates.
