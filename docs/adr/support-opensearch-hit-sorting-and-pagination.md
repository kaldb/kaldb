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
  Answer: No. The first implementation should support the sort forms required by
  compatibility tests and reject unsupported sort options clearly.

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
- The first supported explicit sort fields should include `@timestamp` and
  `_timesinceepoch`, in ascending or descending order.
- Unsupported sort fields or sort options should fail with a clear user-facing
  error instead of being silently ignored.
- The internal gRPC `SearchRequest` and `SearchQuery` representations need to
  carry hit offset and hit sort information.

## Proposed Changes

### Summary

Represent root hit pagination and hit sorting explicitly in KalDB's internal
search request, collect enough sorted hits on each worker, and apply the final
offset and limit after distributed hit merge.

### Detailed Design

The implementation should add internal request fields for:

- `hit_from`: the zero-based hit offset.
- `hit_size`: the number of hits requested.
- `hit_sort`: the ordered list of hit sort keys.

The current `how_many` field can either be renamed in a compatibility-aware way
or treated as `hit_size` while adding new fields for offset and sort. Any proto
change must reserve old field numbers correctly and preserve compatibility for
existing callers.

`OpenSearchRequest` should parse root `from`, `size`, and `sort` from the
request body. It should translate supported sort entries into the internal
request form and reject unsupported sort entries.

`SearchQuery` should carry the parsed offset, size, and sort list. It should
validate that `from >= 0` and `size >= 0`.

`LogIndexSearcherImpl` should build the Lucene `Sort` from the requested sort
list instead of always constructing:

```java
new SortField("_timesinceepoch", SortField.Type.LONG, true)
```

Workers should collect `from + size` hits, not just `size` hits. That is
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

`SearchResultAggregatorImpl` should merge hits using the requested sort order,
then apply:

```text
skip(from)
limit(size)
```

The final merge should use a stable tie-breaker so distributed results are
deterministic when multiple hits have the same sort values. A practical
tie-breaker is the document timestamp followed by a stable identifier such as
the log message ID, provided the selected tie-breaker is available in all
returned hits.

### Supported Sort Scope

The initial implementation should keep the supported surface intentionally
small:

- `@timestamp` ascending and descending.
- `_timesinceepoch` ascending and descending.
- Multi-key sort only after compatibility tests define the expected behavior.

The following should remain out of scope for the first implementation unless
compatibility tests require them:

- `_score` sorting.
- `_doc` sorting.
- `search_after`.
- `track_scores`.
- custom `missing` handling.
- `unmapped_type`.
- script-based sorts.
- geo-distance sorts.
- nested sorts.

## Compatibility, Deprecation, And Migration Plan

This change is additive for clients that omit `from` and `sort`. Those requests
should continue to return the latest hits by timestamp descending.

Clients that already send root `from` or `sort` would move from ignored or
partially honored behavior to explicit supported or explicitly rejected
behavior.

No stored metadata migration is required. If the internal gRPC request changes,
the rollout must be compatible with mixed binaries or coordinated so workers and
query nodes understand the same request fields.

Rollback is safe at the storage layer because no persisted data format changes.
After rollback, root `from` and explicit `sort` would no longer be honored.

## Test Plan

- Parser tests for root `from`, `size`, and supported `sort` forms.
- Parser tests proving unsupported sort options fail clearly.
- Unit tests for local hit collection using timestamp ascending and descending.
- Unit tests proving `from` is applied after sorting.
- Distributed tests where each worker returns `from + size` candidates and the
  final query node applies the global offset.
- Tie-breaker tests for hits with equal sort values.
- Compatibility tests for omitted `sort`, proving timestamp descending remains
  the default.

## Documentation Plan

- Document supported root hit sort fields and directions.
- Document default hit ordering.
- Document how `from` and `size` are interpreted.
- Document unsupported OpenSearch sort options until they are implemented.

## Rejected Alternatives

- Continue ignoring root `from` and explicit root `sort`.
  Rejected because clients can receive plausible but incorrect result windows.

- Apply `from` independently on each worker.
  Rejected because distributed offsets are global. Applying the offset before
  the final merge can drop hits that belong in the final page.

- Fetch only `size` hits from each worker.
  Rejected because it is not enough to construct pages with a non-zero `from`.

- Support every OpenSearch sort option in the first implementation.
  Rejected because many sort options require additional field handling,
  scoring, scripting, or nested-document semantics. The first implementation
  should cover the compatibility workload and fail clearly for unsupported
  options.

## Consequences

Benefits:

- KalDB can honor standard OpenSearch hit pagination requests.
- Clients can request deterministic windows over sorted hit results.
- Timestamp ascending scans become possible through the OpenSearch-compatible
  request shape.
- Unsupported sort forms become explicit errors instead of silent behavior
  changes.

Costs:

- Workers may need to return more hit candidates for non-zero offsets.
- Distributed hit merging must use the requested sort order rather than the
  current hard-coded timestamp descending comparator.
- The internal request contract must grow to carry offset and sort information.

Risks:

- Large `from` values can increase per-worker memory and network cost because
  workers must return `from + size` candidates.
- Missing or inconsistent tie-breakers can produce unstable pagination.
- Mixed-version deployments need care if the gRPC request schema changes.
