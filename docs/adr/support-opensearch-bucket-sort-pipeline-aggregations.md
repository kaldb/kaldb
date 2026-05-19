# ADR 0003: Support OpenSearch Bucket Sort Pipeline Aggregations

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB supports OpenSearch-compatible bucket aggregations such as `terms` and
`date_histogram`, but bucket aggregation results need a separate way to sort and
page the returned buckets. OpenSearch provides that behavior through the
`bucket_sort` pipeline aggregation.

`bucket_sort` is not a document search feature. It does not control returned
hits. It runs after a parent bucket aggregation has created buckets and then
sorts, offsets, or limits those buckets.

For example, a request can first group documents by URL and then ask
`bucket_sort` to return buckets `2..3` after sorting by document count:

```json
{
  "aggs": {
    "by_url": {
      "terms": {
        "field": "url",
        "size": 100
      },
      "aggs": {
        "page": {
          "bucket_sort": {
            "sort": [
              {
                "_count": {
                  "order": "desc"
                }
              }
            ],
            "from": 2,
            "size": 2
          }
        }
      }
    }
  }
}
```

Without `bucket_sort`, clients can ask KalDB to compute buckets but cannot use
the standard OpenSearch request shape for bucket-level pagination. That leaves a
compatibility gap for analytical workloads and ClickBench-style translated
queries.

This ADR is limited to bucket-level sorting and pagination. It does not cover
top-level hit `from`, top-level hit `size`, or top-level hit `sort`.

## Questions

- Question: Is `bucket_sort` part of `multi_terms`?
  Answer: No. `bucket_sort` is a pipeline aggregation that can be placed under
  any supported multi-bucket parent aggregation, including `terms`,
  `multi_terms`, and `date_histogram`.

- Question: Should KalDB apply `bucket_sort` during partial aggregation on
  individual nodes?
  Answer: No. `bucket_sort` should be applied during final reduction, after all
  matching buckets from participating chunks and nodes have been reduced.
  Applying it early can drop buckets that should appear in the global result.

- Question: Does `bucket_sort` make the parent aggregation collect more
  buckets?
  Answer: No. `bucket_sort` only sorts and slices the buckets produced by its
  parent. The parent aggregation's `size` and, for distributed terms-style
  aggregations, `shard_size` must be large enough to make the requested bucket
  page accurate.

- Question: Should sorting by multiple keys be supported?
  Answer: Yes. The OpenSearch `bucket_sort.sort` field is a list, and KalDB
  should delegate sorting behavior to OpenSearch rather than implementing a
  local single-key special case.

- Question: Should `bucket_sort` support sorting buckets by metric
  sub-aggregations, such as sorting URL buckets by `avg_latency`?
  Answer: Yes. KalDB's OpenSearch aggregation path can support this class of
  sort during final reduction, but each documented metric sort shape should
  have compatibility test coverage.

## Public Interfaces

- OpenSearch-compatible search requests may include a `bucket_sort` pipeline
  aggregation under the `aggs` or `aggregations` object of a multi-bucket parent
  aggregation.
- Supported parent bucket aggregations include `terms`, `date_histogram`, and
  `multi_terms` once `multi_terms` itself is supported.
- `bucket_sort` may use OpenSearch-compatible `sort`, `from`, and `size`
  options.
- `bucket_sort.sort` may contain multiple sort entries.
- Supported sort paths include OpenSearch bucket sort paths that can be
  evaluated from the final reduced parent bucket, including `_key`, `_count`,
  and numeric metric sub-aggregation paths.
- No config files, flags, environment variables, metadata formats, or persisted
  state change.

## Proposed Changes

### Summary

Support OpenSearch `bucket_sort` by using OpenSearch's native pipeline
aggregation reduction path during final distributed aggregation reduction.

`bucket_sort` does not collect field values
from matching documents. It is a post-processing step over already-created
buckets. Therefore the implementation should not add a new values-source
execution factory. It should ensure pipeline aggregations are parsed, retained,
and materialized at the correct reduction stage.

### Detailed Design

KalDB already has two aggregation reduction points:

```text
worker or chunk-manager reduction
  -> merges partial results from local chunks

distributed query-node reduction
  -> merges results returned by workers and produces the final response
```

`bucket_sort` must run only in the second location.

```text
worker or chunk-manager reduction
  -> merge buckets
  -> do not apply bucket_sort

distributed query-node reduction
  -> merge buckets from all workers
  -> apply bucket_sort to the final parent bucket list
```

The implementation should keep using OpenSearch's aggregation parser. KalDB
should not parse `bucket_sort` JSON by hand. OpenSearch should continue to read
the request and build the internal aggregation objects for fields such as
`sort`, `from`, and `size`.

During worker or chunk-manager reduction, KalDB should use OpenSearch's partial
reduction context with an empty pipeline tree. That lets workers merge local
bucket results without running `bucket_sort`.

During distributed query-node reduction, KalDB should build the pipeline tree
from the parsed aggregation request and pass it to OpenSearch's final reduction
context. In plain terms, the pipeline tree tells OpenSearch that a parent bucket
aggregation, such as `terms` or `date_histogram`, has a post-processing step
such as `bucket_sort` attached to it.

After ordinary bucket reduction finishes at the query node, KalDB should call
OpenSearch's pipeline reduction API once:

```java
internalAggregation =
    internalAggregation.reducePipelines(internalAggregation, reduceContext, pipelineTree);
```

For example, this request should return buckets `B` and `C`:

```text
parent buckets: A, B, C, D
bucket_sort: from = 1, size = 2
```

### Parent Size And Accuracy

`bucket_sort` cannot recover buckets that the parent aggregation did not return.
Callers must size the parent aggregation for the requested bucket page.

For a request like:

```json
{
  "bucket_sort": {
    "from": 20,
    "size": 10
  }
}
```

the parent aggregation must return at least `30` buckets before `bucket_sort`
can produce that page. For distributed `terms` and `multi_terms`, callers also
need a large enough `shard_size` when ordering by count or metric values, because
each shard or chunk can otherwise omit buckets that should win globally.

KalDB should not silently rewrite parent `size` or `shard_size` as part of this
ADR. The query contract should match OpenSearch: `bucket_sort` pages the buckets
that the parent produced.

## Compatibility, Deprecation, And Migration Plan

This change is additive. Requests without `bucket_sort` continue to behave as
they do today.

Requests containing `bucket_sort` that currently fail or return unsliced parent
buckets would begin returning OpenSearch-compatible bucket-sorted results. No
stored metadata migration is required because the change affects query parsing
and reduction only.

Rollback is safe at the storage layer because no persisted data format changes.
After rollback, `bucket_sort` requests would fail again or behave as unsupported.

## Rejected Alternatives

- Implement bucket sorting and slicing directly in KalDB.
  Rejected because OpenSearch already implements bucket sort parsing, sort-path
  handling, tie-breaking, gap policy behavior, and final pipeline reduction.

- Apply `bucket_sort` independently on each query worker before distributed
  reduction.
  Rejected because early slicing can drop buckets that should be present after
  the global reduce.

- Treat `bucket_sort.from` and `bucket_sort.size` as top-level hit pagination.
  Rejected because they operate on aggregation buckets, not search hits.

- Automatically rewrite parent aggregation `size` or `shard_size`.
  Rejected for this ADR because it changes OpenSearch-compatible query semantics
  and can increase memory use in surprising ways.

## Consequences

Benefits:

- KalDB can support standard OpenSearch bucket-level pagination.
- ClickBench-style bucket offset queries can use the expected OpenSearch query
  shape.
- The implementation remains aligned with OpenSearch's pipeline reduction model.

Costs:

- Parent bucket aggregations may need larger `size` and `shard_size` values to
  make later bucket pages accurate.

Risks:

- Applying `bucket_sort` before final reduction would produce incorrect
  distributed results.
- Tests must distinguish bucket pagination from hit pagination so future changes
  do not accidentally conflate the two.
