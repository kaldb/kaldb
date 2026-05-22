# ADR 0007: Support OpenSearch Bucket Selector Pipeline Aggregations

## Status

Current state: `Accepted`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB supports OpenSearch-compatible bucket aggregations such as `terms` and
`multi_terms`, but ClickBench-style analytical queries also need SQL `HAVING`
semantics. In OpenSearch DSL, SQL `HAVING` maps to the `bucket_selector`
pipeline aggregation.

For example:

```sql
SELECT CounterID, COUNT(*) AS c
FROM hits
GROUP BY CounterID
HAVING COUNT(*) > 100000
```

maps to a bucket aggregation with a child pipeline aggregation:

```json
{
  "aggs": {
    "by_counter": {
      "terms": {
        "field": "CounterID"
      },
      "aggs": {
        "having": {
          "bucket_selector": {
            "buckets_path": {
              "c": "_count"
            },
            "script": "params.c > 100000"
          }
        }
      }
    }
  }
}
```

The parent `terms` aggregation first creates buckets and computes metrics.
`bucket_selector` then removes buckets whose reduced values fail the predicate.
Applying this predicate before all chunks and workers have been reduced can
drop buckets that should survive globally.

## Questions

- Question: Is `bucket_selector` a document-level filter?
  Answer: No. It is a bucket-level post-filter. It runs after the parent bucket
  aggregation has produced buckets and after the values referenced by
  `buckets_path` are available.

- Question: Should `bucket_selector` run during partial worker or chunk-manager
  reduction?
  Answer: No. It must run only during final reduction, after bucket counts and
  sub-aggregation metrics have been reduced across all participating chunks and
  workers.

- Question: Should KalDB implement the predicate language itself?
  Answer: No. KalDB should delegate parsing and execution to OpenSearch's
  pipeline aggregation and Painless scripting implementations.

- Question: How is this different from `bucket_sort`?
  Answer: Both are child pipeline aggregations that operate on parent buckets
  after bucket creation. `bucket_selector` filters buckets by predicate;
  `bucket_sort` sorts and slices buckets. They share the same final-reduction
  staging requirement.

- Question: Which ClickBench queries does this unblock?
  Answer: Q27 and Q28 require SQL `HAVING COUNT(*) > ...`, which maps to
  `bucket_selector`.

## Public Interfaces

- OpenSearch-compatible search requests may include `bucket_selector` under
  the `aggs` or `aggregations` object of a multi-bucket parent aggregation.
- Supported parent bucket aggregations include `terms`, `multi_terms`, and
  `date_histogram` where OpenSearch supports the requested `buckets_path`.
- `bucket_selector.buckets_path` may reference `_count` and numeric metric
  sub-aggregations available from the final reduced parent bucket.
- `bucket_selector.script` uses OpenSearch's existing pipeline script context.
- No config files, flags, metadata formats, or persisted state change.

## Proposed Changes

### Summary

Support OpenSearch `bucket_selector` by preserving child pipeline aggregators
through aggregation parsing, transport, and partial reduction, then applying
them only during final distributed aggregation reduction.

### Detailed Design

KalDB already has two aggregation reduction stages:

```text
worker or chunk-manager reduction
  -> merges partial results from local chunks

distributed query-node reduction
  -> merges worker results into the final response
```

`bucket_selector` must not execute in the first stage:

```text
worker or chunk-manager reduction
  -> merge buckets
  -> do not filter by bucket_selector
```

It must execute in the final stage:

```text
distributed query-node reduction
  -> merge buckets from all workers
  -> reduce sub-aggregation metrics
  -> apply bucket_selector predicates to the final bucket list
```

KalDB should continue to use OpenSearch's aggregation parser. The request JSON
should produce OpenSearch aggregation and pipeline aggregation objects. KalDB's
job is to keep those objects attached to the right parent bucket aggregation
and to provide the correct OpenSearch reduction context at the correct stage.

`SearchResultAggregatorImpl` should build or preserve the full
`PipelineAggregator.PipelineTree` for the parsed aggregation request. The tree
must include child pipeline aggregators nested under parent bucket aggregations,
not only top-level sibling pipeline aggregations.

During partial reduction, KalDB should use a partial reduction context that
does not apply child pipeline predicates. During final reduction, KalDB should
use OpenSearch's final reduction and pipeline reduction APIs so predicates run
against globally reduced buckets.

## Compatibility, Deprecation, And Migration Plan

This change is additive for requests that do not use `bucket_selector`.

Requests containing `bucket_selector` that currently return unfiltered buckets
or fail would begin returning OpenSearch-compatible filtered bucket lists. No
stored metadata migration is required because this is query-time behavior.

Rollback is safe at the storage layer. After rollback, `bucket_selector`
requests would again fail or return unfiltered buckets depending on the old
code path.

## Test Plan

- Parser tests proving `bucket_selector` requests are accepted in the supported
  parent aggregation shapes.
- Local search tests where one bucket fails the predicate and is removed.
- Distributed tests where a bucket would fail on one worker but pass after
  global reduction, proving predicates do not run during partial reduction.
- Tests for `_count` in `buckets_path`.
- Tests for numeric metric sub-aggregation paths in `buckets_path`.
- Regression coverage for ClickBench-shaped Q27 and Q28 request shapes.
- Tests proving `bucket_selector` and `bucket_sort` can coexist under the same
  parent where OpenSearch supports that combination.

## Documentation Plan

- Document that `bucket_selector` implements bucket-level `HAVING`, not
  document-level `WHERE`.
- Document supported `buckets_path` forms.
- Document that child pipeline aggregations run only after final reduction.

## Rejected Alternatives

- Apply `bucket_selector` independently on each worker.
  Rejected because local predicates can remove buckets that would satisfy the
  predicate after all workers are reduced.

- Implement `bucket_selector` predicate evaluation directly in KalDB.
  Rejected because OpenSearch already owns pipeline script parsing, script
  context handling, gap policy behavior, and predicate execution.

- Ask clients to emulate `HAVING` by fetching more buckets and filtering
  client-side.
  Rejected because it leaks a KalDB compatibility gap into clients and can
  require transferring many unnecessary buckets.

## Consequences

Benefits:

- KalDB can answer OpenSearch-compatible bucket `HAVING` requests.
- ClickBench Q27 and Q28 no longer need to be blocked on missing HAVING
  semantics.
- The implementation shares the same final-reduction model as `bucket_sort`
  instead of inventing a second child-pipeline path.

Costs:

- Aggregation reduction must preserve and apply the full nested pipeline tree.
- Tests must distinguish partial and final reduction behavior.

Risks:

- Applying predicates too early produces plausible but wrong distributed
  results.
- Building the pipeline tree from only the first top-level aggregation can miss
  child pipeline aggregators nested under other parents.
