# ADR 0006: Support OpenSearch Terms Aggregation Metadata

## Status

Current state: `Accepted`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

OpenSearch `terms`-style bucket aggregations return metadata in addition to the
visible buckets. Two important fields are:

- `sum_other_doc_count`
- `doc_count_error_upper_bound`

These fields are not decorative. Clients can use them to understand whether
the returned bucket list represents all matching documents and how much
uncertainty may remain in the bucket counts after distributed reduction.

KalDB should treat these fields as part of OpenSearch response compatibility.
If KalDB returns different values from OpenSearch for the same query and data,
that is a real compatibility gap, even when the visible bucket list looks
plausible.

## Questions

- Question: What does `sum_other_doc_count` mean?
  Answer: It is the number of documents that belong to buckets omitted from the
  response because the parent aggregation returned only the top bucket page.

- Question: What does `doc_count_error_upper_bound` mean?
  Answer: It is an upper bound on how much the returned bucket counts could be
  undercounted because distributed shards or chunks may not have returned every
  candidate bucket.

- Question: Should KalDB compute these values by hand?
  Answer: No. KalDB should preserve and reduce OpenSearch's native aggregation
  metadata through OpenSearch's aggregation reduction APIs.

- Question: Are these values always exact?
  Answer: `sum_other_doc_count` is expected to reflect the omitted-bucket
  document count for the buckets the aggregation considered. The
  `doc_count_error_upper_bound` field exists precisely because returned bucket
  counts may be approximate under distributed top-N collection.

- Question: Should the comparison harness normalize these fields away?
  Answer: It may do so temporarily to isolate known gaps, but product
  compatibility should treat them as meaningful response fields.

## Public Interfaces

- OpenSearch-compatible `terms` and `multi_terms` responses may include
  `sum_other_doc_count`.
- OpenSearch-compatible `terms` and `multi_terms` responses may include
  `doc_count_error_upper_bound`.
- Bucket responses may include per-bucket `doc_count_error_upper_bound` when
  requested by OpenSearch-supported options such as `show_term_doc_count_error`.
- No config files, flags, environment variables, metadata formats, or persisted
  state change.

## Proposed Changes

### Summary

Preserve OpenSearch terms aggregation metadata through local collection,
partial reduction, final distributed reduction, serialization, and response
normalization. Avoid local hand-rolled metadata calculations unless a specific
OpenSearch API requires KalDB-owned behavior.

### Detailed Design

KalDB should continue to use OpenSearch's aggregation implementation for
`terms`-style aggregations. The important requirement is that KalDB must not
discard or overwrite metadata while adapting aggregation objects across its
own boundaries.

The search path should preserve metadata at these points:

- local Lucene aggregation collection;
- worker or chunk-manager partial aggregation reduction;
- serialization into KalDB's internal result transport;
- deserialization on the query node;
- final OpenSearch aggregation reduction;
- final JSON response serialization.

`OpenSearchInternalAggregation` should serialize the full internal aggregation
object or full `InternalAggregations` collection, including metadata fields
owned by OpenSearch. `SearchResultAggregatorImpl` should use OpenSearch's
partial and final reduction contexts so OpenSearch can maintain the same error
and omitted-bucket accounting it would maintain in a native cluster.

For `bucket_sort`, KalDB should remember that bucket pagination happens after
the parent aggregation has produced buckets. `bucket_sort` can change which
buckets are visible, but it cannot recover buckets that the parent aggregation
did not collect. Parent `size` and distributed `shard_size` still determine how
accurate later bucket pages and metadata can be.

## Compatibility, Deprecation, And Migration Plan

This change affects response metadata only. It should not change matching,
bucket key creation, or metric values except where existing metadata loss was
also caused by incorrect reduction.

No stored metadata migration is required. Rollback is safe at the storage
layer, but response metadata may again differ from OpenSearch.

## Test Plan

- Unit tests for `terms` aggregation responses that return omitted buckets and
  non-zero `sum_other_doc_count`.
- Distributed tests where different workers see different top buckets, proving
  final reduction produces OpenSearch-compatible metadata.
- Tests for `show_term_doc_count_error` where supported.
- Tests covering `multi_terms` metadata, not only single-field `terms`.
- Bucket-sort tests proving bucket pagination does not get confused with parent
  aggregation size or metadata accounting.
- Harness coverage that reports these fields instead of silently ignoring them
  once the product behavior is implemented.

## Documentation Plan

- Document what `sum_other_doc_count` and `doc_count_error_upper_bound` mean.
- Document that these values are aggregation metadata, not separate query
  results.
- Document the relationship between parent aggregation `size`, distributed
  `shard_size`, `bucket_sort`, and metadata accuracy.

## Rejected Alternatives

- Drop these fields from comparisons and treat visible buckets as sufficient.
  Rejected because OpenSearch clients may rely on metadata to understand result
  completeness and uncertainty.

- Compute `sum_other_doc_count` as "matched docs minus returned bucket docs" in
  every case.
  Rejected because nested aggregations, filters, parent bucket scope, and
  distributed candidate selection make that shortcut easy to misapply.

- Force parent aggregations to collect every bucket to make metadata exact.
  Rejected because high-cardinality terms aggregations can become too expensive.
  OpenSearch exposes size and shard-size controls so callers can choose the
  cost/accuracy tradeoff.

## Consequences

Benefits:

- Terms-style aggregation responses become more OpenSearch-compatible.
- Clients can reason about omitted buckets and count uncertainty.
- ClickBench/OpenSearch comparison reports can stop hiding these fields once
  product behavior is fixed.

Costs:

- Tests need to cover metadata, not only bucket keys and counts.
- Some mismatches that were previously normalized away will become visible.

Risks:

- Hand-written metadata calculations could diverge from OpenSearch semantics.
- Incorrect partial-vs-final reduction can produce plausible buckets with wrong
  metadata, which is harder to notice than a hard failure.
