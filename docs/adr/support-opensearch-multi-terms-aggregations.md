# ADR 0002: Support OpenSearch Multi-Terms Aggregations

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB supports OpenSearch-compatible aggregations for log analytics, but does not
support the OpenSearch `multi_terms` bucket aggregation. That leaves a compatibility
gap for workloads that group by more than one field in a single aggregation.

`multi_terms` is the OpenSearch aggregation equivalent of a SQL grouped result
over multiple dimensions:

```sql
GROUP BY field_a, field_b
```

Without native `multi_terms` support, clients must either issue multiple
queries, use a synthetic combined field at ingest time, or give up on matching
standard OpenSearch query shapes. Those workarounds push KalDB-specific behavior
into clients and make migration from OpenSearch harder.

This ADR is limited to `multi_terms` itself: creating compound-key buckets,
reducing those buckets across chunks and nodes, and returning the
OpenSearch-compatible response shape. 

## Questions

- Question: Should Astra support OpenSearch `multi_terms` as a first-class
  aggregation?
  Answer: Yes. It is the standard OpenSearch shape for grouping by multiple
  fields and is required by common analytical query translations.

- Question: Should clients be required to create synthetic combined fields at
  ingest time instead?
  Answer: No. Synthetic fields can be useful for selected high-volume queries,
  but they are not a substitute for OpenSearch-compatible query behavior.

- Question: Should `multi_terms` support sub-aggregations?
  Answer: Yes. `multi_terms` should behave like other OpenSearch bucket
  aggregations: each compound-key bucket can contain metric or bucket
  sub-aggregations.

- Question: Should `multi_terms` support ordering by `_count`, `_key`, and
  sub-aggregation metrics?
  Answer: Yes for the OpenSearch ordering forms needed by the compatibility
  workload. Ordering should be delegated to OpenSearch's aggregation
  implementation rather than reimplemented in Astra.

## Public Interfaces

- OpenSearch-compatible search requests may include a `multi_terms`
  aggregation under `aggs` or `aggregations`.
- A `multi_terms` request contains a `terms` array, where each entry identifies
  one field participating in the compound bucket key.
- Responses include one bucket per compound key. Each bucket's `key` is an
  ordered array matching the request's `terms` array.
- The aggregation may use OpenSearch-compatible `size` and `order` options.

## Proposed Changes

### Summary

Support OpenSearch `multi_terms` by wiring OpenSearch's native multi-terms
aggregation implementation into Astra's aggregation parse, execution,
transport, distributed reduction, and response paths.

The `multi_terms`-specific production change will be small because the
generic aggregation path already accepts OpenSearch aggregation JSON, executes
top-level aggregators through OpenSearch collectors, transports
`InternalAggregations`, and reduces results with OpenSearch reduction APIs. In
that state, `multi_terms` mainly needs registration in Astra's OpenSearch
adapter and serialization registry.

### Detailed Design

The implementation depends on the generic aggregation path already doing the
following:

- Ensure OpenSearch aggregation parsing accepts `multi_terms` aggregation JSON.
- Execute `multi_terms` through the same local Lucene aggregation collector path
  used by other bucket aggregations.
- Reduce distributed multi-terms results using OpenSearch's internal reduction
  APIs rather than hand-merging buckets in Astra code.
- Preserve sub-aggregation results and ordering during distributed reduction.
- Return the final OpenSearch-compatible JSON response without introducing an
  Astra-specific wrapper or key format.

The `multi_terms`-specific implementation should:

- Register the multi-terms values-source implementation in
  `OpenSearchAdapter` so the aggregator can be built for supported field types.
- Register `InternalMultiTerms` in `OpenSearchInternalAggregation` so results
  can cross Astra's internal transport boundary.

We considered also registering `MultiTermsAggregationBuilder` as an
`AggregationBuilder` named writeable in `OpenSearchInternalAggregation`. That
would be required if Astra serialized aggregation request builders across its
internal binary transport. In the current request path, however, Astra keeps the
aggregation request as JSON and parses it through OpenSearch's XContent registry
on the node that executes the search. Only the resulting `InternalAggregations`
are serialized after local execution. Because of that, `InternalMultiTerms`
needs a named-writeable registration for result transport, but
`MultiTermsAggregationBuilder` does not.

The implementation should support the field types Astra already exposes for
sorting and aggregations, including keyword/string, integer/long, floating
point, boolean, and date-compatible fields where OpenSearch's multi-terms
implementation supports them.

### Out Of Scope

- Ingest-time synthetic combined fields.
- Rollup or precomputed aggregation indexes for common compound dimensions.

## Compatibility, Deprecation, And Migration Plan

This change is additive. Requests that previously used supported aggregations
should continue to behave the same way.

Requests containing `multi_terms` that currently fail would begin returning
OpenSearch-compatible aggregation results. No stored metadata migration is
required because the change affects query execution only.

Rollback is safe at the storage layer because no persisted data format changes.
After rollback, `multi_terms` requests would fail again or return an unsupported
aggregation error.

## Rejected Alternatives

- Require clients to split requests into several single-field `terms`
  aggregations.
  Rejected because separate single-field aggregations do not produce compound
  buckets and cannot represent multi-column grouping semantics.

- Require ingest-time synthetic combined fields.
  Rejected because it moves query compatibility into schema design, increases
  ingest/storage cost, and requires users to know all compound dimensions
  before data is written.

- Implement compound bucket merging directly in Astra.
  Rejected because OpenSearch already provides the parser, collector, bucket
  representation, ordering, sub-aggregation, serialization, and reduction
  behavior. Reimplementing that behavior would increase compatibility risk.

## Consequences

Benefits:

- Astra can answer OpenSearch-compatible multi-dimensional grouping queries.
- Clients can use standard OpenSearch `multi_terms` request and response shapes.
- ClickBench-style grouped analytics queries map more directly to KalDB.
- Distributed reduction remains aligned with OpenSearch semantics.

Costs:

- Aggregation transport must understand `InternalMultiTerms`.
- Per-document aggregation work increases with the number of fields in the
  compound key because each collected document must read each key component.
