# ADR 0001: Support Sibling Aggregations In A Single Search Request

## Status

Current state: `Accepted`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

Astra's OpenSearch-compatible search API currently rejects any request whose
top-level `aggs` object contains more than one named aggregation. The guard
lives in `OpenSearchRequest.getAggregationJson` and throws
`NotImplementedException`, which surfaces to clients as HTTP 500.

This is not a limitation in Lucene or in OpenSearch's aggregation framework.
The bug is in Astra's glue code. OpenSearch already models top-level
aggregations as a collection, can execute sibling aggregators in a single scan
pass, and can reduce them into a combined `InternalAggregations` result. Astra
collapses that collection to a single element at several points in its request,
execution, transport, and response paths.

The single-aggregation assumption appears in at least these places:

- `OpenSearchRequest.getAggregationJson` rejects requests whose top-level
  `aggs` object has more than one field.
- `OpenSearchAdapter.buildAggregatorFromFactory` returns only the first
  top-level `Aggregator`.
- `SearchResultAggregatorImpl` reduces only the first top-level aggregation.
- `OpenSearchInternalAggregation` serializes and deserializes a single
  `InternalAggregation` rather than the full `InternalAggregations` list.
- Response shaping expects one top-level aggregation result rather than a
  collection keyed by aggregation name.

Customers work around this by splitting sibling aggregations across multiple
`_msearch` sub-requests. That is functionally workable but operationally wrong.
It leaks an Astra-specific limitation into clients and duplicates work that the
server should be able to amortize.

The amortization story matters. For a request with `N` sibling top-level
aggregations, a single multi-aggregation request saves versus `N` separate
`_msearch` sub-requests:

- `N -> 1` query rewrite and posting-list / BKD traversal per chunk
- `N -> 1` doc-iteration loop over matched documents
- `N -> 1` per-chunk reader open, factory build, and context setup
- `N -> 1` round-trip from the query node to each chunk-holding node

What does not change is the per-document field read. Each sibling aggregator
still reads its own field's doc-values column for every matched document, so
per-document aggregation work remains linear in the number of siblings.
Multi-aggregation therefore does not make extra siblings free; it removes the
extra traversal and setup that would otherwise be repeated across separate
requests.

The important invariant is that a multi-aggregation request always saves one
extra traversal for each omitted sibling request. If the saved traversal is a
large fraction of total cost, the win is large. If the per-aggregation work
done after traversal dominates total cost, the relative win is smaller even
though the traversal is still saved. Broad scans can therefore still benefit
substantially; they are only "modest" when the repeated traversal cost is small
compared with the per-document aggregation work that remains necessary for each
sibling.

If Astra stays as-is, dashboards and clients that expect standard OpenSearch
sibling aggregation behavior will continue to fail or require application-side
query splitting.

## Questions

- Question: Should Astra continue to require users to split sibling
  aggregations into multiple `_msearch` requests?
  Answer: No. That leaks an Astra-specific limitation into clients, multiplies
  fan-out and setup work, and preserves a compatibility gap with OpenSearch.

- Question: Is removing only the request parser guard sufficient?
  Answer: No. Later stages still assume a single top-level aggregation and
  would continue to drop siblings or fail during reduction, transport, or
  response shaping.

- Question: What internal representation should Astra use for top-level
  sibling aggregations?
  Answer: Astra should use OpenSearch's native `InternalAggregations`
  collection end-to-end rather than extracting only the first
  `InternalAggregation`.

- Question: Does this proposal eliminate per-document aggregation cost for
  additional siblings?
  Answer: No. Each sibling still performs its own field read and state update
  per matched document. The benefit is amortizing query traversal, setup, and
  network fan-out, while still saving one extra traversal for each omitted
  sibling request.

- Question: Are nested sub-aggregations part of the problem being solved here?
  Answer: No. Nested sub-aggregations are already handled by OpenSearch's
  aggregator tree. The target here is multiple sibling top-level aggregations
  in the same request.

## Public Interfaces

- OpenSearch-compatible search requests would be allowed to contain multiple
  sibling top-level aggregations in a single `aggs` object.
- OpenSearch-compatible search responses would include multiple named entries
  under the top-level `"aggregations"` field for a single request.
- Single top-level aggregation requests would remain supported with no query
  shape change.
- No config files, flags, environment variables, metadata formats, or
  persisted state would change.
- No operator workflow changes would be required.

## Proposed Changes

### Summary

Support multiple sibling top-level aggregations in a single search request by
using OpenSearch's native top-level aggregation collection path end-to-end.

### Detailed design

The implementation should:

- Remove the top-level `aggs` cardinality guard from
  `OpenSearchRequest.getAggregationJson` and pass the full aggregation JSON to
  OpenSearch's parser.
- Build all top-level aggregators for a request and execute them through a
  wrapped `BucketCollector` so one `IndexSearcher.search` pass dispatches each
  matched document to every sibling collector.
- Return per-chunk aggregation results as `InternalAggregations` rather than
  extracting only the first `InternalAggregation`.
- Reduce distributed results with OpenSearch's top-level aggregation reduction
  APIs so sibling results are reduced by name and pipeline behavior remains
  correct.
- Serialize and deserialize the full `InternalAggregations` payload across
  Astra's internal transport boundary.
- Emit the final OpenSearch response with one entry per sibling under the
  standard top-level `"aggregations"` object, keyed by the user-supplied
  aggregation name.

The implementation should also audit other aggregation call sites for
single-element assumptions such as `.get(0)`, `iterator().next()`, or helper
methods that imply one top-level aggregation result.

## Rejected Alternatives

- Keep the `_msearch` workaround as the supported path.
  Rejected because it leaks Astra's limitation into clients, multiplies
  fan-out and setup work, and preserves a compatibility gap with OpenSearch.

- Accept multiple siblings at the API layer but execute them sequentially on
  each node.
  Rejected because it gives up the main server-side efficiency benefit of
  sharing one scan pass across all sibling aggregations.

- Introduce an Astra-specific transport or response representation for multiple
  top-level aggregations.
  Rejected because OpenSearch already provides the right internal and external
  representation, and diverging from it would increase long-term maintenance
  cost.

## Consequences

Benefits:

- Astra would accept a standard OpenSearch request shape that currently fails.
- Query nodes would fan out one request instead of forcing clients to
  coordinate multiple `_msearch` sub-requests.
- Per-chunk query rewrite, filter traversal, reader setup, and request fan-out
  would be amortized across all sibling aggregations in the request.
- The implementation would align Astra with OpenSearch's intended aggregation
  model instead of preserving a local special case.

Costs:

- The search path would need to handle `InternalAggregations` as a collection,
  which is broader than the current single-root shortcut.
- Per-document aggregation work would still grow with the number of siblings
  because each sibling collector must read its own field values and update its
  own bucket or metric state.
- Response payload size would grow with the number of sibling aggregations, as
  expected for standard OpenSearch behavior.

Risks:

- The single-aggregation assumption may exist in additional call sites beyond
  the ones already identified, especially around `.get(0)`,
  `iterator().next()`, or single-value transport helpers.
- Response shaping must preserve the exact OpenSearch-compatible JSON shape.
  Returning an extra wrapper object or flattening incorrectly would still break
  clients even if execution succeeds.
- Backward-compatible single-aggregation access paths may need to remain
  temporarily in the implementation until all internal callers are migrated to
  the collection form.

Out of scope:

- Rollup or precomputed aggregation indexes for workloads dominated by
  doc-values scan cost.
- Deduplicating same-field sibling doc-values reads within OpenSearch's
  collector execution model.
