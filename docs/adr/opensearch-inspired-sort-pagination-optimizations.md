# ADR 0009: Use KalDB-Owned OpenSearch-Inspired Sort And Pagination Optimizations

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB needs OpenSearch-compatible hit sorting and pagination for clients that
send OpenSearch-shaped requests. The compatibility goal is external: requests
and responses should look familiar to OpenSearch clients where KalDB supports
the feature.

That does not imply KalDB should use OpenSearch's distributed sort runtime
directly. OpenSearch gets much of its sort efficiency from machinery that is
woven into its shard lifecycle:

- shard-level `can_match` requests;
- min/max shard ordering for the primary sort field;
- per-node shard request concurrency limits;
- bottom-sort propagation to later shard requests in a staged query phase;
- query/fetch split with retained shard search contexts;
- coordinator reduction of lightweight top-doc candidates before fetching full
  documents.

Those are useful ideas, but they are not exposed as a small sorting library.
They assume OpenSearch shards, shard iterators, shard search requests, search
contexts, fetch phases, and reduction objects.

KalDB's distributed unit is different. The query node talks to workers in KalDB
terms, and each worker may search many chunks. A worker currently returns full
KalDB hits, not OpenSearch shard top-docs plus search context IDs. Reusing the
OpenSearch runtime directly would require reshaping KalDB's query/worker/chunk
protocol around OpenSearch's shard model.

If KalDB stays as-is, sorted queries can remain correct but leave efficiency on
the table, especially when many workers return full hits that lose the final
global merge. The goal of this ADR is to adopt the relevant OpenSearch
optimization ideas in KalDB-owned internal contracts.

## Questions

- Question: Should KalDB use OpenSearch's distributed sort implementation
  directly?
  Answer: No. The high-value OpenSearch sort optimizations are coupled to the
  OpenSearch shard/query/fetch lifecycle rather than packaged as small reusable
  library calls.

- Question: Should KalDB still use Lucene's local top-K sort execution?
  Answer: Yes. The Lucene backend should continue using Lucene collectors and
  doc values for local per-chunk sorting.

- Question: Should KalDB preserve OpenSearch-compatible request and response
  shapes?
  Answer: Yes, where the feature is supported. Compatibility belongs at the API
  boundary; execution contracts should remain KalDB-owned.

- Question: Can KalDB use coordinator-visible metadata for sort scheduling?
  Answer: Yes. Finalized KalDB chunks are close to the distributed scheduling
  unit, so the query node can use chunk metadata such as timestamp ranges and
  sortable-field min/max values without asking workers on every query.

- Question: Does `search_after` fall out of this work automatically?
  Answer: No. Cursor pagination needs a stable total ordering, response sort
  tuples, and a KalDB-owned resume contract. It should remain explicit and
  separate from offset pagination.

## Public Interfaces

The external OpenSearch-compatible request and response shape should remain the
same for supported sort and pagination features.

This ADR may require internal interface changes as optimizations are introduced:

- internal query-node merge behavior may change from full sort to bounded
  top-K merge;
- internal worker requests may later carry bottom sort values;
- internal worker responses may later carry lightweight top-hit references;
- chunk metadata may later store sortable-field min/max values;
- a future internal fetch request may load full documents for final winners.

No user-facing behavior should become less compatible as a result of these
internal changes. Unsupported OpenSearch features such as `search_after` should
remain explicitly unsupported until KalDB has the needed cursor semantics.

## Proposed Changes

### Summary

Use OpenSearch as a reference design for sorted search optimization, but
implement the selected optimizations in KalDB's query/worker/chunk model.

The implementation direction is:

1. Keep local Lucene sort execution in the Lucene backend.
2. Use bounded global top-K merge on the query node.
3. Add coordinator-visible chunk metadata for sortable-field min/max values.
4. Use chunk metadata to schedule likely-winning chunks first.
5. Add staged/batched request dispatch before using bottom-sort pruning.
6. Add bottom-sort pruning for hit collection where it is safe.
7. Consider a KalDB-native query/fetch split for larger network and memory
   reductions.

### OpenSearch Ideas To Reuse Conceptually

OpenSearch separates a distributed sorted search into these phases:

```text
can_match phase:
  ask shards whether the query can match
  collect optional sort min/max
  skip shards whose query cannot match
  order remaining shards by sort min/max

query phase:
  collect lightweight top docs
  pass bottom sort values to later shard requests
  reduce candidates globally

fetch phase:
  load full documents only for final winners
```

KalDB should borrow the concepts, not the OpenSearch phase classes.

OpenSearch asks shard-holding data nodes for `can_match` and min/max because a
shard's searchable reader can change as refreshes, deletes, and merges happen.
Lucene segments are immutable, but OpenSearch schedules shards, not individual
segments.

KalDB finalized chunks are closer to the distributed scheduling unit. That
makes coordinator-visible chunk metadata a better first design for KalDB than a
worker round trip for every min/max decision.

### KalDB Optimization Phases

#### Phase 1: Bounded Global Top-K Merge

Replace full sorting of all returned hits with a bounded merge that keeps only
the best `from + size` candidates needed for the response.

This reduces query-node memory and comparison work for ordinary sorted
requests. It does not make deep offset pagination cheap:

```text
from = 1,000,000
size = 10
```

still requires identifying the first 1,000,010 winners.

#### Phase 2: Chunk Sort Metadata

Store useful sort metadata when a chunk is finalized. At minimum, timestamp
ranges can be used for timestamp-descending log queries. A broader version can
store per-field min/max values for fields that are valid sort fields.

The query node can then:

- filter chunks by existing time metadata;
- order chunks by sort-field min/max;
- send requests for likely-winning chunks earlier;
- use early results to form a stronger bottom sort value.

Worker involvement is only needed for facts that the coordinator cannot know
from metadata, such as missing stats, live/unfinalized chunks, or backend-local
query rewrite behavior.

#### Phase 3: Bottom-Sort Pruning

Bottom-sort pruning only helps if KalDB sends work in stages or batches. The
query node needs early results first, then it can compute the current bottom
sort value and attach that value to later worker requests.

If KalDB fans out every worker request at once, there are no "later" requests
to improve with the learned bottom value. In that model, bottom-sort pruning
does not reduce work, although a bounded query-node merge can still reduce
query-node memory.

With staged dispatch, once the query node has enough candidates, it can send the
current bottom sort value to later worker requests. Workers can use that value
to reduce hit collection for chunks that cannot produce competitive hits.

This must be careful with aggregations. A chunk that cannot contribute a top hit
may still contain documents that affect aggregation counts, histograms, or
terms. Pruning must distinguish:

```text
top hits:
  only need the best from + size documents

aggregations:
  may need all matching documents across all relevant chunks
```

For hit-only queries, more pruning is possible. For hit-plus-aggregation
queries, KalDB may still need to scan chunks for aggregation collection even if
hit collection can be reduced.

#### Phase 4: KalDB-Native Query/Fetch Split

A query/fetch split can reduce network bytes, serialization cost, and query-node
memory by returning lightweight hit references first and fetching full documents
only for final winners.

KalDB should define its own reference shape, for example:

```text
TopHitRef {
  chunk id
  backend row/doc reference
  sort values
  score if needed
}
```

For Lucene chunks, a doc reference may need a reader lifetime rule if it uses
Lucene doc IDs. Current KalDB avoids this issue because it searches and fetches
inside one local operation while holding the same searcher.

## Compatibility, Deprecation, and Migration Plan

The proposed direction should preserve existing supported OpenSearch-compatible
request and response behavior.

Phase 1 can be implemented without external API, metadata, or storage changes.
It changes only how the query node merges hits.

Phase 2 requires persisted or coordinator-visible chunk metadata for
sortable-field statistics. Older chunks without the new metadata should remain
queryable; they can fall back to the existing scheduling behavior.

Phase 3 requires staged or batched internal dispatch plus worker request changes
to carry optional bottom sort values. Workers that do not receive or cannot use
bottom sort values can keep the existing behavior.

Phase 4 requires a larger internal protocol change. It should be introduced
behind an internal capability boundary so KalDB can fall back to returning full
hits when a worker or chunk cannot return safe lightweight references.

No existing behavior is deprecated by this ADR.

## Test Plan

Validation should cover both correctness and the optimization boundaries.

- Unit tests for sort comparators and bounded global top-K merge, including
  ties, missing values where supported, `from`, and `size`.
- Distributed query tests showing that optimized merge returns the same hits as
  the full-sort implementation.
- Metadata scheduling tests for timestamp-descending and other sortable fields
  once chunk min/max metadata exists.
- Staged dispatch tests proving that later batches receive bottom sort values
  learned from earlier batches.
- Pruning tests proving that bottom-sort pruning does not skip documents needed
  for aggregations.
- Compatibility tests for OpenSearch-compatible response hit `sort` arrays.
- Regression tests for unsupported cursor behavior, including clear rejection of
  `search_after` until it is intentionally supported.

Performance tests should compare:

- query-node memory with full sort versus bounded merge;
- network bytes for full-hit responses versus future lightweight references;
- chunk work saved by metadata ordering and bottom-sort pruning;
- chunk work saved by staged dispatch compared with immediate fanout.

## Documentation Plan

Documentation should be updated as the phases are implemented:

- OpenSearch-compatible search API docs for supported sort and pagination
  semantics.
- Internal design docs for chunk sort metadata and bottom-sort pruning.
- Internal design docs for any future query/fetch split and reader lifetime
  rules.
- Operational notes if new chunk metadata affects storage size or upgrade
  behavior.

## Rejected Alternatives

- Directly embed OpenSearch's distributed sort runtime.
  Rejected because the relevant OpenSearch machinery assumes OpenSearch shards,
  search contexts, transport requests, fetch phases, and reduction lifecycle.
  KalDB's query/worker/chunk protocol does not match that model.

- Ask workers for sort min/max on every query as the primary design.
  Rejected as the default path for finalized chunks because chunk metadata can
  make the same scheduling information available to the query node without an
  extra round trip. Worker checks remain useful for live chunks or missing
  metadata.

- Support `search_after` by using only the last value of the user sort field.
  Rejected because a single sort-field value is not a stable cursor when
  multiple documents share that value. Cursor pagination needs a complete
  deterministic sort tuple and a KalDB-owned resume contract.

## Consequences

Benefits:

- Preserves OpenSearch-compatible API behavior while keeping execution in KalDB
  terms.
- Uses Lucene's local top-K strengths without adopting OpenSearch's shard
  runtime.
- Reduces query-node memory and global sort work with bounded top-K merge.
- Opens a path to reduce worker work through chunk metadata and bottom-sort
  pruning.
- Opens a path to reduce network bytes through a KalDB-native query/fetch split.

Costs:

- Requires KalDB-owned internal contracts for sort values, top-hit candidates,
  bottom sort values, and possibly fetch references.
- Chunk metadata for sortable-field min/max values increases metadata size and
  requires clear field-type semantics.
- Bottom-sort pruning requires staged or batched dispatch; it is not useful
  when all worker requests are already in flight.
- Bottom-sort pruning must also be carefully scoped so aggregation correctness
  is preserved.
- A query/fetch split adds protocol and reader-lifetime complexity.

Risks:

- Stale or incomplete metadata could lead to bad scheduling, and must never be
  allowed to cause incorrect skipping.
- Deep offset pagination remains expensive even with bounded top-K merge.
- Staged dispatch can add latency if batches are too small or ordering metadata
  is weak.
- Query/fetch split is a larger architectural change and should not be hidden
  inside the initial sort compatibility feature.
