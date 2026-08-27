# ADR 0009: Elide Dataset Filters for Exclusive Chunks

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

Astra scopes a search for a concrete OpenSearch index by adding a Lucene filter equivalent to:

```text
service_name = requested dataset
```

The filter is required when a Lucene chunk contains documents from multiple datasets. Without it,
an index-scoped query could return documents from another dataset.

Some datasets use dedicated Kafka partitions, which makes content-exclusive chunks likely. The
assignment alone does not prove exclusivity, however, because preprocessors and indexers can
observe assignment changes at different times and an active chunk can span a change. When the
documents actually written to a chunk all belong to one dataset, the injected `service_name`
filter matches every document and performs no logical filtering. Removing that redundant clause
avoids executing it for every query against the chunk.

The optimization must be decided independently for each chunk. A single distributed query can
select both exclusive and shared chunks, especially across assignment changes and historical query
windows. Requiring every selected chunk to be exclusive would unnecessarily retain the filter on
chunks where it is redundant.

Query routing does not prove that every selected chunk contains the requested dataset. It selects
candidate chunks using partition and time-window overlap, then relies on the injected
`service_name` filter for final dataset isolation. For example, `search` and `payments` can share a
partition while a particular chunk happens to contain only `search` documents. That chunk can
still be selected for a `payments` query. A content-exclusive boolean would not be enough to omit
the filter safely; query execution must verify that the chunk's exclusive owner is `payments`.

Current dataset and partition-assignment metadata is not sufficient to make this decision safely:

- `usingDedicatedPartitions` describes a dataset's current assignment mode, not the contents of an
  individual chunk.
- Historical partition-assignment windows do not record whether the assignment was shared or
  dedicated.
- An active indexing chunk can remain open across an assignment change unless the chunk is
  explicitly rolled over.
- Snapshot metadata records the Kafka partition ID, but not whether the resulting chunk is
  exclusive to a dataset.

Query-time inspection of Lucene term statistics could determine whether the filter currently
matches every document, but it would add term-dictionary work to the query hot path. Deriving
ownership from partition assignments would require a coordinated transition between ZooKeeper,
preprocessors, Kafka, and indexers that Astra does not currently provide.

Instead, the indexer can derive ownership from the documents actually added to each chunk. The
system already stores per-snapshot metadata in ZooKeeper and loads that metadata when chunks are
restored. Persisting the content-derived ownership assertion with the chunk makes the query-time
decision a local comparison without relying on assignment timing.

## Questions

- Question: Should filter removal be decided once for the entire distributed query?
  Answer: No. It should be decided independently for every local chunk.

- Question: Is a boolean such as `dedicated = true` sufficient?
  Answer: No. The query executor must also know which dataset exclusively owns the chunk. The
  metadata should carry an optional dataset name. Query routing intentionally overselects chunks,
  so a chunk containing only `search` documents can still be considered for a `payments` query.

- Question: Should content inspection and dedicated-assignment metadata jointly determine whether
  a chunk is exclusive?
  Answer: No. Content ownership is the correctness assertion and must not depend on mutable
  assignment state.

- Question: Does current partition dedication prove that an existing chunk is exclusive?
  Answer: No. The partition may previously have been shared, and an active chunk may span an
  assignment change. Ownership must be derived from the documents actually indexed into the chunk.

- Question: Should data nodes read ZooKeeper synchronously for every query?
  Answer: No. Data nodes already use ZooKeeper metadata, but the query hot path should use ownership
  maintained by the local indexing chunk or loaded when a snapshot is restored.

- Question: Should assignment changes trigger chunk rollover so ownership can be assigned at chunk
  creation?
  Answer: No. Correctly ordering a manager update, preprocessor routing changes, Kafka records, and
  indexer rollover would require a new distributed transition protocol. Content-derived ownership
  provides the required guarantee locally without that coordination.

- Question: What happens when ownership is absent or uncertain?
  Answer: Astra retains the `service_name` filter. The optimization must fail closed.

- Question: Does this remove a `service_name` clause supplied explicitly by the user?
  Answer: No. It applies only to the dataset-scoping filter injected by Astra.

## Public Interfaces

- Add an optional `exclusive_dataset` field to persisted snapshot metadata. The field asserts that
  every document in that chunk belongs to the named dataset.
- Propagate `exclusive_dataset` into the in-memory chunk metadata used by indexing, cache, and
  recovery nodes.
- Existing snapshot metadata without this field remains readable. Absence means shared or unknown,
  and Astra retains the filter.
- OpenSearch request and response shapes do not change.
- No configuration, environment variable, CLI, or operator workflow changes are required.
- Query results do not change. The proposal removes only a clause proven redundant by chunk
  ownership metadata.

## Proposed Changes

### Summary

Track the optional exclusive dataset owner from the documents added to each Lucene chunk and
persist the final assertion with the snapshot. Immediately before building the local Lucene query,
omit Astra's injected `service_name` filter when the requested dataset equals the chunk's exclusive
owner. Retain the filter for mixed, unknown, mismatched, and legacy chunks.

### Ownership metadata

Add an optional field to `SnapshotMetadata`:

```proto
// Dataset that exclusively owns every document in this snapshot. Leave unset for shared or
// unknown snapshots. Query execution may omit its injected dataset filter only when this value
// equals the requested dataset.
optional string exclusive_dataset = 10;
```

The corresponding Java `SnapshotMetadata` and `ChunkInfo` models carry the same optional value.
The snapshot serializer persists it in the existing snapshot ZooKeeper node. A separate ZooKeeper
node per chunk is not necessary.

The value is an assertion about chunk contents, not a copy of the partition's assignment mode:

```text
exclusive_dataset = "payments"  every document belongs to payments
exclusive_dataset absent        shared, mixed, legacy, or otherwise unproven
```

Empty strings are invalid owners. Unknown state must be represented by absence.

Dedicated-assignment metadata must not be consulted when computing or restoring this value. Its
meaning can change during the lifetime of a chunk, and the current `usingDedicatedPartitions`
value does not preserve the mode associated with each historical assignment window.

### Content-derived ownership during indexing

An indexing chunk starts with an in-memory ownership state of `UNKNOWN`. Its single ingestion
writer updates that state from the effective `service_name` of every document:

```text
UNKNOWN + payments document            -> EXCLUSIVE(payments)
EXCLUSIVE(payments) + payments document -> EXCLUSIVE(payments)
EXCLUSIVE(payments) + search document   -> MIXED
MIXED + any document                    -> MIXED
```

`MIXED` is permanent for the lifetime of the chunk. A chunk does not become exclusive again merely
because its recent documents belong to one dataset.

The ownership tracker and Lucene document builder must use the same effective `service_name`
derivation, including the same behavior for a missing field. That logic should have one shared
implementation so ownership metadata cannot disagree with the indexed term.

The state transition must occur before the document is added to the Lucene store. A concurrent
query may therefore retain the filter before a newly added document is visible, which is
conservative. It must never observe an exclusive owner after a mismatching document becomes
searchable.

This state machine does not require partition-assignment metadata on indexers and does not require
chunks to roll over when assignments change. It proves the property needed by query execution from
the chunk's actual contents.

### Snapshot, cache, and recovery propagation

For a live indexing chunk, query execution reads its current in-memory ownership state. When the
chunk is rolled and snapshotted, only `EXCLUSIVE(dataset)` persists `exclusive_dataset`; `UNKNOWN`
and `MIXED` leave the field absent. Cache and recovery nodes copy the persisted value from
`SnapshotMetadata` into `ChunkInfo` when opening the immutable chunk. Legacy snapshots without the
field are treated as unknown.

The ownership value should not be recomputed from the current dataset assignment when an old
snapshot is restored. Current assignments may differ from the assignment under which the snapshot
was written.

### Local query execution

`ChunkManagerBase` already invokes each selected chunk independently. For each concrete dataset
query, local execution applies this rule immediately before constructing the Lucene query:

```text
if chunk.exclusive_dataset == requested_dataset:
    execute the user's query without Astra's injected service_name filter
else:
    execute the user's query with service_name = requested_dataset
```

`_all` and `*` queries continue to bypass dataset scoping as they do today. Explicit user-authored
`service_name` clauses remain part of the user query.

No per-request ZooKeeper read is performed. The decision uses only the `ChunkInfo` already attached
to the chunk being searched.

### Failure behavior and observability

Missing, blank, inconsistent, or mismatched ownership metadata always retains the filter.

The implementation should expose counters for local chunk searches where the injected filter was:

- elided because ownership matched;
- retained because the chunk was shared or ownership was unknown;
- retained because the recorded owner did not match the requested dataset.

These counters allow operators to verify adoption and quantify the optimization without logging
per-query ownership details.

## Compatibility, Deprecation, and Migration Plan

The snapshot metadata change is additive. Existing ZooKeeper records do not have
`exclusive_dataset`; deserialization treats the field as absent and query execution retains the
existing filter. No backfill is required for correctness.

Newly created chunks begin tracking ownership after writers understand the new field. During a
rolling deployment, old writers produce unknown snapshots and new readers retain the filter for
them. New writers may produce the field while old readers ignore it, so rollback preserves
correctness but loses the optimization.

Old snapshots may be left unoptimized until normal retention removes them. A separate migration
that proves and backfills historical ownership is outside this ADR.

## Rejected Alternatives

- Check `DatasetMetadata.usingDedicatedPartitions` once for the whole query.
  Rejected because it is current dataset state, does not describe historical chunks, and prevents
  safe per-chunk decisions for mixed queries.

- Require both content exclusivity and dedicated-assignment metadata before recording ownership.
  Rejected because assignment mode is mutable, is not preserved on historical assignment windows,
  and cannot prove what was written to a chunk.

- Use current partition occupancy at query time.
  Rejected because current occupancy does not prove the contents of an existing chunk and would
  require metadata access in the query path.

- Roll over chunks when partition assignments change and set ownership from the new assignment.
  Rejected because Astra has no atomic transition spanning the ZooKeeper assignment update,
  preprocessor routing caches, records already written to Kafka, and indexer rollover. A stale
  producer or reordered observation could put a mismatching document into a supposedly exclusive
  chunk.

- Read an ownership flag synchronously from ZooKeeper for every chunk query.
  Rejected because query execution should not depend on a remote metadata round trip when the
  immutable value can be loaded with the chunk.

- Store only a `dedicated` boolean.
  Rejected because it does not identify the owner and cannot prove that the requested dataset is
  the one isolated in the chunk.

- Inspect Lucene `service_name` term statistics on every query.
  Rejected because it adds repeated segment-level term-dictionary work to the query hot path.

- Require every chunk selected by a distributed query to be exclusive before removing any filter.
  Rejected because query construction already occurs per chunk, so shared chunks should not prevent
  optimization of exclusive chunks in the same request.

## Consequences

Benefits:

- Content-exclusive chunks avoid a redundant Lucene filter using an O(1) local metadata
  comparison.
- Queries spanning exclusive and mixed chunks are optimized safely at chunk granularity.
- Query execution performs no additional ZooKeeper or Lucene term-statistics lookup.
- Persisted ownership remains meaningful across assignment changes and historical queries.
- Legacy and uncertain chunks fail closed by retaining the existing filter.

Costs:

- Snapshot metadata and in-memory chunk metadata gain a new field.
- Indexing performs one ownership-state comparison for every document.
- The effective `service_name` derivation must be shared between ownership tracking and Lucene
  document construction.
- Existing snapshots are not optimized unless ownership is proven and backfilled separately.

Risks:

- A disagreement between ownership tracking and the effective indexed `service_name` could make
  metadata incorrect and cause cross-dataset query results. The two paths must share one derivation.
- Incorrect publication ordering between the ownership state and Lucene visibility could allow a
  query to use stale exclusive ownership. The state must become mixed before the mismatching
  document is added.
