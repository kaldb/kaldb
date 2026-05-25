# NRT Segment Replication Design

## Status

Draft.

This document expands `docs/adr/nrt_adr.md` into an implementation design for near-real-time
Lucene segment replication. It describes the intended design, not the current experimental branch.

## Problem

KalDB indexers serve fresh data from an active Lucene chunk. When the chunk rolls over, the indexer
publishes an immutable snapshot to S3 and cache nodes serve it through the existing replica
assignment flow.

If an indexer fails before the active chunk is finalized, indexed data is durable in Kafka but not
available from cache until recovery replays Kafka, rebuilds Lucene, publishes a snapshot, and cache
nodes load it. NRT replication reduces that unavailable window to the configured replication lag
plus cache failover time.

## Goals

- Replicate active-indexer Lucene commit points to blob storage before final snapshot publication.
- Use the existing snapshot, replica, assignment, and search metadata flows where possible.
- Let cache nodes keep assigned standby copies of active chunk data.
- Publish only complete and consistent Lucene commit points.
- Use ZooKeeper watches so cache nodes react to live snapshot pointer changes.
- Preserve Astra's existing single-writer indexing assumption for Phase 3.
- Keep finalized immutable snapshots as the source of truth after rollover.
- Preserve the current recovery path as fallback.

## Non-Goals

- Cache nodes do not become Lucene writers.
- Replication is not synchronous per Kafka record.
- Query nodes do not download or host Lucene data.
- This design does not require changing the finalized snapshot storage format.
- This design does not promise exactly-zero unavailability.

## Decision Summary

1. Extend `SnapshotMetadata` instead of adding a separate `NrtChunkMetadata` object.
2. Represent active chunks as `SnapshotMetadata` with `snapshotType = LIVE`.
3. Store the current immutable NRT manifest path in the live `SnapshotMetadata.snapshotPath`.
4. The cluster manager creates normal replica metadata for live snapshots, just as it does for
   sealed snapshots.
5. Cache nodes load assigned live replicas, set a watch on the live `SnapshotMetadata` znode, and
   refresh when the NRT generation or manifest path changes.
6. The indexer uploads immutable Lucene files, writes a new immutable generation manifest, then
   updates the live `SnapshotMetadata` pointer. Phase 3 follows Astra's existing single-writer
   indexing assumption; it does not add a new NRT-only fencing or metadata CAS mechanism.
7. Cache nodes publish searchable endpoints through the existing `SearchMetadata` mechanism after
   they successfully open the replicated Lucene reader.
8. Query planning uses the existing searchable snapshot endpoints and must choose one serving
   source for each active chunk to avoid duplicate live-indexer plus NRT-cache results.

## Metadata Direction

The foundation is an expanded `SnapshotMetadata` model:

```text
SnapshotMetadata {
  snapshotId
  snapshotType = LIVE | SEALED
  indexType = LUCENE | ...
  snapshotPath
  snapshotGeneration
  startTimeEpochMs
  endTimeEpochMs
  partitionId
  maxOffset
  sizeInBytesOnDisk
  version
}
```

For sealed snapshots, `snapshotPath` points to the immutable snapshot root.

For live snapshots, `snapshotPath` points to the immutable NRT manifest object for the currently
visible generation. `snapshotGeneration` is the latest published NRT generation.

Updating `snapshotPath`, `snapshotGeneration`, offset, or time bounds on the live snapshot is the
watchable handoff from the indexer to assigned cache nodes. The ZK metadata update is the single
commit point and must be performed with metadata-store CAS.

No separate NRT metadata object is required for the first implementation. The main reason is that
ZooKeeper already gives cache nodes watches on snapshot metadata updates, and the existing replica
assignment path already knows how to assign work by snapshot ID. Keeping NRT state on live
`SnapshotMetadata` avoids a second metadata object whose lifecycle must be reconciled with the live
snapshot.

## Current Model

```text
Kafka -> Indexer active chunk -> queryable on indexer
Kafka -> Indexer finalized chunk -> S3 snapshot -> replica assignment -> queryable on cache
Kafka -> Recovery indexer -> S3 snapshot -> replica assignment -> queryable on cache
```

## Proposed Model

```text
Kafka -> Indexer active chunk
  -> live SnapshotMetadata
  -> live ReplicaMetadata
  -> cache assignment
  -> cache watches live SnapshotMetadata path/generation
  -> cache downloads immutable NRT manifest from S3
  -> cache refreshes Lucene reader
  -> cache publishes existing SearchMetadata endpoint
```

```mermaid
flowchart LR
  Kafka[(Kafka)] --> Indexer[Indexer]
  Indexer --> Blob[(Blob Store)]
  Indexer --> LiveSnap[Live SnapshotMetadata with NRT pointer]
  LiveSnap --> Manager[Cluster Manager]
  Manager --> LiveReplica[Live ReplicaMetadata]
  LiveReplica --> Assignment[Cache Assignment]
  Assignment --> Cache[Cache Node]
  LiveSnap -->|ZK watch| Cache
  Blob --> Cache
  Cache --> SearchMeta[SearchMetadata]
  SearchMeta --> Query[Query Node]
  LiveSnap --> Query
```

Control responsibilities:

- Indexer writes Lucene files and an immutable generation manifest to blob storage.
- Indexer updates live `SnapshotMetadata.snapshotPath`, `snapshotGeneration`, `maxOffset`, and time
  bounds after a valid NRT point is durable.
- Cluster manager creates and assigns replicas for live snapshots.
- Cache nodes watch assigned live snapshot metadata and refresh from the pointed NRT state.
- Cache and indexer nodes publish searchable endpoints through existing `SearchMetadata`.
- Query nodes stay stateless and choose serving URLs from metadata.

## Replication Unit

The replication unit is one active chunk for one dataset partition.

Identity:

```text
dataset/topic
partitionId
chunkId / snapshotId
writerNodeId
```

The active chunk is represented by a live `SnapshotMetadata` entry. Its assigned cache replicas use
the same snapshot ID as the unit of work.

## Blob Layout

Use a stable prefix per active chunk and immutable file objects under that prefix.

```text
nrt/v1/partitions/<partitionId>/chunks/<chunkId>/
  manifests/<zeroPaddedGeneration>-<writerNodeId>.json
  files/<luceneFileName>
```

The live snapshot's `snapshotPath` points to the currently committed immutable manifest object:

```text
nrt/v1/partitions/0/chunks/chunk-abc/manifests/00000000000000000042-indexer-7.json
```

Rules:

- Lucene files are immutable once uploaded.
- There is one immutable manifest object per published generation.
- Manifest paths include writer identity so concurrent writers for the same generation never
  overwrite each other's uncommitted manifests.
- Readers must not infer latest state by listing blob storage.
- The live `SnapshotMetadata.snapshotPath` and `snapshotGeneration` identify the latest NRT point
  cache nodes should load.
- Updating `SnapshotMetadata` happens only after all files and the immutable manifest are uploaded
  successfully.
- The metadata update uses the existing `SnapshotMetadataStore.updateSync` path. Correctness relies
  on the same single-writer ownership assumption used by sealed snapshot rollover.

## Manifest Schema

The manifest represents one complete Lucene commit point for one active chunk.

```json
{
  "version": 1,
  "snapshotId": "chunk-abc",
  "partitionId": "0",
  "writerNodeId": "indexer-0",
  "manifestGeneration": 2,
  "createdAtEpochMs": 1700000060000,
  "luceneCommitGeneration": 4,
  "startOffsetInclusive": 1000,
  "maxIndexedOffsetInclusive": 2300,
  "startTimeEpochMs": 1700000000000,
  "endTimeEpochMs": 1700000059000,
  "schemaFile": {
    "name": "schema",
    "key": "nrt/v1/partitions/0/chunks/chunk-abc/files/schema",
    "length": 1024,
    "checksum": "object-or-content-checksum"
  },
  "files": [
    {
      "name": "_0.cfe",
      "key": "nrt/v1/partitions/0/chunks/chunk-abc/files/_0.cfe",
      "length": 12345,
      "checksum": "object-or-content-checksum"
    },
    {
      "name": "segments_4",
      "key": "nrt/v1/partitions/0/chunks/chunk-abc/files/segments_4",
      "length": 456,
      "checksum": "object-or-content-checksum"
    }
  ]
}
```

Field meanings:

- `version`: manifest schema version.
- `manifestGeneration`: monotonic sequence for this live snapshot.
- `writerNodeId`: diagnostic identity of the indexer that wrote this manifest.
- `maxIndexedOffsetInclusive`: recovery checkpoint for this Lucene commit.
- `startTimeEpochMs` and `endTimeEpochMs`: query planning and chunk filtering bounds.
- `schemaFile`: schema needed to open/query the local read-only chunk.
- `files`: complete Lucene file set needed to open this commit.

The Kafka checkpoint must be written into Lucene commit metadata before publishing the manifest, so
the manifest and Lucene commit describe the same ingest point.

## Publisher Flow

```mermaid
sequenceDiagram
  participant I as Indexer
  participant Z as SnapshotMetadataStore
  participant B as Blob Store

  I->>Z: read live SnapshotMetadata
  Z-->>I: snapshotId, snapshotPath, generation, maxOffset
  I->>I: commit active Lucene index
  I->>I: collect IndexCommit files, schema, checkpoint, time bounds
  I->>B: upload missing immutable Lucene files
  I->>B: PUT immutable manifest generation
  I->>Z: update live SnapshotMetadata path + generation + offset + time bounds
  Z-->>I: update accepted or failed
```

Publisher steps:

1. Confirm the active chunk still has live `SnapshotMetadata`.
2. Commit the active Lucene index.
3. Collect the `IndexCommit` file list, schema file, checkpoint, and time bounds.
4. Upload missing immutable Lucene files.
5. Write a new immutable manifest object for `snapshotGeneration + 1`.
6. Update live `SnapshotMetadata.snapshotPath`, `snapshotGeneration`, `maxOffset`,
   `startTimeEpochMs`, and `endTimeEpochMs`.
7. Stop and retry on the next tick if the metadata update fails.

Metadata update rules:

- The new offset must not move backward.
- The new manifest generation must be greater than the previous `snapshotGeneration`.
- The new `snapshotPath` must point to the immutable manifest for the new generation.
- `snapshotType` remains `LIVE` until rollover publishes the sealed snapshot.

Failure behavior:

- File upload failure: do not upload the manifest and do not update `SnapshotMetadata`.
- Manifest upload failure: do not update `SnapshotMetadata`.
- Metadata update failure after manifest upload: stop and reload live `SnapshotMetadata` on the next
  tick. The uncommitted manifest is an orphan and will be garbage collected later.
- Split-brain/stale writer handling is not solved inside NRT Phase 3. It should be handled by the
  same future ownership/fencing design that protects sealed rollover.

## Cache Consumption Flow

```mermaid
sequenceDiagram
  participant C as Cache Node
  participant A as Assignment Store
  participant Z as SnapshotMetadataStore
  participant B as Blob Store
  participant Q as SearchMetadataStore

  C->>A: receive live replica assignment
  C->>Z: read and watch assigned live SnapshotMetadata
  Z-->>C: snapshotPath, generation, maxOffset, time bounds
  C->>B: GET manifest from snapshotPath
  B-->>C: manifest
  C->>C: validate manifest identity, offset, file list
  C->>B: download missing files
  C->>C: open Lucene reader in staging dir
  C->>C: atomically swap reader
  C->>Q: publish/update existing SearchMetadata endpoint
  Z-->>C: watch fires on path/generation update
  C->>C: repeat apply flow
```

Cache node rules:

- Follow only assigned live replicas.
- Set a ZooKeeper watch on the assigned live `SnapshotMetadata`.
- Load only the manifest referenced by `SnapshotMetadata.snapshotPath`.
- Reject manifests whose snapshot ID, partition, offset, or generation do not match expectations.
- Download only missing files, but validate every manifest file by existence, length, and checksum
  before opening a reader.
- Keep the previous known-good reader serving until the new reader opens.
- Publish or update `SearchMetadata` only after reader open succeeds.

## Manager Discovery And Assignment

Cluster manager should treat live snapshots as assignable when NRT replication is enabled.

```mermaid
flowchart TD
  A[List live SnapshotMetadata] --> B{Has snapshotPath?}
  B -->|yes| C[Create or maintain live ReplicaMetadata]
  B -->|no| D[Optionally preassign standby replica]
  C --> E[Assign replica to cache node]
  E --> F[Cache watches live SnapshotMetadata]
  G[Sealed snapshot appears] --> H[Evict live replica after sealed snapshot is searchable]
```

Manager responsibilities:

- Discover active chunks from live `SnapshotMetadata`.
- Create live replicas using the existing `ReplicaMetadata` and assignment flow.
- Assign live replicas according to NRT replica count.
- Evict live replica assignments after the sealed snapshot is searchable.
- Reconcile abandoned live replicas whose live snapshot no longer exists.

## Query Planning

Cache and indexer nodes already publish searchable endpoints for chunks they serve. NRT should use
that same `SearchMetadata` mechanism rather than introducing another publish path.

Selection order per partition/chunk coverage range:

1. Sealed immutable snapshot cache replica, if searchable.
2. Healthy live indexer serving the active chunk.
3. Searchable live-snapshot cache replica loaded from the NRT point.
4. Existing recovery fallback when no searchable source exists.

The query layer must not query the live indexer and NRT cache for the same live snapshot unless it
also has a deduplication strategy. The initial design avoids duplicates by selecting one source.

The existing searchable endpoint metadata must be sufficient to group candidates by snapshot ID,
partition, time range, and endpoint. If it is not, extend the existing search metadata model rather
than creating an NRT-specific search publication path.

## Snapshot Finalization

When an active chunk rolls over:

1. Indexer publishes the sealed immutable snapshot through the existing snapshot flow.
2. The sealed snapshot is assigned and loaded by cache nodes.
3. Query planning prefers the sealed snapshot once searchable.
4. Manager evicts live replica assignments for the chunk.
5. Blob GC deletes the NRT manifests and files after no readers can reference them.

Final snapshots remain the long-term source of truth. Reusing NRT segment files for final snapshot
upload can be an optimization later, but correctness must not depend on it.

## Indexer Restart

On startup, an indexer for a partition:

1. Acquires partition ownership through the existing indexer assignment flow.
2. Finds the latest live `SnapshotMetadata` for the partition.
3. Reads `snapshotPath` and `snapshotGeneration`.
4. Downloads and validates the referenced manifest.
5. Downloads the listed files into the local active chunk directory.
6. Opens Lucene and validates the checkpoint.
7. Starts consuming Kafka at `maxIndexedOffsetInclusive + 1`.
8. Publishes future manifests as the active single writer.

Fallback to the existing snapshot-plus-Kafka-replay path if the NRT manifest is missing, invalid,
stale, or schema-incompatible.

## Recovery Integration

Recovery may seed from NRT only after the previous writer is fenced or declared unhealthy.

Recovery flow:

1. Determine the latest sealed snapshot checkpoint.
2. Determine the latest valid NRT checkpoint from live `SnapshotMetadata.snapshotPath` and
   `snapshotGeneration`.
3. Prefer NRT if it is newer than the sealed snapshot and passes validation.
4. Download the manifest files and resume at `maxIndexedOffsetInclusive + 1`.
5. Fall back to existing replay if validation fails.

## Garbage Collection

NRT data is temporary and must be cleaned separately from sealed snapshot retention.

GC has three ownership domains: manager-owned metadata cleanup, manager-owned remote blob cleanup,
and cache-owned local disk cleanup. Manager cleanup must be periodic reconciliation, not only
watch-driven, because manager restarts and missed events must converge to the same state.

### Metadata GC

Owner: manager role.

Primary integration points:

- `CacheNodeAssignmentService` for dynamic cache assignments.
- `ReplicaEvictionService` and `ReplicaDeletionService` for slot-based replica lifecycle.
- `SnapshotDeletionService` only after NRT state has transitioned to sealed snapshot retention.

Scheduling:

- Run as part of the existing manager `AbstractScheduledService` reconciliation loops.
- Also trigger opportunistically from existing metadata listeners when live snapshots, replicas, or
  assignments change.
- Never depend only on watch/listener events; the scheduled pass is the source of convergence.

Responsibilities:

- Remove or mark live replica assignments for eviction when the corresponding sealed snapshot is
  searchable.
- Remove stale live replica assignments whose live snapshot no longer exists.
- Delete expired and unassigned live replica metadata after cache nodes have acknowledged eviction.
- Keep live `SnapshotMetadata` long enough for restart and recovery within the configured grace
  period.

### Blob GC

Owner: manager role.

Primary integration point:

- Add a dedicated `NrtBlobDeletionService` under the manager role, modeled after
  `SnapshotDeletionService`.

Why a separate service:

- `SnapshotDeletionService` deletes sealed snapshot blobs and intentionally skips live snapshots.
- NRT blob cleanup has different safety rules: immutable manifests, manifest-referenced segment
  files, live replica eviction state, sealed snapshot searchability, and an NRT grace period.

Scheduling:

- Run periodically as an `AbstractScheduledService`.
- Use a low-frequency schedule and rate-limited deletes, similar to `SnapshotDeletionService`.
- Add schedule and grace-period config under the manager-owned NRT blob deletion service when this
  phase is implemented. Do not put cleanup policy under a generic NRT replication config.

Responsibilities:

- There is one manifest object per published generation.
- For active live snapshots, read `snapshotPath` and `snapshotGeneration`, then read the current
  immutable manifest and compute the referenced file set.
- Delete abandoned NRT files and uncommitted manifests after a grace period.
- Delete the full chunk NRT directory only after the sealed snapshot is searchable, live replicas
  have been evicted/deleted, and the cleanup grace period has elapsed.
- Never delete files referenced by the current committed manifest.

### Cache-Local GC

Owner: cache node.

Primary integration points:

- The NRT cache loader/reader-swap path in `CachingChunkManager` or a helper owned by it.
- Existing `ReadOnlyChunkImpl` cleanup patterns for assignment eviction and directory cleanup.

Scheduling:

- Run event-driven after a successful NRT reader swap.
- Run event-driven when a live replica assignment is evicted.
- Run a startup safety sweep for stale staging directories and abandoned local files.
- Optionally run a low-frequency cache-side periodic sweep if local disk pressure warrants it.

Responsibilities:

- Delete failed staging directories.
- Delete local files not referenced by the current local manifest after old readers are closed.
- Never delete files used by the currently open Lucene reader.
- Do not delete remote metadata or blob-store data from cache nodes; cache nodes only clean local
  disk and update assignment state after eviction.

## Failure Handling

| Failure | Expected behavior |
| --- | --- |
| Indexer crashes before file upload completes | Live `SnapshotMetadata` is not updated; caches keep previous reader. |
| Indexer uploads files but crashes before manifest upload | Extra files are unreachable until GC. |
| Indexer uploads manifest but crashes before metadata update | Caches keep the previous reader; GC later deletes the uncommitted manifest. |
| Metadata update fails | Publisher retries after rereading live `SnapshotMetadata`. |
| Split-brain indexers publish the same live snapshot | Not addressed by NRT Phase 3; requires a shared ownership/fencing design that also protects sealed rollover. |
| Cache reads manifest with mismatched identity | Cache rejects it and keeps previous reader. |
| Manifest references missing or invalid files | Cache rejects it and keeps previous reader. |
| Reader open fails | Cache keeps previous reader and does not update `SearchMetadata`. |
| Sealed snapshot becomes searchable | Query planning prefers sealed snapshot; manager evicts live replica. |
| Kafka checkpoint and Lucene commit disagree | Manifest is invalid and must not be served or used for recovery. |

## Configuration

No NRT runtime config is required for the metadata and blob manifest foundation. Avoid adding a
generic `NrtReplicationConfig` until runtime owners consume the fields.

Future config should be added next to the owner that consumes it:

- Indexer publisher cadence should either derive from existing Lucene commit/refresh settings or
  live under an indexer-owned publisher config if separate tuning is required.
- Live replica count should reuse existing manager replica creation and assignment policy unless
  live snapshots need a manager-owned replica policy.
- Staging directories should derive from indexer/cache data directories unless operators need an
  explicit disk override.
- Blob cleanup schedule and grace period belong under the manager-owned NRT blob deletion service
  when that phase is implemented.
- Upload throttling should only be configured if the publisher implements explicit backpressure.

## Metrics And Logs

Publisher metrics:

- Latest replicated offset.
- Replication lag in records and seconds.
- Manifest generation.
- Live `SnapshotMetadata` update count.
- Files uploaded per publish.
- Bytes uploaded per publish.
- Publish latency.
- Metadata update failures.

Cache metrics:

- Latest applied offset.
- Latest applied manifest generation.
- Cache replication freshness.
- ZooKeeper watch event count.
- Download latency.
- Download failures by cause.
- Validation failures.
- Reader swap latency.
- Number of active NRT readers.

Recovery metrics:

- Recovery attempts seeded from NRT.
- Recovery attempts falling back to Kafka replay.
- Restart offset saved by NRT.
- Time from indexer failure to NRT cache serving.

Logs should include partition, snapshot ID, writer node ID, manifest generation, snapshot path, and
checkpoint offset for publish, apply, reject, and cleanup events.

## Rollout Plan

### Phase 1: Snapshot Metadata Foundation

- Extend `SnapshotMetadata` with `snapshotType`, `indexType`, `snapshotPath`,
  `snapshotGeneration`, and `version`.
- Update metadata serialization/deserialization.
- Update sealed snapshot creation to populate `snapshotPath`.
- Preserve existing search and replica metadata behavior for sealed snapshots.
- Unit test live and sealed `SnapshotMetadata` validation.
- Unit test metadata serialization for `snapshotType`, `indexType`, `snapshotPath`,
  `snapshotGeneration`, and `version`.

### Phase 2: Blobstore And Manifest Primitives

- Add manifest schema and validation.
- Add immutable generation manifest key layout.
- Read the current immutable manifest by `SnapshotMetadata.snapshotPath`.
- Defer runtime config until publisher, manager, and cache owners consume specific fields.
- Unit test manifest serialization and validation.
- Unit test blob key layout and malformed key rejection.
- Unit test manifest writes use immutable generation paths.

### Phase 3: Indexer Publishing

- Publish from the active indexer chunk when `indexerConfig.nrtEnabled` is true.
- Use the existing Lucene commit interval as the NRT publish cadence; do not add a separate NRT
  publish interval until we have a concrete reason to decouple the two.
- Commit and refresh the active Lucene index before each manifest publish.
- Upload missing Lucene files and schema.
- Write an immutable manifest for each publish.
- Update live `SnapshotMetadata.snapshotPath`, `snapshotGeneration`, offset, and time bounds.
- Keep disabled by default.
- Track the first indexed Kafka offset in `IndexingChunkManager` for the active chunk and write it
  as manifest `startOffsetInclusive`.
- Unit test publisher behavior for file upload failure, manifest upload failure, metadata update
  failure, and successful path/generation update.
- Unit test indexer wiring so enabling NRT updates the live snapshot pointer and disabled mode leaves
  live snapshots unchanged.

### Phase 4: Live Replica Assignment And Cache Loading

- Create live replicas for live snapshots when NRT is enabled.
- Assign live replicas to cache nodes.
- Watch assigned live `SnapshotMetadata`.
- Stage, validate, and open manifest file sets.
- Publish existing `SearchMetadata` only after reader open.
- Unit test live replica creation and assignment filtering.
- Unit test cache metadata watch handling for updated path and generation.
- Unit test cache staging so failed applies do not replace the previous reader.
- Unit test cache rejects manifest identity or generation mismatches.

### Phase 5: Query Source Selection

- Ensure query planning chooses one source per live snapshot coverage range.
- Prefer live indexer while healthy.
- Use NRT cache when live indexer is unavailable or stale.
- Test duplicate avoidance.
- Unit test source selection order: sealed cache, healthy live indexer, NRT cache, fallback.
- Unit test duplicate avoidance when live indexer and NRT cache both publish search metadata for
  the same live snapshot.

### Phase 6: Restart, Recovery, And Cleanup

- Seed indexer startup from valid live `snapshotPath` and `snapshotGeneration`.
- Seed recovery from valid live `snapshotPath` and `snapshotGeneration` after writer fencing.
- Evict live replicas after sealed snapshot searchability.
- Delete NRT blobs after the grace period.
- Unit test startup seed validation and fallback when the manifest is missing, stale, or invalid.
- Unit test recovery seed selection prefers valid NRT only when newer than the sealed snapshot.
- Unit test cleanup preserves files referenced by the current committed manifest.

## Test Plan

Each rollout phase must include its unit tests in the same change. The cross-phase integration test
plan is:

- Integration test cluster manager creates and assigns live replicas.
- Integration test indexer publishes a manifest and cache applies it from a live replica assignment.
- Integration test second commit updates the manifest path and live snapshot generation while cache
  downloads only missing files.
- Integration test query selection avoids duplicate live indexer plus NRT cache results.
- Integration test sealed snapshot supersedes NRT cache data.
- Integration test indexer restart from NRT checkpoint.
- Integration test recovery fallback when the NRT manifest is missing or invalid.

## ADR Review Notes

The architect feedback changes the key metadata decision:

- Use live `SnapshotMetadata` as both active chunk registry and NRT pointer.
- Do not add `NrtChunkMetadata` for the first implementation.
- Use immutable per-generation manifest objects and commit visibility through live
  `SnapshotMetadata` updates.
- Use existing live replica assignment to place NRT standby work on cache nodes.
- Use ZooKeeper watches on live snapshot metadata to trigger cache refreshes.
- Use existing `SearchMetadata` publication from cache and indexer nodes.
- Keep query planning responsible for avoiding duplicate live and NRT serving.

## Open Questions

- Should Astra add a shared partition ownership/fencing mechanism that protects both sealed
  rollover and NRT publishing?
- Should live replicas be created only after the first `snapshotPath` is published, or preassigned
  immediately when the live snapshot is created?
- Should NRT cache serve only during failover, or can it serve while the live indexer is healthy?
- What validation level is required before serving: length, checksum, Lucene reader open, or a
  heavier index check?
- What are safe defaults for publish interval, max allowed lag, and cleanup grace period?
