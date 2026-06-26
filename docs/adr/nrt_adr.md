## GitHub Issue: Add High Availability for Indexer Using Real-Time Lucene Segment Replication

Design doc: [nrt_design.md](nrt_design.md)

### Summary

KalDB indexers currently consume data from Kafka, build Lucene indexes locally, and periodically publish immutable snapshots to S3. Once a snapshot is published, it becomes read-only and can be served by cache nodes.

However, data that has been indexed locally by an active indexer but has not yet been snapshotted is not highly available. If the indexer node goes down, a recovery indexer can take over and asynchronously re-index the missing data from Kafka, but during that recovery window, the recently indexed data is unavailable for queries.

We need to make indexer-owned data highly available by replicating Lucene segment data in near real time from the active indexer to S3, and then allowing cache nodes to periodically download and serve that data.

Lucene already supports this pattern through its NRT replication functionality, but KalDB does not currently implement it.

---

### Problem

Today, indexer availability depends on the live indexer node until a durable snapshot is published.

The current flow is roughly:

1. Indexer consumes records from Kafka.
2. Indexer writes data into a local Lucene index.
3. Indexer periodically publishes immutable snapshots to S3.
4. Cache nodes download published snapshots from S3 and serve queries.
5. If the indexer crashes before publishing a snapshot, a recovery indexer replays the data from Kafka and rebuilds the missing index state.

This works for durability, but not for high availability.

During recovery:

* Data indexed after the last published snapshot may not be queryable.
* The recovery indexer has to re-index data that had already been indexed once.
* Indexer restarts are expensive because they may require replaying data from Kafka.
* Shutdown and restart workflows are inefficient.
* Query availability depends on how quickly recovery indexing completes.

For cache nodes, this is less of an issue because we can maintain multiple replicas for the same immutable snapshot data. The availability gap is specific to active indexer data that has not yet become a finalized snapshot.

---

### Goal

Implement high availability for indexer data by introducing real-time Lucene segment replication from:

```text
Indexer -> S3 -> Cache
```

The indexer should continuously or periodically publish Lucene segment updates to S3 before the final immutable snapshot is created. Cache nodes should be able to download those replicated segments and make the in-progress indexer data queryable.

This should allow recently indexed data to remain available even if the original indexer node goes down.

---

### Proposed Approach

Use Lucene’s near-real-time replication model as the basis for KalDB indexer HA.

Lucene provides NRT replication capabilities that allow an active writer to expose newly created segments to replica nodes. In KalDB, we need to adapt this pattern to our architecture where S3 is the shared durable medium between indexers and cache nodes.

Instead of direct indexer-to-cache replication, the replication path should be:

```text
Active Indexer
   -> publishes live Lucene segment files / metadata to S3
      -> Cache nodes periodically poll or subscribe
         -> Cache nodes download updated segment data
            -> Cache nodes serve queries over the replicated index state
```

The cache node does not need to become a writer. It only needs to serve replicated Lucene data that originated from the active indexer.

---

### Requirements

#### Functional Requirements

1. **Live Lucene Segment Replication**

   * The active indexer should publish newly created Lucene segment files to S3 before the final snapshot is completed.
   * Replication should happen incrementally.
   * Already published segment files should not be uploaded repeatedly unless required.

2. **Replication Metadata**

   * The indexer should publish metadata that identifies the latest consistent replicated index state.
   * Cache nodes should only load a consistent Lucene view.
   * Partial uploads should not be visible as queryable states.

3. **Cache Node Consumption**

   * Cache nodes should periodically check S3 for replicated indexer data.
   * When new replicated data is available, cache nodes should download the new segment files.
   * Cache nodes should expose the replicated index state for queries.

4. **Indexer Restart Efficiency**

   * On restart, the indexer should be able to download the latest replicated Lucene data from S3 and resume indexing from that point where possible.
   * This should avoid replaying and re-indexing all data since the last finalized snapshot.

5. **Recovery Indexer Compatibility**

   * If an indexer fails, a recovery indexer should either:

     * resume from the latest replicated Lucene state in S3, or
     * start from the latest finalized snapshot and incrementally index only the remaining missing data.
   * The recovery path should avoid unnecessary full re-indexing.

6. **Immutable Snapshot Compatibility**

   * Existing immutable snapshot publishing should continue to work.
   * The live replicated data should eventually be finalized into the normal read-only snapshot flow.
   * Once a final snapshot is published, the temporary live replication state should be cleaned up safely.

7. **Query Availability**

   * Cache nodes should be able to serve recently indexed data from replicated Lucene segments.
   * If the active indexer goes down, queries should continue to have access to the latest replicated index state, subject to the configured replication lag.

---

### Non-Goals

* This project does not require cache nodes to become writable Lucene indexers.
* This does not require synchronous replication on every Kafka message.
* This does not require changing the immutable snapshot format unless necessary.
* This does not require replacing the current recovery indexer flow immediately.
* This does not require exactly-zero data unavailability. The initial goal is to reduce the availability gap to the replication interval.

---

### Benefits

#### Availability

* Makes active indexer data highly available.
* Reduces the window where newly indexed data is unavailable after an indexer crash.
* Allows cache nodes to serve replicated in-progress index data even if the original indexer is down.

#### Efficiency

* Avoids re-indexing large amounts of data after indexer restarts.
* Allows indexers or recovery indexers to resume from a recent replicated Lucene state.
* Reduces Kafka replay and duplicate indexing work.
* Makes shutdown and restart workflows much more efficient.

#### Operational Simplicity

* Improves behavior during rolling restarts.
* Reduces the cost of node failures.
* Makes recovery less dependent on replaying from Kafka.
* Brings active indexer data closer to the same HA model as cache data.

---

### Suggested Phased Implementation

#### Phase 1: Design and Metadata Model

Define the replication model for live indexer data.

Key questions:

* What S3 path layout should be used for live replicated indexer data?
* How do we identify the latest consistent Lucene point-in-time view?
* What metadata is required for cache nodes to safely load replicated state?
* How do we avoid exposing partially uploaded segment files?
* How does live replicated data relate to finalized immutable snapshots?
* What cleanup policy should be used after final snapshot publication?

Expected output:

* Design doc or implementation plan.
* S3 layout proposal.
* Metadata schema for replicated Lucene state.
* Failure mode analysis.

---

#### Phase 2: Indexer to S3 Segment Replication

Implement replication from the active indexer to S3.

The indexer should:

* Track newly created Lucene segment files.
* Upload new segment files to S3.
* Publish a manifest or metadata file representing a consistent replicated index state.
* Ensure metadata publication only happens after all required files are successfully uploaded.
* Avoid re-uploading files that already exist in S3.
* Emit metrics for replication lag, upload latency, bytes uploaded, and failures.

Success criteria:

* Indexer can publish live Lucene segment data to S3.
* Replicated state can be inspected and validated independently.
* Partial upload failures do not produce queryable broken states.

---

#### Phase 3: Cache Node Download and Query Support

Implement cache-side consumption of live replicated indexer data.

Cache nodes should:

* Periodically check for new replicated index metadata.
* Download newly referenced Lucene segment files.
* Open a Lucene reader over the replicated state.
* Serve queries against the latest available replicated index state.
* Continue serving the previous known-good state if the latest replication state is incomplete or invalid.

Success criteria:

* Cache nodes can query replicated indexer data before it becomes a finalized snapshot.
* Cache nodes safely roll forward to newer replicated states.
* Cache nodes do not serve partial or corrupt replicated states.

---

#### Phase 4: Indexer Restart From Replicated State

Improve indexer restart behavior.

On restart, the indexer should:

* Locate the latest replicated Lucene state in S3.
* Download the replicated data.
* Determine the Kafka offset or indexing checkpoint associated with that state.
* Resume indexing from the correct point.
* Avoid re-indexing data that is already present in the replicated Lucene state.

Success criteria:

* Restarting an indexer does not require replaying all data since the last finalized snapshot.
* Restart time is significantly reduced.
* Duplicate indexing is avoided or safely handled.

---

#### Phase 5: Recovery Indexer Integration

Update recovery indexing to use replicated state where possible.

The recovery indexer should:

* Prefer the latest replicated Lucene state over starting from the last finalized snapshot.
* Resume indexing from the checkpoint associated with the replicated state.
* Fall back to the existing recovery path if replicated state is unavailable or invalid.

Success criteria:

* Recovery indexing is faster.
* Query unavailability during recovery is reduced.
* Existing recovery behavior remains available as a fallback.

---

#### Phase 6: Cleanup and Final Snapshot Integration

Integrate live replicated data with final snapshot publication.

When a finalized immutable snapshot is published:

* Determine whether live replicated files can be reused.
* Remove temporary live replication metadata and files that are no longer needed.
* Ensure cache nodes transition cleanly from live replicated state to finalized snapshot state.
* Avoid deleting files that are still referenced by active cache readers.

Success criteria:

* Live replication data does not leak indefinitely in S3.
* Finalized snapshots remain the source of truth for immutable data.
* Cache nodes handle transition from live data to finalized snapshots safely.

---

### Failure Modes to Handle

* Indexer crashes while uploading segment files.
* Indexer uploads segment files but crashes before publishing metadata.
* Cache node sees metadata before all files are available.
* S3 read-after-write delay or transient inconsistency.
* Cache node fails while downloading replicated data.
* Recovery indexer starts while old indexer is still partially alive.
* Duplicate recovery attempts.
* Final snapshot is published while cache node is still using live replicated files.
* Replicated state is stale or corrupted.
* Kafka checkpoint and Lucene segment state are inconsistent.

---

### Metrics and Observability

Add metrics for:

* Replication lag.
* Last replicated Kafka offset or checkpoint.
* Segment upload latency.
* Segment upload failures.
* Bytes uploaded to S3.
* Number of replicated segment files.
* Cache download latency.
* Cache download failures.
* Cache replicated state freshness.
* Time to recover after indexer restart.
* Time to recover after indexer failure.
* Number of times recovery falls back to full replay.

---

### Acceptance Criteria

* Active indexer data can be replicated to S3 before final snapshot publication.
* Cache nodes can download and query replicated indexer data.
* Cache nodes only serve consistent Lucene states.
* Indexer restart can resume from replicated Lucene data when available.
* Recovery indexer can use replicated state instead of re-indexing everything from the last finalized snapshot.
* Existing snapshot publishing and cache serving behavior continues to work.
* System falls back safely to the current recovery path if replication data is unavailable.
* Replication lag and recovery behavior are observable through metrics.

---

### Open Questions

* Should live replicated indexer data be queried by cache nodes only, or also by query workers directly?
* What should the default replication interval be?
* Should replication be time-based, segment-commit-based, byte-threshold-based, or offset-threshold-based?
* What exact Lucene NRT replication APIs should we use or adapt?
* How should Kafka offsets be associated with a replicated Lucene state?
* Can final snapshots reuse live replicated segment files to avoid duplicate S3 writes?
* How should cache nodes discover which live indexer replication paths to follow?
* What is the cleanup policy for abandoned live replication state?
* Do we need fencing or generation IDs to prevent split-brain replication from multiple indexers for the same partition?
* How do we validate replicated Lucene data before serving it?

---

### Notes

The key idea is to move from a model where active indexer data is available only on the indexer node to a model where active indexer data is continuously materialized into S3 and served by cache replicas.

This gives KalDB a more robust HA story for active indexing, reduces expensive recovery re-indexing, and makes indexer restarts much faster.
