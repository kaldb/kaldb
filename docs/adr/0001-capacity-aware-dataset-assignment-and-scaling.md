# ADR 0001: Capacity-Aware Dataset Assignment and Scaling

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

This ADR covers how the manager assigns partitions to datasets and how those assignments scale as dataset throughput changes.

It does not cover partition catalog creation, deletion, draining, or cluster-wide partition count changes. Those are separate concerns and should be handled in a separate ADR or design doc.

Before the proposed changes, partition assignment was a manual dataset metadata update. `UpdatePartitionAssignment` accepted a dataset name, a throughput value, and a list of partition IDs. The manager wrote a new `DatasetPartitionMetadata` time window onto the dataset, and downstream services used the dataset's partition assignment history to route ingest and query work.

That model had three important properties:

- Partition assignment history lived on `DatasetMetadata`.
- The manager did not maintain a catalog of valid assignable partitions.
- The manager did not know whether a partition had enough capacity for a requested assignment.

This meant operators had to choose partition IDs themselves and had to do capacity math outside the system. It also meant the manager could accept impossible assignments, such as assigning too much throughput to a partition or referring to a partition that should not exist.

The motivation for this ADR is to make dataset assignment capacity-aware while preserving historical query behavior and keeping the persisted model as small and coherent as possible.

## Questions

- Question: What is the source of truth for historical queryability?
  Answer: `DatasetMetadata` and `SnapshotMetadata`. Historical queryability must not depend on the current partition catalog.

- Question: Does this ADR include partition catalog lifecycle management?
  Answer: No. This ADR assumes a partition catalog exists and can be read, but catalog resize, deletion, and draining are out of scope.

- Question: Should capacity be modeled as pooled across selected partitions?
  Answer: No. Assignment uses equal per-partition throughput. Each selected partition must individually satisfy its share of the dataset throughput.

- Question: Should manual assignment remain supported?
  Answer: Yes. When `partition_ids` is non-empty, manual assignment remains supported. Validation against the same capacity rules is recommended and still pending final decision.

## Public Interfaces

This proposal changes manager behavior and operational read models in the following ways:

- `UpdatePartitionAssignment` may auto-assign partitions when `partition_ids` is omitted or empty.
- `UpdatePartitionAssignment` uses throughput and dedication mode as assignment inputs rather than treating partition IDs as the only required operator input.
- `ListPartition` becomes a calculated operational view over partition catalog metadata and active dataset assignments.
- Persisted metadata now includes a partition catalog in `PartitionMetadataStore`, while dataset assignment history continues to live in `DatasetMetadata`.

This ADR does not change historical query semantics. Queries still rely on dataset assignment history and snapshot metadata rather than the current partition catalog.

## Proposed Changes

### Summary

Add a persistent partition catalog and derive live partition occupancy from existing dataset assignments.

Each `PartitionMetadata` entry represents an assignable partition and its maximum provisionable capacity. The manager stores this metadata in ZooKeeper through `PartitionMetadataStore`.

Keep active and historical dataset-to-partition assignment on `DatasetMetadata`. Do not persist calculated occupancy on partitions. Instead, calculate a live partition view from:

- All declared partition catalog entries.
- All datasets' latest active partition assignment.
- Each dataset's provisioned throughput.
- Each dataset's shared or dedicated mode.

Expose that calculated view through `ListPartition`, and use the same view for assignment decisions.

Auto-assignment is also the dataset scaling mechanism. As throughput changes, the manager reevaluates the dataset's partition set using the same eligibility and per-partition capacity rules, which can expand or contract the dataset across partitions.

### Detailed Design

The word "partition" appears in several layers. They should not be treated as the same thing.

| Concept | Source of truth | Meaning | Used for |
| --- | --- | --- | --- |
| Kafka partition | Kafka | Physical/logical Kafka topic partition exists | Producing and consuming raw ingest data |
| Dataset assignment | `DatasetMetadata.partitionConfigs` | Dataset used these partition IDs during this time window | Current ingest routing and historical query mapping |
| Snapshot partition | `SnapshotMetadata.partitionId` | Indexed data in this snapshot came from this partition ID | Finding snapshots for query and restore |
| Partition catalog entry | `PartitionMetadataStore` | Manager may consider this partition as assignable capacity with this max capacity | Capacity calculation and auto-assignment |

Historical queryability depends on `DatasetMetadata` and `SnapshotMetadata`, not on `PartitionMetadataStore`.

For a query, the system:

```text
query dataset + time range
-> DatasetMetadata partition history gives partition IDs for that dataset/time
-> SnapshotMetadata is filtered by partitionId and snapshot time
-> SearchMetadata identifies query nodes serving matching snapshots
```

When `UpdatePartitionAssignmentRequest.partition_ids` is empty, auto-assign partitions:

- Preserve the existing dataset dedication mode unless `require_dedicated_partition` is explicitly set.
- Treat dataset throughput as evenly divided across selected partitions.
- Use `ceil(throughput_bytes / selected_partition_count)` as the per-partition capacity requirement.
- Reject an assignment unless every selected partition can carry that per-partition requirement.
- Use at least the configured minimum number of partitions.

For shared assignments, eligible partitions are partitions that are not dedicated to another dataset. The allocator prefers keeping the dataset's existing partitions where possible, then fills from partitions with enough available capacity.

For dedicated assignments, eligible partitions are empty partitions and partitions already dedicated to the same dataset. The allocator must not treat selected partitions as a single pooled capacity bucket. Even if total capacity is sufficient, each selected partition must individually have enough capacity for the equal traffic share.

Manual assignment remains supported when `partition_ids` is non-empty.

### Rollout or Phases

1. Add partition catalog metadata through `PartitionMetadataStore`.
2. Implement calculated partition occupancy from partition catalog plus active dataset assignments.
3. Expose the calculated view through `ListPartition`.
4. Add auto-assignment when `UpdatePartitionAssignmentRequest.partition_ids` is empty.
5. Optionally add validation for manual assignment against the same capacity rules.

### Open Questions

- Should manual assignment be strictly validated against the same capacity checks as auto-assignment, or only optionally validated?
- Should additional operator-facing diagnostics be returned when no feasible assignment exists?

## Compatibility, Deprecation, and Migration Plan

Existing dataset assignment history remains on `DatasetMetadata`, so historical query behavior remains compatible.

This proposal adds capacity-aware assignment behavior without requiring removal of manual assignment. Existing operator workflows can continue using explicit `partition_ids`.

The proposal introduces a partition catalog as persisted metadata for current assignable capacity. Calculated occupancy is derived rather than stored, which avoids a second persistent source of truth and reduces migration complexity.

This ADR does not deprecate historical query behavior or dataset assignment history. It changes manager-side assignment behavior and expands what `ListPartition` reports.

Rollback is simpler because occupancy is not persisted separately. If auto-assignment behavior needs to be disabled, dataset metadata remains the authoritative assignment record.

## Test Plan

Validate the design with a mix of unit and integration tests:

- Unit tests for occupancy calculation from partition catalog plus active dataset assignments.
- Unit tests for shared assignment eligibility and dedicated assignment eligibility.
- Unit tests for equal-share capacity checks using `ceil(throughput / partition_count)`.
- Unit tests ensuring the allocator prefers existing dataset partitions when feasible.
- Integration tests for `UpdatePartitionAssignment` with empty `partition_ids`.
- Integration tests for manual assignment behavior, including any chosen validation rules.
- Regression tests showing historical queries continue to resolve partition IDs from `DatasetMetadata` and `SnapshotMetadata` rather than current partition catalog state.
- Concurrency-focused tests around manager mutation behavior, especially if multiple manager instances can write to the same metadata store.

## Documentation Plan

If implemented, update the following documentation as needed:

- `docs/adr/0001-capacity-aware-dataset-assignment-and-scaling.md`
- Manager API documentation for `UpdatePartitionAssignment`
- Operator documentation for `ListPartition`
- Runbooks or operational docs that describe dataset assignment behavior

If a larger implementation design is written later, link this ADR to that design doc rather than expanding the ADR indefinitely.

## Rejected Alternatives

- Keep only manual assignment.
  This leaves capacity math outside the system and allows impossible assignments to be persisted.

- Persist partition occupancy as first-class metadata.
  This duplicates information already implied by dataset metadata and creates reconciliation problems.

- Treat selected partitions as a pooled capacity bucket.
  This does not match the current dataset assignment model, which assumes equal throughput share across assigned partitions.

- Expand this ADR to include partition catalog lifecycle and cluster-wide partition scaling.
  That is a separate problem with different operational and failure-mode tradeoffs.

## Consequences

- Operators can create a partition catalog and ask the manager to choose dataset assignments automatically.
- The manager now has enough information to avoid some invalid assignments before they are persisted.
- `ListPartition` becomes a calculated operational view: it reports max capacity, current provisioned capacity, available capacity, and occupancy based on current dataset metadata.
- Historical queryability remains anchored in dataset assignment history and snapshot metadata, not in calculated partition occupancy.
- The manager owns more assignment logic than it should long-term. The current implementation keeps the assignment planner inside `ManagerApiGrpc`, which mixes transport, metadata orchestration, capacity calculation, and allocation policy. Once behavior is pinned down, the planner should be extracted into a testable domain model.
- This design assumes a single manager writer for assignment changes. The current code synchronizes manager mutation RPCs inside one manager process, but it does not solve cross-process write races if multiple manager instances are deployed against the same metadata store.
