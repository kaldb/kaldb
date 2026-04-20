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

- Question: What happens if `require_dedicated_partition` is omitted for an existing dataset?
  Answer: The existing mode is preserved. A dedicated dataset stays dedicated unless the request explicitly sets `require_dedicated_partition = false`. New datasets default to shared partitions.

- Question: What happens when a dataset moves from dedicated to shared?
  Answer: The mode changes only when the request explicitly sets `require_dedicated_partition = false`. The dataset may keep the same partition IDs on that update if they are still a valid shared assignment, but after the mode flip those partitions are no longer reserved exclusively for that dataset.

- Question: Where does the minimum partition count come from?
  Answer: From manager config, specifically `ManagerConfig.partition_assignment_config.min_number_of_partitions`. The assignment algorithm should take this value as an explicit input and use it when proposing both shared and dedicated assignments.

- Question: How should the allocator choose which partition IDs to add or remove when multiple eligible assignments exist?
  Answer: Preserve current partitions where possible. When adding capacity, prefer lower-numbered eligible partitions first. When removing capacity, prefer removing the highest-numbered currently assigned partitions first, while still satisfying capacity and minimum partition count constraints.

## Public Interfaces

This proposal changes manager behavior and operational read models in the following ways:

- `UpdatePartitionAssignment` may auto-assign partitions when `partition_ids` is omitted or empty.
- `UpdatePartitionAssignment` uses throughput and dedication mode as assignment inputs rather than treating partition IDs as the only required operator input.
- Manager configuration includes `ManagerConfig.partition_assignment_config.min_number_of_partitions`, which is an input to the assignment algorithm.
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

The assignment algorithm takes `ManagerConfig.partition_assignment_config.min_number_of_partitions` as an explicit input rather than relying on a hidden default.

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

### Partition Ordering Policy

The assignment algorithm should produce stable, predictable partition sets in addition to satisfying capacity constraints.

- Preserve a dataset's current partitions where possible.
- When adding capacity, prefer the lowest-numbered eligible partitions first.
- When removing capacity, prefer removing the highest-numbered currently assigned partitions first.
- Apply the ordering policy only after filtering to partitions that are eligible for the dataset's mode and that satisfy the required per-partition capacity.
- Treat partition ID ordering as numeric ordering. If partition IDs are stored as strings, the algorithm should still compare them numerically so that `10` sorts after `2`, not before it.

This ordering matches common operational expectations from systems like Kafka and Kubernetes, where partition or shard numbering starts at `0` and grows upward.

### Pros and Cons of Lowest-First Fill / Highest-First Removal

Pros:

- Keeps assignment behavior predictable for operators and easier to explain in runbooks and dashboards.
- Aligns with Kafka-style and Kubernetes-style numbering where lower IDs are created first and are the most familiar operational reference points.
- Tends to keep lower-numbered partitions densely utilized and leaves higher-numbered partitions available for later growth.
- Makes scale down more stable by preserving the lower-numbered partitions that operators are most likely to expect to remain in service.

Cons:

- This policy must not override hard capacity constraints, so it adds ordering logic on top of the capacity calculation.
- It assumes partition IDs have a meaningful numeric order and therefore needs explicit numeric comparison semantics.
- A pure low-ID preference can be less flexible than a strategy that optimizes only for available headroom, especially if higher-numbered partitions have better spare capacity.
- If the system later introduces non-numeric or externally managed partition identifiers, this policy may need to be revisited.

### Examples

Unless stated otherwise, these examples assume `ManagerConfig.partition_assignment_config.min_number_of_partitions = 2`.

#### Example 1: Minimum Partition Count Is an Algorithm Input

Declared partition catalog:

```text
0: max_capacity=100
1: max_capacity=100
2: max_capacity=100
```

Request:

```text
UpdatePartitionAssignment(
  name = "logs",
  throughput_bytes = 20,
  partition_ids = []
)
```

Result:

- The dataset defaults to shared mode.
- The allocator still chooses two partitions because `min_number_of_partitions = 2`.
- It chooses partitions `0` and `1`, not just `0`.
- It fills the lowest-numbered eligible partitions first.
- Per-partition throughput is `ceil(20 / 2) = 10`.

If the config were `min_number_of_partitions = 3`, the same request would assign `0`, `1`, and `2`, and per-partition throughput would be `ceil(20 / 3) = 7`.

#### Example 2: Shared Auto-Assignment for a New Dataset

Declared partition catalog:

```text
0: max_capacity=100
1: max_capacity=100
2: max_capacity=100
```

Request:

```text
UpdatePartitionAssignment(
  name = "logs",
  throughput_bytes = 100,
  partition_ids = []
)
```

Result:

- The dataset defaults to shared mode.
- The allocator chooses partitions `0` and `1`.
- It chooses `0` and `1` instead of `1` and `2` because expansion prefers the lowest-numbered eligible partitions first.
- Per-partition throughput is `ceil(100 / 2) = 50`.
- `ListPartition` reports provisioned capacity `50` on `0`, `50` on `1`, and `0` on `2`.

#### Example 3: Dedicated Auto-Assignment Avoids Shared Capacity

Starting state:

- Shared dataset `search` already uses partitions `0` and `1` with throughput `100`.
- Partition catalog contains `0`, `1`, `2`, and `3`.

Request:

```text
UpdatePartitionAssignment(
  name = "payments",
  throughput_bytes = 150,
  partition_ids = [],
  require_dedicated_partition = true
)
```

Result:

- The allocator chooses partitions `2` and `3`, not `0` and `1`.
- It chooses `2` before any higher partition IDs because dedicated growth also prefers the lowest-numbered eligible partitions first.
- Per-partition throughput is `ceil(150 / 2) = 75`.
- `ListPartition` reports `2` and `3` as dedicated to `payments`.

#### Example 4: Dedicated Assignment Must Satisfy Equal Share Per Partition

Declared partition catalog:

```text
0: max_capacity=1
1: max_capacity=100
```

Request:

```text
UpdatePartitionAssignment(
  name = "audit",
  throughput_bytes = 101,
  partition_ids = [],
  require_dedicated_partition = true
)
```

Result:

- The allocator cannot treat total capacity `1 + 100 = 101` as sufficient.
- With two partitions, per-partition throughput would be `ceil(101 / 2) = 51`.
- Partition `0` cannot carry `51`, so the request fails.
- This is intentional. Dedicated assignment does not use pooled capacity math.

#### Example 5: Scaling a Dedicated Dataset Expands Onto Lower-Numbered Eligible Partitions First

Declared partition catalog:

```text
0: max_capacity=50
1: max_capacity=55
2: max_capacity=70
```

Request:

```text
UpdatePartitionAssignment(
  name = "trace-index",
  throughput_bytes = 120,
  partition_ids = [],
  require_dedicated_partition = true
)
```

Result:

- Two partitions are not enough because `ceil(120 / 2) = 60`, and only partition `2` can carry `60`.
- With three partitions, per-partition throughput is `ceil(120 / 3) = 40`.
- The allocator assigns `0`, `1`, and `2`.
- This is how scaling works in practice: the manager grows the dataset's partition set until each selected partition can carry its equal share.
- Because all three partitions are needed, the ordering policy keeps the lower-numbered partitions in the assignment rather than preferring any higher-numbered alternative.

#### Example 6: Shared Dataset Growth Adds Lower-Numbered Partitions First

Starting state:

- Dataset `logs` is currently assigned to partitions `0` and `1`.
- Eligible free partitions `2` and `3` are also available.
- All partitions have enough capacity for the dataset's equal per-partition share.

Growth request:

```text
UpdatePartitionAssignment(
  name = "logs",
  throughput_bytes = 150,
  partition_ids = []
)
```

Result:

- The allocator preserves `0` and `1`.
- It adds `2` before `3` because growth prefers lower-numbered eligible partitions first.
- The resulting assignment is `0, 1, 2`.
- If `3` had been the only eligible partition with enough capacity, the capacity rule would override the numeric preference.

#### Example 7: Throughput Reduction Shrinks by Removing Highest-Numbered Partitions First

Starting state:

- Dataset `logs` is currently assigned to partitions `0`, `1`, `2`, and `3`.
- A lower throughput now allows the dataset to fit on only two partitions while still meeting the configured minimum partition count.

Shrink request:

```text
UpdatePartitionAssignment(
  name = "logs",
  throughput_bytes = 80,
  partition_ids = []
)
```

Result:

- The allocator keeps `0` and `1`.
- It removes `3` and `2` before considering removal of `1` or `0`.
- The resulting assignment is `0, 1`.
- This keeps the lower-numbered partitions stable across contraction.

#### Example 8: Prefer Low IDs When Growing and Remove High IDs When Shrinking

The growth and shrink policy can be summarized as:

```text
grow:   0, 1   -> 0, 1, 2
shrink: 0, 1, 2, 3 -> 0, 1
```

Result:

- When extra capacity is needed, preserve the current low IDs and add the next-lowest eligible partition.
- When less capacity is needed, preserve the current low IDs and remove the highest-numbered currently assigned partitions first.
- This matches the goal of filling lower-numbered partitions first and trimming higher-numbered ones first.

#### Example 9: Preserving Dedicated Mode vs. Explicitly Clearing It

Starting state:

- Dataset `payments` is dedicated on partitions `0` and `1`.
- Current throughput is `100`.

Update 1:

```text
UpdatePartitionAssignment(
  name = "payments",
  throughput_bytes = 80,
  partition_ids = []
)
```

Result:

- The dataset remains dedicated because `require_dedicated_partition` was omitted.
- The allocator may keep the existing partitions `0` and `1`.

Update 2:

```text
UpdatePartitionAssignment(
  name = "payments",
  throughput_bytes = 80,
  partition_ids = [],
  require_dedicated_partition = false
)
```

Result:

- The dataset explicitly switches to shared mode.
- The assignment may still remain on `0` and `1` if that is a valid shared assignment.
- The important semantic change is the mode, not necessarily an immediate move to different partition IDs.

### Edge Cases and Operational Semantics

- Shared auto-assignment must not place a dataset onto partitions dedicated to another dataset.
- A dataset switching from dedicated to shared may keep the same partition IDs on the transition update if those partitions are otherwise valid for shared use.
- Omitting `require_dedicated_partition` preserves the current dataset mode. This avoids accidental mode flips during ordinary throughput updates.
- New datasets default to shared mode unless the request explicitly requires dedicated partitions.
- Auto-assignment prefers keeping a dataset on its current partitions where possible, which reduces unnecessary churn during throughput changes.
- When new capacity is needed, the allocator should fill lower-numbered eligible partitions first.
- When capacity can be removed, the allocator should prefer removing the highest-numbered currently assigned partitions first.
- Manual assignment to nonexistent partition IDs must be rejected.
- Auto-assignment must fail clearly when there are not enough eligible partitions with sufficient capacity to satisfy the configured minimum partition count and per-partition capacity check.

### Rollout or Phases

1. Add partition catalog metadata through `PartitionMetadataStore`.
2. Plumb `ManagerConfig.partition_assignment_config.min_number_of_partitions` into the assignment algorithm.
3. Implement calculated partition occupancy from partition catalog plus active dataset assignments.
4. Expose the calculated view through `ListPartition`.
5. Add auto-assignment when `UpdatePartitionAssignmentRequest.partition_ids` is empty.
6. Add deterministic partition ordering for growth and shrink decisions.
7. Optionally add validation for manual assignment against the same capacity rules.

### Open Questions

- Should manual assignment be strictly validated against the same capacity checks as auto-assignment, or only optionally validated?
- Should additional operator-facing diagnostics be returned when no feasible assignment exists?
- Should the partition ordering policy apply strictly numerically in every case, or should it be allowed to yield to stronger locality or balancing heuristics in the future?

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
- Unit tests with different values of `ManagerConfig.partition_assignment_config.min_number_of_partitions`.
- Unit tests ensuring the allocator prefers existing dataset partitions when feasible.
- Unit tests ensuring growth fills lower-numbered eligible partitions first.
- Unit tests ensuring shrink removes highest-numbered currently assigned partitions first.
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
