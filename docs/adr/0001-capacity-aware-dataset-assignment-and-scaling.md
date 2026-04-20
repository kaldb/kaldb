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
  Answer: Open. The current implementation preserves reusable current partitions where possible, chooses the smallest partition count that satisfies the equal-share capacity check and minimum partition count, then prefers tighter available-capacity fits with partition ID as a deterministic tie-breaker. A simpler alternative is to choose among eligible partitions by partition ID order. Scaling up and scaling down are both outcomes of rerunning the same assignment algorithm with a new throughput value, but the candidate ordering policy and partition ID ordering semantics still need a final decision.

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

For shared assignments, eligible partitions are partitions that are not dedicated to another dataset. The current implementation evaluates the dataset's current partitions first, then other eligible partitions by lowest available capacity, then partition ID as a deterministic tie-breaker.

For dedicated assignments, eligible partitions are empty partitions and partitions already dedicated to the same dataset. The current implementation evaluates partitions already dedicated to the same dataset before empty partitions. Within each group, it sorts by lowest available capacity, then partition ID as a deterministic tie-breaker. The allocator must not treat selected partitions as a single pooled capacity bucket. Even if total capacity is sufficient, each selected partition must individually have enough capacity for the equal traffic share.

Manual assignment remains supported when `partition_ids` is non-empty.

### Partition Selection and Dataset Scaling Policy

The assignment algorithm should produce stable, predictable partition sets while satisfying capacity constraints. Some parts of the algorithm are fixed by correctness requirements:

- Remove the dataset being updated from the calculated live occupancy view before proposing the next assignment. This lets the allocator evaluate the dataset's current partitions as reusable capacity rather than as capacity consumed by another assignment.
- For each proposed partition count, calculate `ceil(throughput_bytes / proposed_partition_count)`.
- Reject a proposed partition count unless every selected partition can carry that per-partition requirement.
- Return the first valid proposal that also satisfies `min_number_of_partitions`.

Scaling up and scaling down are not separate operations. A throughput increase may cause the first valid proposal to require more partitions. A throughput decrease may cause the first valid proposal to require fewer partitions. In both cases, the allocator uses the same eligibility, equal-share, minimum-count, and candidate-ordering rules.

The open decision is how candidate partitions should be ordered when multiple valid assignments exist.

#### Option A: Reuse-First Capacity-Fit Selection

This is what the current implementation does, and it came from the copied Airbnb branch.

- For shared assignments, sort candidate partitions by current-membership first, then lowest available capacity, then lexicographic partition ID.
- For dedicated assignments, sort reusable same-dataset dedicated partitions before empty partitions. Within each group, sort by lowest available capacity, then lexicographic partition ID.
- Store and return the selected partition IDs in stable lexicographic partition ID order after selection.

This policy does not try to keep low-numbered partition IDs full or remove high-numbered partition IDs first. Partition catalog lifecycle and cluster-wide partition count changes are out of scope, so numeric partition order is not a primary assignment goal under this option.

Pros:

- Avoids unnecessary churn by evaluating reusable current partitions before unrelated partitions.
- Uses one algorithm for initial assignment, throughput growth, and throughput reduction.
- Chooses the fewest valid partitions, subject to the configured minimum partition count.
- Favors tighter available-capacity fits after reuse, which leaves larger remaining-capacity partitions available for future assignments that may need them.
- Keeps behavior deterministic through stable partition ID tie-breaking.

Cons:

- More complex to explain than simple partition ID ordering.
- The resulting partition set may not be the lowest-ID valid set.
- A throughput reduction may remove a lower-sorting partition ID and keep a higher-sorting partition ID if that is the tighter available-capacity fit.
- The algorithm is greedy and local to the dataset being updated; it does not globally rebalance all datasets.
- Partition ID ordering is only a deterministic tie-breaker and should not be treated as a capacity or lifecycle policy.
- This option still needs an explicit partition ID ordering choice: treat partition IDs as opaque strings sorted lexicographically, or require numeric-like identifiers sorted numerically.

#### Option B: Partition-ID-Ordered Selection

This is closer to the earlier ADR wording. The allocator would still filter by eligibility and per-partition capacity, but candidate ordering would primarily use partition ID order instead of available-capacity fit.

Pros:

- Simpler to explain and reason about in runbooks.
- Makes scale-up and scale-down examples easier to predict.
- Avoids implying that the manager is trying to optimize future placement based on a local greedy capacity-fit heuristic.
- If partition IDs are assigned in an operationally meaningful order, this can align with operator intuition.

Cons:

- May churn away from current partitions more often unless current-membership is still given priority.
- May consume larger-capacity partitions earlier than necessary and leave smaller fragments that are harder to use later.
- Requires deciding whether partition IDs are opaque strings sorted lexicographically or constrained numeric identifiers sorted numerically.
- The simpler policy would require changing the current implementation and updating tests.

#### Partition ID Ordering Choices

Both candidate-ordering options need an explicit partition ID ordering choice:

- Opaque string IDs with lexicographic ordering. This matches the current data model and current implementation because partition IDs are strings. It avoids assuming the IDs are generated from numbers, but `10` sorts before `2`.
- Numeric-like IDs with numeric ordering. This matches operator intuition if partitions are intentionally named after Kafka-style partition numbers, but it requires validating or defining what happens when a partition ID is not numeric.

### Examples

Unless stated otherwise, these examples assume `ManagerConfig.partition_assignment_config.min_number_of_partitions = 2`.

Examples that depend on candidate ordering describe the current reuse-first capacity-fit implementation. If the final decision is partition-ID-ordered selection, update those examples to match the chosen policy.

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
- Because all partitions have equal available capacity, the partition ID tie-breaker chooses `0` and `1`.
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
- Because all three partitions have equal available capacity, the partition ID tie-breaker chooses `0` and `1`.
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
- It chooses `2` and `3` because they are empty, while `0` and `1` are already occupied by a shared dataset.
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

#### Example 5: Dedicated Scaling Uses the Smallest Valid Partition Count

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
- No two-partition assignment is valid, so growth to three partitions is required by the per-partition capacity rule.

#### Example 6: Shared Growth Preserves Current Partitions and Adds the Tightest Fit

Starting state:

- Dataset `logs` is currently assigned to partitions `0` and `1`.
- After removing `logs` from the live occupancy view, partitions `0` and `1` each have `100` bytes of available capacity.
- Eligible free partition `2` has `100` bytes of available capacity.
- Eligible free partition `3` has `80` bytes of available capacity.

Growth request:

```text
UpdatePartitionAssignment(
  name = "logs",
  throughput_bytes = 240,
  partition_ids = []
)
```

Result:

- Two partitions are not enough because `ceil(240 / 2) = 120`.
- With three partitions, per-partition throughput is `ceil(240 / 3) = 80`.
- The allocator preserves current partitions `0` and `1`.
- It adds `3` before `2` because `3` is the tighter available-capacity fit that can still carry `80`.
- The resulting assignment is `0, 1, 3`.

#### Example 7: Shared Throughput Reduction Shrinks to the Tightest Current Fit

Starting state:

- Dataset `logs` is currently assigned to partitions `0`, `1`, `2`, and `3`.
- After removing `logs` from the live occupancy view, current partitions have this available capacity:

```text
0: available_capacity=100
1: available_capacity=40
2: available_capacity=50
3: available_capacity=60
```

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

- With two partitions, per-partition throughput is `ceil(80 / 2) = 40`.
- The allocator considers the dataset's current partitions first.
- Among current partitions that can carry `40`, it prefers the tighter available-capacity fit.
- The resulting assignment is `1, 2`, not `0, 1`.

#### Example 8: Growth and Shrink Are Both Capacity Replanning

The growth and shrink policy can be summarized as:

```text
grow:   current partitions first, then tightest eligible added capacity
shrink: smallest valid count using current partitions ordered by available-capacity fit
```

Result:

- When extra capacity is needed, the allocator grows to the smallest valid partition count whose equal per-partition share fits.
- When less capacity is needed, the allocator shrinks to the smallest valid partition count that still satisfies the minimum and equal-share capacity checks.
- In the current implementation, partition IDs are used as a stable tie-breaker after reuse and available-capacity ordering, not as the primary scale-up or scale-down policy.

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
- The current implementation prefers keeping a dataset on its current partitions where possible, which reduces unnecessary churn during throughput changes. If partition-ID-ordered selection is chosen instead, decide whether current partitions still get priority over unrelated partitions.
- Growth and shrink must still satisfy the same eligibility, minimum partition count, and equal-share capacity checks regardless of the chosen candidate-ordering policy.
- Manual assignment to nonexistent partition IDs must be rejected.
- Auto-assignment must fail clearly when there are not enough eligible partitions with sufficient capacity to satisfy the configured minimum partition count and per-partition capacity check.

### Rollout or Phases

1. Add partition catalog metadata through `PartitionMetadataStore`.
2. Plumb `ManagerConfig.partition_assignment_config.min_number_of_partitions` into the assignment algorithm.
3. Implement calculated partition occupancy from partition catalog plus active dataset assignments.
4. Expose the calculated view through `ListPartition`.
5. Add auto-assignment when `UpdatePartitionAssignmentRequest.partition_ids` is empty.
6. Finalize the candidate-ordering policy and ensure growth and shrink decisions use that deterministic policy.
7. Optionally add validation for manual assignment against the same capacity rules.

### Open Questions

- Should manual assignment be strictly validated against the same capacity checks as auto-assignment, or only optionally validated?
- Should additional operator-facing diagnostics be returned when no feasible assignment exists?
- Should auto-assignment keep the current reuse-first capacity-fit candidate ordering, or switch to simpler partition-ID-ordered selection?
- Should partition IDs be treated as opaque strings sorted lexicographically, or constrained numeric-like identifiers sorted numerically?

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
- Unit tests for the chosen candidate-ordering policy, including growth and shrink behavior.
- If reuse-first capacity-fit selection is retained, unit tests ensuring growth adds the tightest eligible available-capacity fit after reusable current partitions.
- If reuse-first capacity-fit selection is retained, unit tests ensuring shrink uses the smallest valid partition count and keeps reusable current partitions by available-capacity fit.
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
