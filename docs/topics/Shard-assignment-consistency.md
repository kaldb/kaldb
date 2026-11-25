# Shard Assignment Consistency

This page documents the known consistency hazards for the shard-autoassignment manager APIs.

It is an engineering note, not an external API contract.

## Scope

This note covers:

- dataset partition-assignment updates
- partition create/delete manager APIs
- derived live partition state used for assignment planning and listing
- the consistency implications of cache-backed metadata reads after recent writes

The manager is assumed to be a singleton for a cluster. That avoids cross-manager write races, but
it does not by itself guarantee fresh metadata reads after recent writes.

## Current model

Manager mutation RPCs are synchronized within the manager process. That prevents two concurrent
requests handled by the same process from interleaving their writes.

This does **not** guarantee fresh reads from cache-backed metadata stores.

### Cache-backed metadata reads

Several correctness-sensitive paths still rely on `listSync()` reads from metadata stores. In the
ZooKeeper-backed path, those reads can be stale relative to a recently completed write.

This means the manager can serialize updates correctly within one process while still computing the
next decision from an older global view.

## Known hazards

### Partition delete can observe stale dataset references

The delete-partition manager API checks whether any dataset partition history still references the
partition before allowing deletion.

That check currently uses a cache-backed dataset list. After a rapid assignment update, a stale read
can incorrectly conclude that the partition is unreferenced and allow deletion of a still-referenced
partition.

Consequence:

- dataset history can point at a partition that no longer exists in the partition catalog

### Live partition planning and listing can observe a stale global view

Derived live partition state is built from cache-backed dataset and partition metadata lists.

Assignment planning and validation use that derived view. A rapid sequence of updates can therefore
plan from stale capacity or occupancy information. `ListPartition` can also render from that stale
derived view.

Consequences include:

- choosing partitions from an older provisioning view
- transient overcommit
- apparent conflicts during validation even though writes are converging correctly

### Dedicated occupancy can fail closed on a transient derived state

The derived live partition view enforces that a partition cannot appear dedicated to multiple
datasets at the same time.

With independently refreshed cached metadata lists, a transient stale combination can make the
derived view look impossible even when the underlying writes are converging. In those specific
cases, the current code fails closed by throwing an inconsistent-state exception, and manager APIs
such as `UpdatePartitionAssignment` and `ListPartition` return `FAILED_PRECONDITION` rather than
continuing with a partial or best-effort result.

This is only a partial safeguard. It catches some obviously impossible derived occupancy states, but
it does not detect every stale-read inconsistency. In particular:

- references to partitions missing from the catalog are currently warned and skipped rather than
  failing closed
- stale but internally plausible capacity or occupancy views can still pass through planning and
  validation

## Practical implications

### Safe assumptions today

- manager mutations are serialized within one manager process
- the stale-read hazards are local to cache-backed metadata views, not concurrent writes inside a
  single RPC handler

### Unsafe assumptions today

- a successful manager write implies every subsequent `listSync()` sees the new global state
- partition deletion is strongly consistent with the latest dataset assignment state
- live partition planning always reflects the latest capacity and occupancy writes
