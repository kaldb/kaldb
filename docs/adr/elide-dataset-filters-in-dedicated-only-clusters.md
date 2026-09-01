# ADR 0009: Elide Dataset Filters in Dedicated-Only Clusters

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

This filter is required in a shared cluster because one Lucene chunk can contain documents from
multiple datasets. Without it, an index-scoped query could return documents from another dataset.

Some Astra deployments use dedicated partitions exclusively. In those deployments the filter is
redundant for every local chunk search and adds avoidable query work. These deployments need the
optimization, but do not need the complexity of proving ownership independently for every chunk.

The optimization is safe only under a historical cluster-wide invariant. Every live chunk and
every persisted chunk that remains searchable must have been produced under dedicated-only
routing, without a partition reuse or assignment transition that allowed a chunk to contain data
from different datasets. Current assignment metadata alone does not establish this property for
retained data.

## Questions

- Question: Who decides whether the injected filter can be omitted?
  Answer: The operator does, using trusted static cluster configuration. Query clients cannot
  select or override this policy.

- Question: Is it enough that current dataset assignments are dedicated?
  Answer: No. The invariant must cover all live and persisted chunks that can be selected by a
  query. Enabling the option while older shared or mixed chunks remain searchable is unsafe.

- Question: Does this remove a `service_name` clause explicitly supplied by the user?
  Answer: No. It omits only the dataset-scoping clause injected by Astra. The user's query is
  otherwise unchanged.

- Question: What happens for `_all` and `*` dataset selectors?
  Answer: Their behavior is unchanged; Astra does not inject a dataset filter for them today.

- Question: Should Astra validate the invariant at runtime?
  Answer: No. The deployment-wide option intentionally serves clusters whose operators can
  establish the invariant. Runtime validation against current assignment metadata would not prove
  the history of retained chunks.

## Public Interfaces

- Add `clusterConfig.allPartitionsDedicated`, defaulting to `false`.
- The sample configuration exposes it as `ASTRA_ALL_PARTITIONS_DEDICATED`.
- When enabled, index and cache nodes log a warning at startup that filter elision is active.
- OpenSearch and gRPC request and response shapes do not change.
- No persisted metadata or ZooKeeper schema changes are introduced.
- Correctly configured clusters return the same results. Misconfiguration can violate dataset
  isolation by returning documents from another dataset.

## Proposed Changes

### Configuration and trust boundary

`ClusterConfig` receives the following protobuf field:

```proto
// If true, every partition is dedicated and every live and persisted searchable chunk contains
// data only for the dataset whose assignment selects it. This allows data nodes to omit Astra's
// injected service_name filter. Leave false unless this covers all retained searchable data.
bool all_partitions_dedicated = 3;
```

The option is static configuration and requires a node restart to change. It must have the same
value on every index and cache node. The default remains the existing shared-cluster behavior.

The flag is an operator assertion about the entire searchable retention horizon. It must not be
enabled merely because `DatasetMetadata.usingDedicatedPartitions` is currently true. Before
enabling it, operators must establish that no searchable live or persisted chunk can contain data
for more than the dataset whose assignment selects it. This includes chunks spanning earlier
assignment changes and partitions reused by another dataset.

### Local query execution

The distributed query request remains unchanged. Each index or cache node derives an internal
`applyDatasetFilter` policy from its local trusted configuration:

```text
applyDatasetFilter = !clusterConfig.allPartitionsDedicated
```

That policy is carried by the internal `SearchQuery` and consumed immediately before the Lucene
query is built. For a concrete dataset:

```text
allPartitionsDedicated = false  add service_name = requested dataset
allPartitionsDedicated = true   execute the user query without the injected clause
```

Dataset-selector validation still runs in both modes. Explicit `service_name` clauses in the
user's query are retained. `_all` and `*` continue to bypass injected dataset scoping.

No client-controlled policy field is added to `SearchRequest`. This keeps the data-isolation
decision at the server trust boundary rather than allowing an untrusted caller to disable it.

### Observability

An index or cache node logs a warning when it starts with filter elision enabled. Per-query logging
and metrics are not required because the policy is static for the node and does not vary by chunk.

## Compatibility, Deprecation, and Migration Plan

The protobuf config change is additive and defaults to `false`, so existing configurations retain
the `service_name` filter. Astra's configuration parser ignores unknown fields, so an older binary
can read a configuration containing `allPartitionsDedicated`; it continues applying the filter.

The preferred rollout is:

1. Establish the dedicated-only invariant for the full searchable retention horizon. This may
   require waiting for shared historical chunks to expire and ensuring active chunks cannot span
   an unsafe assignment transition.
2. Deploy the new binary with `allPartitionsDedicated: false`.
3. Enable the option consistently on index and cache nodes and restart them.

A mixed-version rollout is safe after the invariant is established: older nodes retain the
redundant filter while enabled newer nodes omit it. Rolling back the binary or setting the option
back to `false` is also safe and only loses the optimization.

No stored data migration, metadata backfill, or deprecation is required.

## Test Plan

- Verify the new cluster option parses and defaults to `false` in the sample configuration.
- Verify shared-cluster policy injects the concrete dataset's `service_name` filter.
- Verify dedicated-only policy omits only Astra's injected filter.
- Verify an explicit user-authored `service_name` filter remains in the Lucene query.
- Verify `_all`, `*`, and invalid concrete dataset selectors behave as before.
- Verify index and cache service construction derives the policy from `ClusterConfig`.
- Run the existing local-search and distributed-query suites to detect query-result regressions.

## Documentation Plan

- Add `allPartitionsDedicated` and `ASTRA_ALL_PARTITIONS_DEDICATED` to the sample configuration.
- Document the historical invariant, isolation risk, default, and node consistency requirement in
  `docs/topics/Config-options.md`.
- Keep this ADR as the operational rationale and rollout guidance.

## Rejected Alternatives

- Record a dataset owner for every chunk after inspecting its contents.
  This permits optimization in clusters containing both dedicated and shared chunks, but requires
  a chunk ownership state machine, snapshot metadata changes, cache and recovery propagation, and
  per-chunk query decisions. The known target deployments are entirely dedicated, so that
  generality is not currently worth the implementation and operational complexity.

- Store a dedicated boolean or owner in ZooKeeper for every shard or chunk.
  Current assignment state does not prove historical chunk contents, and adding a remote metadata
  dependency to local query execution is unnecessary for a deployment-wide policy.

- Read current `usingDedicatedPartitions` assignment metadata for each query.
  The value is mutable and does not describe older chunks within the query's time range. It would
  also add metadata work to the query path.

- Let the query coordinator or client send a flag that disables filtering.
  Request input is not a suitable trust boundary for a data-isolation decision. Data nodes should
  derive the policy only from their own trusted configuration.

- Roll over chunks atomically whenever partition assignments change.
  Astra has no atomic transition spanning manager metadata, preprocessor routing, Kafka records,
  and indexer rollover. Building that protocol is outside the scope of this optimization.

## Consequences

Benefits:

- Dedicated-only deployments avoid a redundant Lucene filter on every concrete-dataset search.
- The hot path uses a local boolean and performs no ZooKeeper access or content inspection.
- The implementation and rollout are small, explicit, and reversible.

Costs:

- Mixed clusters do not receive the optimization, even for their dedicated chunks.
- Operators must prove and maintain a cluster-wide historical invariant.
- Enabling the option incorrectly can break dataset isolation; Astra does not validate the
  assertion at runtime.
