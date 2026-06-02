# Shard Coverage Failures Should Set `_shards.failed`

## Status

Proposed.

## Problem

Astra can return a partial OpenSearch search result when logical shard coverage is lost during
query execution.

One concrete example is:

- a required rolled snapshot matches the query
- but there is no queryable `SearchMetadata` for it, for example because no free cache slot was
  available to load it

More broadly, the same customer-visible problem exists any time a shard-sized unit of logical
coverage is lost.

In those cases:

- the response has a gap
- but `_shards.failed` can remain `0`
- so Dashboards shows no indication that the result is incomplete

## Scope

This file only covers the narrow customer-visible rule that incomplete logical shard coverage
should surface through `_shards.failed`.

## Clarification

The OpenSearch response contract is shard-oriented.

Dashboards reacts to:

- `_shards.total`
- `_shards.failed`

It does not understand Astra-specific node-health counters such as:

- `failedNodes`
- `totalNodes`

Those node counters should not be repurposed to mean lost logical shard coverage.

## Proposed Decision

For this narrow scope, if Astra loses logical shard coverage during a search, it should:

1. count that as a failed shard
2. leave node-failure counters unchanged
3. surface the failure through the OpenSearch `_shards` response fields derived from snapshot
   coverage

This includes, for example:

- required snapshots with no queryable `SearchMetadata`
- failed distributed subrequests that were responsible for logical shard coverage
- local search failures that are represented as shard-level error results

The four cases are encoded once as named factories on `SearchResult` —
`localHardFailure`, `localSoftFailure`, `failedDistributedSubrequest(N)`, and
`missingQueryableSnapshotCoverage(N)` — each fixing the
`(requestedSnapshots, fulfilledSnapshots, failedNodes, totalNodes)` tuple. See the
catalog comment at the top of
`astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java` for the
authoritative invariant.

## Consequences

Positive:

- Dashboards can show that the result is incomplete
- node-health counters remain node-oriented

Tradeoff:

- Astra reuses snapshot coverage accounting for `_shards`, using `requestedSnapshots` and
  `fulfilledSnapshots` as the internal coverage model

## Addendum: Why `_shards.skipped` Should Stay `0`

OpenSearch uses `_shards.skipped` for shards that were in scope for the request but were later
pruned by a prefilter phase because the engine could prove they could not match.

Astra does not currently have an equivalent query-aware shard prefilter phase.

What Astra does have is earlier metadata filtering:

- dataset partition filtering
- snapshot time-range filtering

That filtering defines which logical shards are candidates for the request in the first place. It
is not the same as OpenSearch-style skipped-shard accounting.

As a result:

- snapshots outside the dataset/time window are not "skipped"; they were never in scope
- unused replicas are not "skipped"; OpenSearch `skipped` is not a replica-selection counter
- matched snapshots with no queryable `SearchMetadata` are not "skipped"; they are incomplete
  shard coverage and should contribute to `_shards.failed`

Until Astra implements an actual prefilter step that proves an in-scope logical shard cannot
match, `_shards.skipped` should remain `0`.
