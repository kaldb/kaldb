# ADR 0000: Replica-Aware Skipped Shards

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

Astra currently reports a failed shard only when querying a selected shard/node fails. It also reports total snapshots and snapshots with replicas, but it does not expose whether an incomplete distributed response missed logical chunk data. This can make OpenSearch Dashboards show a clean UI even when query results are partial.

The customer preference is:

- If another replica satisfies the same logical chunk, report the missed replica as `skipped`.
- If no replica returns for a logical chunk, report it as `failed`.
- Emit Prometheus counters for both skipped and failed shard outcomes.

This distinction matters operationally. In a `rep=1` deployment, a cache-side timeout means data is missing. Reporting that as `skipped` would silence the OpenSearch Dashboards shard-failure banner even though the result is incomplete. Reporting it as `failed` keeps the degradation visible.

OpenSearch and Elasticsearch search responses include `_shards.total`, `_shards.successful`, `_shards.skipped`, and `_shards.failed`. OpenSearch Dashboards surfaces failed shards as user-visible shard-failure banners, while skipped shards are treated as non-error accounting.

## Questions

- Question: Should an incomplete logical chunk be reported as `failed` or `skipped`?
  Answer: `failed`. If no replica returns for a logical snapshot/chunk required by the query, result completeness is affected.

- Question: When should Astra report `skipped`?
  Answer: Only when a concrete replica attempt does not return, but another replica returns data for the same logical snapshot/chunk.

- Question: What should happen before replica-aware retry is implemented?
  Answer: Prefer `failed` over `skipped`. Noisy dashboards are better than silently incomplete results.

- Question: Should OpenSearch `_shards.total` count logical chunks or concrete shard attempts?
  Answer: Use concrete OpenSearch-visible shard accounting for `_shards`: `total == successful + skipped + failed`. Keep Astra's internal `total_snapshots` as logical chunk count.

## Public Interfaces

This proposal changes externally visible, operational, and compatibility-sensitive behavior.

- OpenSearch-compatible search responses will include `_shards.successful` and `_shards.skipped` in addition to existing `total` and `failed`.
- OpenSearch-compatible `_shards.failed` will become non-zero when a required logical snapshot/chunk has no successful replica.
- OpenSearch-compatible `_shards.skipped` will become non-zero when a failed/unused replica attempt is covered by another successful replica.
- Internal gRPC `AstraSearch.SearchResult` will carry explicit snapshot-level shard accounting.
- Metrics will be added:
  - `astra_query_skipped_shards_total`
  - `astra_query_failed_shards_total`
- Query-visible behavior changes: incomplete results become visible as failed shards in OpenSearch Dashboards.

No config flag is proposed. The new behavior should be the default because it fixes silent partial results.

## Proposed Changes

### Summary

Add snapshot-level query outcome accounting to Astra's search result path, then use that accounting to populate OpenSearch-compatible shard metadata.

The desired semantics are:

```text
failed = no replica returned data for a required logical snapshot/chunk
skipped = a replica attempt did not return, but another replica returned the same logical snapshot/chunk
successful = at least one replica returned the logical snapshot/chunk
```

Example with `rep=1` and one timed-out cache shard:

```json
"_shards": {
  "total": 1,
  "successful": 0,
  "skipped": 0,
  "failed": 1
}
```

Example with two replicas where the first times out and the second succeeds:

```json
"_shards": {
  "total": 2,
  "successful": 1,
  "skipped": 1,
  "failed": 0
}
```

Astra's internal logical counters remain:

```text
total_snapshots = 1
snapshots_with_replicas = 1
```

### Detailed Design

Add fields to `astra/src/main/proto/astra_search.proto`:

```proto
message SearchResult {
  repeated string hits = 3;
  bytes internal_aggregations = 4;
  int64 took_micros = 5;

  int32 failed_nodes = 6;
  int32 total_nodes = 7;
  int32 total_snapshots = 8;
  int32 snapshots_with_replicas = 9;
  int32 failed_snapshots = 10;
  int32 skipped_snapshots = 11;
  repeated string successful_snapshot_ids = 12;
}
```

Update these classes to carry and aggregate the new fields:

- `astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java`
- `astra/src/main/java/com/slack/astra/logstore/search/SearchResultUtils.java`
- `astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java`

`SearchResult` should carry:

```java
public final int failedSnapshots;
public final int skippedSnapshots;
public final List<String> successfulSnapshotIds;
```

`SearchResultAggregatorImpl` should sum `failedSnapshots`, sum `skippedSnapshots`, and merge `successfulSnapshotIds`.

### Local Query Accounting

Update `astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java` so local chunk query results retain the chunk ID that succeeded.

Current behavior queries each local chunk and aggregates results, but the returned `SearchResult` does not identify which chunk IDs succeeded. The distributed coordinator needs those IDs to decide whether a requested logical snapshot was fulfilled.

Implementation approach:

- Keep each local chunk subtask paired with `chunk.id()`.
- If the chunk query succeeds and returns `snapshotsWithReplicas > 0`, add that chunk ID to `successfulSnapshotIds`.
- If the chunk subtask fails or times out, return a search result with `failedSnapshots = 1`.
- Leave `LogIndexSearcherImpl` unaware of chunk identity. The chunk manager owns the chunk ID and should attach it.

### Distributed Query Accounting

Update `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`.

The coordinator currently selects one `SearchMetadata` entry per snapshot, batches snapshot IDs by node URL, queries those nodes, and aggregates node responses. That is enough for one-shot fanout, but not enough to classify covered replica misses.

Add a retryable search plan:

```java
private record SnapshotSearchPlan(String snapshotName, List<SearchMetadata> candidates) {}
```

For each logical snapshot:

- Preserve the existing preferred candidate selection as the first candidate.
- Keep the remaining candidates as fallback replicas.
- Query unresolved snapshots in batches grouped by node URL.
- Track `fulfilledSnapshotIds`.
- Track failed replica attempt counts by snapshot ID.
- Retry unresolved snapshots against remaining candidates until fulfilled, no candidates remain, or the query deadline is reached.

Final classification:

```text
failedSnapshots = snapshots requested - fulfilledSnapshotIds
skippedSnapshots = count of failed replica attempts whose snapshot ID is in fulfilledSnapshotIds
```

Only final classification should drive response fields and metrics. Raw failed attempts are not customer-visible failures when another replica covers the same logical chunk.

### OpenSearch Response Mapping

Update:

- `astra/src/main/java/com/slack/astra/elasticsearchApi/searchResponse/EsSearchResponse.java`
- `astra/src/main/java/com/slack/astra/elasticsearchApi/ElasticsearchApiService.java`

Change `_shards` construction from:

```json
{
  "total": 1,
  "failed": 0
}
```

to:

```json
{
  "total": 1,
  "successful": 1,
  "skipped": 0,
  "failed": 0
}
```

Map fields as:

```text
successful = snapshots_with_replicas
skipped = skipped_snapshots
failed = failed_snapshots
total = successful + skipped + failed
```

This intentionally uses concrete shard-attempt accounting for OpenSearch `_shards.total`, while Astra's existing `total_snapshots` remains logical chunk accounting.

### Metrics

Add counters in `AstraDistributedQueryService`:

```java
public static final String ASTRA_QUERY_SKIPPED_SHARDS_TOTAL =
    "astra_query_skipped_shards_total";
public static final String ASTRA_QUERY_FAILED_SHARDS_TOTAL =
    "astra_query_failed_shards_total";
```

Increment from final aggregated classification:

```text
astra_query_failed_shards_total += failed_snapshots
astra_query_skipped_shards_total += skipped_snapshots
```

Do not tag these counters with snapshot IDs, chunk IDs, or node URLs. Those labels are too high-cardinality for Prometheus.

### Rollout

Implement in phases:

1. Add snapshot-level fields to `SearchResult` and protobuf conversion.
2. Aggregate the new fields through `SearchResultAggregatorImpl`.
3. Attach successful local chunk IDs in `ChunkManagerBase`.
4. Update OpenSearch `_shards` response shape to include `successful`, `skipped`, and `failed`.
5. Mark unresolved distributed snapshots as `failed`.
6. Add replica fallback and classify replica-covered misses as `skipped`.
7. Emit Prometheus counters.

The minimum safe behavior is phase 5. If phase 6 is deferred, unresolved snapshots should still be reported as `failed`, not `skipped`.

### Files To Modify

- `astra/src/main/proto/astra_search.proto`
- `astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java`
- `astra/src/main/java/com/slack/astra/logstore/search/SearchResultUtils.java`
- `astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java`
- `astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java`
- `astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcherImpl.java`
- `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`
- `astra/src/main/java/com/slack/astra/elasticsearchApi/searchResponse/EsSearchResponse.java`
- `astra/src/main/java/com/slack/astra/elasticsearchApi/ElasticsearchApiService.java`

## Implementation Details

### Task 1: Add Snapshot Accounting Fields

Modify `astra/src/main/proto/astra_search.proto` by adding fields `10`, `11`, and `12` to `SearchResult`:

```proto
int32 failed_snapshots = 10;
int32 skipped_snapshots = 11;
repeated string successful_snapshot_ids = 12;
```

Modify `astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java`:

- Add `failedSnapshots`, `skippedSnapshots`, and `successfulSnapshotIds`.
- Update the main constructor to accept those fields between `snapshotsWithReplicas` and `internalAggregation`.
- Update `empty()`, `error()`, and `soft_error()` to set new fields to zero and `List.of()`.
- Add `error(int failedSnapshots)` for call sites that know how many logical snapshots failed.
- Update `toString`, `equals`, and `hashCode`.

Modify `astra/src/main/java/com/slack/astra/logstore/search/SearchResultUtils.java`:

- `fromSearchResultProto` should read `getFailedSnapshots()`, `getSkippedSnapshots()`, and `getSuccessfulSnapshotIdsList()`.
- `toSearchResultProto` should set `failedSnapshots`, `skippedSnapshots`, and `successfulSnapshotIds`.
- Add trace tags for failed/skipped snapshot counts and successful snapshot ID count.

Test with `astra/src/test/java/com/slack/astra/server/SearchResultTest.java` by extending `testSearchResultObjectConversions` to assert the new fields survive object-to-proto-to-object conversion.

### Task 2: Aggregate New Fields

Modify `astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java`:

```java
int failedSnapshots = 0;
int skippedSnapshots = 0;
List<String> successfulSnapshotIds = new ArrayList<>();

for (SearchResult<T> searchResult : searchResults) {
  failedSnapshots += searchResult.failedSnapshots;
  skippedSnapshots += searchResult.skippedSnapshots;
  successfulSnapshotIds.addAll(searchResult.successfulSnapshotIds);
}
```

Pass the aggregated values into the returned `SearchResult`.

Update all existing `SearchResult` constructor call sites in `astra/src/test/java/com/slack/astra/logstore/search/SearchResultAggregatorImplTest.java` to include the new constructor args. Add a focused test that combines two results and verifies:

```text
failedSnapshots is summed
skippedSnapshots is summed
successfulSnapshotIds are concatenated
```

### Task 3: Attach Successful Local Chunk IDs

Modify `astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java`.

Introduce a private record to retain chunk identity through structured task completion:

```java
private record ChunkSearchTask<T>(
    String chunkId, StructuredTaskScope.Subtask<SearchResult<T>> subtask) {}
```

When creating local chunk query subtasks, wrap each subtask with the chunk ID. When collecting a successful result:

- If `result.snapshotsWithReplicas > 0`, return a copy of the result with `successfulSnapshotIds = List.of(chunkId)`.
- If the task fails or times out, return `SearchResult.error(1)`.

Modify `astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcherImpl.java` only to satisfy the new `SearchResult` constructor. Do not give `LogIndexSearcherImpl` chunk identity responsibility.

Add or update `astra/src/test/java/com/slack/astra/logstore/search/AstraLocalQueryServiceTest.java` to assert a local query response includes the queried chunk ID in `successful_snapshot_ids`.

### Task 4: Fix OpenSearch `_shards` Shape

Modify `astra/src/main/java/com/slack/astra/elasticsearchApi/searchResponse/EsSearchResponse.java`.

Replace the current two-field shard builder:

```java
public Builder shardsMetadata(int total, int failed)
```

with:

```java
public Builder shardsMetadata(int successful, int skipped, int failed) {
  this.shardsMetadata =
      Map.of(
          "total", successful + skipped + failed,
          "successful", successful,
          "skipped", skipped,
          "failed", failed);
  return this;
}
```

Modify `astra/src/main/java/com/slack/astra/elasticsearchApi/ElasticsearchApiService.java` so both success and error response paths call:

```java
.shardsMetadata(
    searchResult.getSnapshotsWithReplicas(),
    searchResult.getSkippedSnapshots(),
    searchResult.getFailedSnapshots())
```

While editing this block, fix the existing tracing typo so `resultTotalSnapshots` uses `searchResult.getTotalSnapshots()`, not `searchResult.getTotalNodes()`.

Update `astra/src/test/java/com/slack/astra/elasticsearchApi/ElasticsearchApiServiceTest.java` to assert `_shards.total`, `_shards.successful`, `_shards.skipped`, and `_shards.failed`.

### Task 5: Classify Uncovered Distributed Snapshots As Failed

Modify `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`.

Before adding replica fallback, make the one-shot path safe:

- Keep each distributed node request paired with its requested snapshot names.
- If a node request fails, add `SearchResult.error(requestedSnapshotNames.size())`.
- If a node request succeeds but returns fewer `successfulSnapshotIds` than requested, classify the missing requested snapshots as failed.

This phase ensures `rep=1` timeout or cache request failure returns:

```text
totalSnapshots = requested logical snapshot count
snapshotsWithReplicas = successful logical snapshot count
failedSnapshots = missing logical snapshot count
skippedSnapshots = 0
```

Add a test in `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java` where one requested snapshot has one candidate and the stub fails. Assert:

```text
totalSnapshots = 1
snapshotsWithReplicas = 0
failedSnapshots = 1
skippedSnapshots = 0
```

### Task 6: Add Replica Fallback

Modify `AstraDistributedQueryService` to preserve all candidates per logical snapshot.

Add:

```java
private record SnapshotSearchPlan(String snapshotName, List<SearchMetadata> candidates) {}
```

Build plans from `getMatchingSearchMetadata`. Candidate order should preserve current behavior:

1. The existing `pickSearchNodeToQuery(candidates)` result first.
2. Remaining candidates after that.

Use a loop that queries unresolved snapshots until no work remains or the deadline is reached:

```java
Set<String> fulfilledSnapshotIds = new HashSet<>();
Map<String, Integer> failedAttemptCountsBySnapshot = new HashMap<>();
Map<String, Integer> nextCandidateIndexBySnapshot = new HashMap<>();
List<SearchResult<LogMessage>> successfulResults = new ArrayList<>();
```

For each round:

- Build a node URL to snapshot name batch using the next candidate for every unresolved snapshot.
- Query batches with the existing gRPC stub/deadline/interceptor behavior.
- Add returned `successfulSnapshotIds` to `fulfilledSnapshotIds`.
- Increment `failedAttemptCountsBySnapshot` for each requested snapshot ID missing from that attempt's successful IDs.
- Continue only for snapshots that remain unresolved and still have untried candidates.

After all attempts:

```java
Set<String> failedSnapshotIds = new HashSet<>(searchPlansBySnapshot.keySet());
failedSnapshotIds.removeAll(fulfilledSnapshotIds);

int skippedSnapshots =
    failedAttemptCountsBySnapshot.entrySet().stream()
        .filter(entry -> fulfilledSnapshotIds.contains(entry.getKey()))
        .mapToInt(Map.Entry::getValue)
        .sum();
```

Add one accounting-only `SearchResult` to the successful results before aggregation:

```java
new SearchResult<>(
    List.of(),
    0,
    failedSnapshotIds.size(),
    0,
    failedSnapshotIds.size(),
    0,
    failedSnapshotIds.size(),
    skippedSnapshots,
    List.of(),
    null)
```

This result contributes final failed/skipped counts without adding hits or aggregations.

Add a test where one logical snapshot has two candidates: the first stub fails, the second returns `successful_snapshot_ids = [snapshot]`. Assert:

```text
totalSnapshots = 1
snapshotsWithReplicas = 1
failedSnapshots = 0
skippedSnapshots = 1
```

Add a second test where all candidates fail. Assert:

```text
totalSnapshots = 1
snapshotsWithReplicas = 0
failedSnapshots = 1
skippedSnapshots = 0
```

### Task 7: Emit Metrics

Modify `AstraDistributedQueryService`:

```java
public static final String ASTRA_QUERY_SKIPPED_SHARDS_TOTAL =
    "astra_query_skipped_shards_total";
public static final String ASTRA_QUERY_FAILED_SHARDS_TOTAL =
    "astra_query_failed_shards_total";

private final Counter skippedShardsTotal;
private final Counter failedShardsTotal;
```

Initialize both counters in the constructor with `meterRegistry.counter(ASTRA_QUERY_SKIPPED_SHARDS_TOTAL)` and `meterRegistry.counter(ASTRA_QUERY_FAILED_SHARDS_TOTAL)`.

After final aggregation in `doSearch`, increment:

```java
skippedShardsTotal.increment(aggregatedResult.skippedSnapshots);
failedShardsTotal.increment(aggregatedResult.failedSnapshots);
```

Add assertions to distributed query tests:

```text
rep=1 failed request increments astra_query_failed_shards_total by 1
replica-covered miss increments astra_query_skipped_shards_total by 1
```

## Compatibility, Deprecation, and Migration Plan

The protobuf change is additive. Existing serialized responses that do not contain the new fields will read the counts as zero and the successful snapshot IDs as empty.

OpenSearch-compatible JSON response shape changes by adding `_shards.successful` and `_shards.skipped`. This is compatible with OpenSearch and Elasticsearch response conventions. Consumers that only read `_shards.total` and `_shards.failed` should continue to work.

User-visible behavior changes intentionally:

- Queries with missing logical snapshot data will now show failed shards.
- OpenSearch Dashboards may show red shard-failure banners for partial results that previously looked clean.
- Queries fully covered by another replica will not show failed shards.

No metadata migration is required. No config migration is required.

Rollback is safe at the binary/API level because the protobuf fields are additive. Rolling back would restore the old behavior where some incomplete responses may appear clean in OpenSearch Dashboards.

## Test Plan

Add or update tests in:

- `astra/src/test/java/com/slack/astra/server/SearchResultTest.java`
- `astra/src/test/java/com/slack/astra/logstore/search/SearchResultAggregatorImplTest.java`
- `astra/src/test/java/com/slack/astra/logstore/search/AstraLocalQueryServiceTest.java`
- `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java`
- `astra/src/test/java/com/slack/astra/elasticsearchApi/ElasticsearchApiServiceTest.java`

Required coverage:

- Proto conversion preserves `failed_snapshots`, `skipped_snapshots`, and `successful_snapshot_ids`.
- Aggregation sums failed/skipped snapshot counts and merges successful snapshot IDs.
- Local chunk queries attach successful chunk IDs.
- OpenSearch responses include `_shards.successful`, `_shards.skipped`, and `_shards.failed`.
- `rep=1` timeout/failure returns `failed=1`, `skipped=0`.
- First replica timeout plus second replica success returns `failed=0`, `skipped=1`.
- All replicas fail returns `failed=1`, `skipped=0`.
- Metrics increment from final classification, not raw attempts.

Focused verification command:

```bash
mvn -pl astra -Dtest=SearchResultTest,SearchResultAggregatorImplTest,AstraLocalQueryServiceTest,AstraDistributedQueryServiceTest,ElasticsearchApiServiceTest test
```

Broader verification command:

```bash
mvn -pl astra test
```

## Documentation Plan

Update docs if this ADR is implemented:

- Add or update a query behavior doc under `docs/topics/` describing shard failure and skipped semantics.
- Add the new metrics to the metrics documentation under `docs/metrics/`.
- Mention that OpenSearch Dashboards shard-failure banners indicate incomplete logical chunk coverage.

No README update is required unless the query behavior docs are linked from README in a later cleanup.

## Rejected Alternatives

- Always report incomplete shard responses as `skipped`.
  - Rejected because it can make incomplete results look healthy in OpenSearch Dashboards, especially for `rep=1`.

- Always report every replica miss as `failed`.
  - Rejected because it creates noisy dashboards when another replica returned complete data for the same logical chunk.

- Keep only the existing `total_snapshots` and `snapshots_with_replicas` fields.
  - Rejected because OpenSearch Dashboards relies on `_shards.failed` for user-visible partial-result warnings.

- Add only metrics and leave API responses unchanged.
  - Rejected because operators need both Prometheus visibility and UI visibility in OpenSearch Dashboards.

- Add a config flag to choose skipped versus failed behavior.
  - Rejected for the initial implementation because the completeness semantics are objective. A missing logical chunk should be failed.

## Consequences

Benefits:

- Incomplete query results become visible in OpenSearch Dashboards.
- Replica-covered misses avoid noisy shard-failure banners.
- Operators get Prometheus counters independent of UI behavior.
- Astra's OpenSearch-compatible response shape moves closer to Elasticsearch/OpenSearch conventions.

Costs:

- `AstraDistributedQueryService` becomes more complex because it must track per-snapshot replica candidates and retry unresolved snapshots.
- The response path carries additional snapshot ID data.
- Some existing clean-looking queries may start showing shard-failure banners because they were already returning partial results.

Follow-up work:

- Consider adding `_shards.failures` details with shard IDs and timeout/error reasons.
- Consider honoring `allow_partial_search_results=false` once Astra can reliably classify partial results.
- Consider exposing a debug field with logical `total_snapshots` and concrete `_shards.total` if operators find the distinction confusing.
