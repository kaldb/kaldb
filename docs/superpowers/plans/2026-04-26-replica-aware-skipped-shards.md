# Replica-Aware Skipped Shards Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface incomplete Astra query results as failed shards, and surface replica-covered shard misses as skipped shards.

**Architecture:** Preserve Astra's existing two-level aggregation model: local query services search chunks, and `AstraDistributedQueryService` aggregates node responses. Add explicit snapshot-level shard accounting to the internal `SearchResult`, populate successful snapshot IDs locally, classify distributed missing chunks after all replica attempts, and map those counts into OpenSearch-compatible `_shards` fields.

**Tech Stack:** Java, Maven, protobuf, gRPC, Micrometer counters, OpenSearch-compatible JSON response classes.

---

## Behavioral Contract

Use these definitions throughout the implementation:

- `failed` means no replica returned data for a logical snapshot/chunk required by the query. This affects result completeness and should produce an OpenSearch Dashboards shard-failure banner.
- `skipped` means a concrete replica attempt did not return, but another replica returned data for the same logical snapshot/chunk. This does not affect result completeness and should keep the UI clean.
- `successful` means a logical snapshot/chunk returned data from at least one replica.
- `total_snapshots` remains the number of logical snapshots/chunks required by the query.
- OpenSearch `_shards.total` should be `successful + skipped + failed`, where `skipped` represents covered replica misses. This keeps `_shards.successful + _shards.skipped + _shards.failed == _shards.total`.

## File Structure

- Modify `astra/src/main/proto/astra_search.proto`
  - Add snapshot-level shard accounting fields to `SearchResult`.
- Modify `astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java`
  - Carry `failedSnapshots`, `skippedSnapshots`, and `successfulSnapshotIds`.
- Modify `astra/src/main/java/com/slack/astra/logstore/search/SearchResultUtils.java`
  - Convert new fields to and from protobuf.
- Modify `astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java`
  - Sum new counts and merge successful snapshot IDs.
- Modify `astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java`
  - Attach chunk IDs to successful local chunk query results and mark local chunk failures as failed snapshots.
- Modify `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`
  - Track replica candidates, retry unresolved snapshots, classify final skipped and failed shard counts, and emit metrics.
- Modify `astra/src/main/java/com/slack/astra/elasticsearchApi/searchResponse/EsSearchResponse.java`
  - Include `successful` and `skipped` in `_shards`.
- Modify `astra/src/main/java/com/slack/astra/elasticsearchApi/ElasticsearchApiService.java`
  - Map snapshot-level counts into OpenSearch-compatible `_shards`.
- Test `astra/src/test/java/com/slack/astra/server/SearchResultTest.java`
- Test `astra/src/test/java/com/slack/astra/logstore/search/SearchResultAggregatorImplTest.java`
- Test `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java`
- Test `astra/src/test/java/com/slack/astra/elasticsearchApi/ElasticsearchApiServiceTest.java`

## Task 1: Add Snapshot-Level Fields To SearchResult

**Files:**
- Modify: `astra/src/main/proto/astra_search.proto`
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java`
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/SearchResultUtils.java`
- Test: `astra/src/test/java/com/slack/astra/server/SearchResultTest.java`

- [ ] **Step 1: Write the failing proto conversion test**

In `SearchResultTest.testSearchResultObjectConversions`, update the `SearchResult` construction to include failed/skipped counts and successful snapshot IDs:

```java
SearchResult<LogMessage> searchResult =
    new SearchResult<>(
        logMessages,
        1,
        1,
        5,
        7,
        6,
        1,
        2,
        List.of("snapshot-a", "snapshot-b"),
        internalAggregation);
```

Add these assertions after the existing snapshot assertions:

```java
assertThat(protoSearchResult.getFailedSnapshots()).isEqualTo(1);
assertThat(protoSearchResult.getSkippedSnapshots()).isEqualTo(2);
assertThat(protoSearchResult.getSuccessfulSnapshotIdsList())
    .containsExactly("snapshot-a", "snapshot-b");
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
mvn -pl astra -Dtest=SearchResultTest#testSearchResultObjectConversions test
```

Expected: compilation fails because `failed_snapshots`, `skipped_snapshots`, and `successful_snapshot_ids` do not exist yet.

- [ ] **Step 3: Add proto fields**

In `astra_search.proto`, update `message SearchResult`:

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

- [ ] **Step 4: Update `SearchResult` fields and constructor**

In `SearchResult.java`, add:

```java
public final int failedSnapshots;
public final int skippedSnapshots;
public final List<String> successfulSnapshotIds;
```

Update the main constructor signature:

```java
public SearchResult(
    List<T> hits,
    long tookMicros,
    int failedNodes,
    int totalNodes,
    int totalSnapshots,
    int snapshotsWithReplicas,
    int failedSnapshots,
    int skippedSnapshots,
    List<String> successfulSnapshotIds,
    InternalAggregation internalAggregation) {
  this.hits = hits;
  this.tookMicros = tookMicros;
  this.failedNodes = failedNodes;
  this.totalNodes = totalNodes;
  this.totalSnapshots = totalSnapshots;
  this.snapshotsWithReplicas = snapshotsWithReplicas;
  this.failedSnapshots = failedSnapshots;
  this.skippedSnapshots = skippedSnapshots;
  this.successfulSnapshotIds = successfulSnapshotIds;
  this.internalAggregation = internalAggregation;
}
```

Update the no-arg constructor and static constants so the new counts are zero and successful IDs are `List.of()`.

Add a factory for known failed snapshot counts:

```java
public static SearchResult<LogMessage> error(int failedSnapshots) {
  return new SearchResult<>(
      Collections.emptyList(), 0, 1, 1, failedSnapshots, 0, failedSnapshots, 0, List.of(), null);
}
```

Keep `error()` for existing callers, but route new shard-aware call sites to `error(int failedSnapshots)`.

- [ ] **Step 5: Update equality, hash, and string rendering**

In `SearchResult.toString`, include:

```java
+ ", failedSnapshots="
+ failedSnapshots
+ ", skippedSnapshots="
+ skippedSnapshots
+ ", successfulSnapshotIds="
+ successfulSnapshotIds
```

In `equals`, compare:

```java
if (failedSnapshots != that.failedSnapshots) return false;
if (skippedSnapshots != that.skippedSnapshots) return false;
if (!successfulSnapshotIds.equals(that.successfulSnapshotIds)) return false;
```

In `hashCode`, include:

```java
result = 31 * result + failedSnapshots;
result = 31 * result + skippedSnapshots;
result = 31 * result + successfulSnapshotIds.hashCode();
```

- [ ] **Step 6: Update protobuf conversion**

In `SearchResultUtils.fromSearchResultProto`, pass the new proto fields into the constructor:

```java
protoSearchResult.getFailedSnapshots(),
protoSearchResult.getSkippedSnapshots(),
protoSearchResult.getSuccessfulSnapshotIdsList(),
```

In `SearchResultUtils.toSearchResultProto`, add span tags:

```java
span.tag("failedSnapshots", String.valueOf(searchResult.failedSnapshots));
span.tag("skippedSnapshots", String.valueOf(searchResult.skippedSnapshots));
span.tag("successfulSnapshotIds", String.valueOf(searchResult.successfulSnapshotIds.size()));
```

Set proto fields:

```java
searchResultBuilder.setFailedSnapshots(searchResult.failedSnapshots);
searchResultBuilder.setSkippedSnapshots(searchResult.skippedSnapshots);
searchResultBuilder.addAllSuccessfulSnapshotIds(searchResult.successfulSnapshotIds);
```

- [ ] **Step 7: Run the conversion test**

Run:

```bash
mvn -pl astra -Dtest=SearchResultTest#testSearchResultObjectConversions test
```

Expected: `SearchResultTest#testSearchResultObjectConversions` passes.

- [ ] **Step 8: Commit**

```bash
git add astra/src/main/proto/astra_search.proto astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java astra/src/main/java/com/slack/astra/logstore/search/SearchResultUtils.java astra/src/test/java/com/slack/astra/server/SearchResultTest.java
git commit -m "Add snapshot shard accounting to search results"
```

## Task 2: Aggregate Snapshot Shard Accounting

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java`
- Test: `astra/src/test/java/com/slack/astra/logstore/search/SearchResultAggregatorImplTest.java`

- [ ] **Step 1: Write the failing aggregation test**

Add a test to `SearchResultAggregatorImplTest`:

```java
@Test
public void testAggregatesSnapshotShardAccounting() throws IOException {
  long tookMs = 10;
  Instant startTime = Instant.now();
  long searchStartMs = startTime.toEpochMilli();
  long searchEndMs = startTime.plus(1, ChronoUnit.HOURS).toEpochMilli();

  SearchResult<LogMessage> searchResult1 =
      new SearchResult<>(
          Collections.emptyList(),
          tookMs,
          0,
          1,
          2,
          1,
          1,
          0,
          List.of("snapshot-a"),
          null);
  SearchResult<LogMessage> searchResult2 =
      new SearchResult<>(
          Collections.emptyList(),
          tookMs + 1,
          0,
          1,
          2,
          1,
          0,
          1,
          List.of("snapshot-b"),
          null);

  SearchQuery searchQuery =
      new SearchQuery(
          MessageUtil.TEST_DATASET_NAME,
          searchStartMs,
          searchEndMs,
          0,
          Collections.emptyList(),
          QueryBuilderUtil.generateQueryBuilder("Message1", searchStartMs, searchEndMs),
          null,
          createDateHistogramAggregatorFactoriesBuilder(
              "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, "10m", 1));

  SearchResult<LogMessage> aggregated =
      new SearchResultAggregatorImpl<LogMessage>(searchQuery)
          .aggregate(List.of(searchResult1, searchResult2), true);

  assertThat(aggregated.totalSnapshots).isEqualTo(4);
  assertThat(aggregated.snapshotsWithReplicas).isEqualTo(2);
  assertThat(aggregated.failedSnapshots).isEqualTo(1);
  assertThat(aggregated.skippedSnapshots).isEqualTo(1);
  assertThat(aggregated.successfulSnapshotIds).containsExactly("snapshot-a", "snapshot-b");
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
mvn -pl astra -Dtest=SearchResultAggregatorImplTest#testAggregatesSnapshotShardAccounting test
```

Expected: compilation fails or assertions fail because the aggregator does not merge the new fields.

- [ ] **Step 3: Update aggregator implementation**

In `SearchResultAggregatorImpl.aggregate`, add local accumulators:

```java
int failedSnapshots = 0;
int skippedSnapshots = 0;
List<String> successfulSnapshotIds = new ArrayList<>();
```

Inside the loop:

```java
failedSnapshots += searchResult.failedSnapshots;
skippedSnapshots += searchResult.skippedSnapshots;
successfulSnapshotIds.addAll(searchResult.successfulSnapshotIds);
```

Pass the fields into the returned `SearchResult`:

```java
return new SearchResult<>(
    resultHits,
    tookMicros,
    failedNodes,
    totalNodes,
    totalSnapshots,
    snapshpotReplicas,
    failedSnapshots,
    skippedSnapshots,
    successfulSnapshotIds,
    internalAggregation);
```

- [ ] **Step 4: Update existing constructor call sites in tests**

Update each existing `SearchResult` constructor call in `SearchResultAggregatorImplTest` by inserting:

```java
0,
0,
List.of(),
```

between `snapshotsWithReplicas` and `internalAggregation`, unless the test is explicitly checking failed/skipped snapshot accounting.

- [ ] **Step 5: Run aggregator tests**

Run:

```bash
mvn -pl astra -Dtest=SearchResultAggregatorImplTest test
```

Expected: all `SearchResultAggregatorImplTest` tests pass.

- [ ] **Step 6: Commit**

```bash
git add astra/src/main/java/com/slack/astra/logstore/search/SearchResultAggregatorImpl.java astra/src/test/java/com/slack/astra/logstore/search/SearchResultAggregatorImplTest.java
git commit -m "Aggregate snapshot shard accounting"
```

## Task 3: Attach Local Chunk Success IDs

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java`
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcherImpl.java`
- Test: `astra/src/test/java/com/slack/astra/logstore/search/AstraLocalQueryServiceTest.java`

- [ ] **Step 1: Write the failing local query test**

In `AstraLocalQueryServiceTest`, add a test that queries a known chunk ID and asserts the response carries that ID in `successful_snapshot_ids`. Use the existing local query setup in the file and assert:

```java
AstraSearch.SearchResult result = localQueryService.doSearch(searchRequest);

assertThat(result.getTotalSnapshots()).isEqualTo(1);
assertThat(result.getSnapshotsWithReplicas()).isEqualTo(1);
assertThat(result.getFailedSnapshots()).isEqualTo(0);
assertThat(result.getSkippedSnapshots()).isEqualTo(0);
assertThat(result.getSuccessfulSnapshotIdsList()).containsExactly(chunkId);
```

- [ ] **Step 2: Run the failing local query test**

Run:

```bash
mvn -pl astra -Dtest=AstraLocalQueryServiceTest test
```

Expected: the new assertion fails because successful snapshot IDs are empty.

- [ ] **Step 3: Add a result wrapper in `ChunkManagerBase`**

In `ChunkManagerBase`, add a private record near the class fields:

```java
private record ChunkSearchTask<T>(
    String chunkId, StructuredTaskScope.Subtask<SearchResult<T>> subtask) {}
```

Change `chunkSubtasks` to keep chunk IDs:

```java
List<ChunkSearchTask<T>> chunkSubtasks =
    chunksMatchingQuery.stream()
        .map(
            (chunk) ->
                new ChunkSearchTask<>(
                    chunk.id(),
                    scope.fork(
                        currentTraceContext.wrap(
                            () -> {
                              ScopedSpan span =
                                  Tracing.currentTracer()
                                      .startScopedSpan("ChunkManagerBase.chunkQuery");
                              span.tag("chunkId", chunk.id());
                              concurrentQueries.acquire();
                              try {
                                return chunk.query(query);
                              } finally {
                                concurrentQueries.release();
                                span.finish();
                              }
                            }))))
        .toList();
```

Update the result mapping to use `chunkSearchTask.subtask()` and `chunkSearchTask.chunkId()`.

- [ ] **Step 4: Mark successful chunk IDs and failed chunks**

When a chunk subtask succeeds:

```java
SearchResult<T> result = searchResultSubtask.get();
if (result.snapshotsWithReplicas > 0) {
  return new SearchResult<>(
      result.hits,
      result.tookMicros,
      result.failedNodes,
      result.totalNodes,
      result.totalSnapshots,
      result.snapshotsWithReplicas,
      result.failedSnapshots,
      result.skippedSnapshots,
      List.of(chunkSearchTask.chunkId()),
      result.internalAggregation);
}
return result;
```

When a chunk subtask fails or times out:

```java
return (SearchResult<T>) SearchResult.error(1);
```

- [ ] **Step 5: Update `LogIndexSearcherImpl` constructor call**

In `LogIndexSearcherImpl`, update the successful `SearchResult` return:

```java
return new SearchResult<>(
    results,
    elapsedTime.elapsed(TimeUnit.MICROSECONDS),
    0,
    0,
    1,
    1,
    0,
    0,
    List.of(),
    internalAggregation);
```

The chunk manager owns the chunk ID because `LogIndexSearcherImpl` should not need chunk identity.

- [ ] **Step 6: Run local query tests**

Run:

```bash
mvn -pl astra -Dtest=AstraLocalQueryServiceTest test
```

Expected: `AstraLocalQueryServiceTest` passes.

- [ ] **Step 7: Commit**

```bash
git add astra/src/main/java/com/slack/astra/chunkManager/ChunkManagerBase.java astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcherImpl.java astra/src/test/java/com/slack/astra/logstore/search/AstraLocalQueryServiceTest.java
git commit -m "Track successful local chunk query ids"
```

## Task 4: Return OpenSearch-Compatible Shard Counts

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/elasticsearchApi/searchResponse/EsSearchResponse.java`
- Modify: `astra/src/main/java/com/slack/astra/elasticsearchApi/ElasticsearchApiService.java`
- Test: `astra/src/test/java/com/slack/astra/elasticsearchApi/ElasticsearchApiServiceTest.java`

- [ ] **Step 1: Write the failing API response test**

Add a test that uses an `AstraQueryServiceBase` fake returning a fixed proto result:

```java
@Test
public void testSearchResponseIncludesSuccessfulSkippedAndFailedShards() throws Exception {
  AstraQueryServiceBase fakeSearcher =
      new AstraQueryServiceBase() {
        @Override
        public AstraSearch.SearchResult doSearch(AstraSearch.SearchRequest request) {
          return AstraSearch.SearchResult.newBuilder()
              .setTookMicros(1000)
              .setTotalSnapshots(4)
              .setSnapshotsWithReplicas(2)
              .setFailedSnapshots(1)
              .setSkippedSnapshots(1)
              .build();
        }

        @Override
        public AstraSearch.SchemaResult getSchema(AstraSearch.SchemaRequest request) {
          return AstraSearch.SchemaResult.newBuilder().build();
        }
      };

  ElasticsearchApiService service =
      new ElasticsearchApiService(
          fakeSearcher,
          DEFAULT_CLUSTER_NAME,
          DEFAULT_HOST,
          DEFAULT_PORT,
          mock(DatasetMetadataStore.class));

  HttpResponse response =
      service.search(TEST_DATASET_NAME, "{\"size\":0,\"query\":{\"match_all\":{}}}");
  JsonNode body = OBJECT_MAPPER.readTree(response.aggregate().join().contentUtf8());

  assertThat(body.get("_shards").get("total").asInt()).isEqualTo(4);
  assertThat(body.get("_shards").get("successful").asInt()).isEqualTo(2);
  assertThat(body.get("_shards").get("skipped").asInt()).isEqualTo(1);
  assertThat(body.get("_shards").get("failed").asInt()).isEqualTo(1);
}
```

- [ ] **Step 2: Run the failing API response test**

Run:

```bash
mvn -pl astra -Dtest=ElasticsearchApiServiceTest#testSearchResponseIncludesSuccessfulSkippedAndFailedShards test
```

Expected: the test fails because `_shards.successful` and `_shards.skipped` are missing.

- [ ] **Step 3: Update `EsSearchResponse.Builder`**

Replace `shardsMetadata(int total, int failed)` with:

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

- [ ] **Step 4: Update `ElasticsearchApiService` mapping**

In `doSearch`, compute counts from snapshot fields:

```java
int successfulShards = searchResult.getSnapshotsWithReplicas();
int skippedShards = searchResult.getSkippedSnapshots();
int failedShards = searchResult.getFailedSnapshots();
```

Replace both `.shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())` calls with:

```java
.shardsMetadata(successfulShards, skippedShards, failedShards)
```

Fix the existing trace typo while editing this block:

```java
span.tag("resultTotalSnapshots", String.valueOf(searchResult.getTotalSnapshots()));
```

- [ ] **Step 5: Update existing `_shards` assertions**

Update existing tests such as `testSingleSearchReturnsAttributeAggregation` to assert:

```java
assertThat(jsonNode.get("_shards").get("total").asInt()).isEqualTo(1);
assertThat(jsonNode.get("_shards").get("successful").asInt()).isEqualTo(1);
assertThat(jsonNode.get("_shards").get("skipped").asInt()).isEqualTo(0);
assertThat(jsonNode.get("_shards").get("failed").asInt()).isEqualTo(0);
```

- [ ] **Step 6: Run API tests**

Run:

```bash
mvn -pl astra -Dtest=ElasticsearchApiServiceTest test
```

Expected: `ElasticsearchApiServiceTest` passes.

- [ ] **Step 7: Commit**

```bash
git add astra/src/main/java/com/slack/astra/elasticsearchApi/searchResponse/EsSearchResponse.java astra/src/main/java/com/slack/astra/elasticsearchApi/ElasticsearchApiService.java astra/src/test/java/com/slack/astra/elasticsearchApi/ElasticsearchApiServiceTest.java
git commit -m "Expose OpenSearch shard skipped and successful counts"
```

## Task 5: Classify Missing Distributed Shards As Failed

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`
- Test: `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java`

- [ ] **Step 1: Write the failing distributed failure test**

In `AstraDistributedQueryServiceTest`, add a test with one snapshot, one search metadata entry, and a future stub that throws:

```java
@Test
public void testDistributedSearchMarksUncoveredSnapshotAsFailedShard() {
  Instant endTime = Instant.now();
  Instant startTime = endTime.minus(1, ChronoUnit.HOURS);
  String dataset = "foo";
  String snapshot = "snapshot1";

  SearchMetadataStore searchMetadataStoreMock = mock(SearchMetadataStore.class);
  when(searchMetadataStoreMock.listSync())
      .thenReturn(List.of(new SearchMetadata("foo", snapshot, "http://127.0.0.1")));
  SnapshotMetadataStore snapshotMetadataStoreMock = mock(SnapshotMetadataStore.class);
  when(snapshotMetadataStoreMock.listSync())
      .thenReturn(
          List.of(
              new SnapshotMetadata(
                  snapshot,
                  startTime.toEpochMilli(),
                  endTime.toEpochMilli(),
                  10,
                  "1",
                  0)));
  DatasetMetadataStore datasetMetadataStoreMock = mock(DatasetMetadataStore.class);
  when(datasetMetadataStoreMock.listSync())
      .thenReturn(
          List.of(
              new DatasetMetadata(
                  dataset,
                  dataset,
                  10,
                  List.of(
                      new DatasetPartitionMetadata(
                          startTime.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                          Long.MAX_VALUE,
                          List.of("1"))),
                  dataset)));

  AstraDistributedQueryService service =
      new AstraDistributedQueryService(
          searchMetadataStoreMock,
          snapshotMetadataStoreMock,
          datasetMetadataStoreMock,
          metricsRegistry,
          Duration.ofSeconds(2),
          Duration.ofSeconds(2));

  AstraServiceGrpc.AstraServiceFutureStub futureStub = mock(AstraServiceGrpc.AstraServiceFutureStub.class);
  service.stubs.put("http://127.0.0.1", futureStub);
  when(futureStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(futureStub);
  when(futureStub.withInterceptors(any())).thenReturn(futureStub);
  when(futureStub.search(any(AstraSearch.SearchRequest.class)))
      .thenReturn(Futures.immediateFailedFuture(new RuntimeException("cache timeout")));

  AstraSearch.SearchResult result =
      service.doSearch(
          AstraSearch.SearchRequest.newBuilder()
              .setDataset(dataset)
              .setStartTimeEpochMs(startTime.toEpochMilli())
              .setEndTimeEpochMs(endTime.toEpochMilli())
              .setHowMany(1)
              .setQuery("{\"match_all\":{}}")
              .build());

  assertThat(result.getTotalSnapshots()).isEqualTo(1);
  assertThat(result.getSnapshotsWithReplicas()).isEqualTo(0);
  assertThat(result.getFailedSnapshots()).isEqualTo(1);
  assertThat(result.getSkippedSnapshots()).isEqualTo(0);

  service.close();
}
```

- [ ] **Step 2: Run the failing distributed failure test**

Run:

```bash
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksUncoveredSnapshotAsFailedShard test
```

Expected: the test fails because failed snapshot count remains zero.

- [ ] **Step 3: Track requested snapshots per node request**

In `AstraDistributedQueryService`, add a private record:

```java
private record SearchBatch(
    String nodeUrl,
    List<String> snapshotNames,
    StructuredTaskScope.Subtask<SearchResult<LogMessage>> subtask) {}
```

Build `SearchBatch` values instead of bare subtasks in `distributedSearch`.

- [ ] **Step 4: Convert failed node requests into failed snapshot counts**

When processing completed subtasks, replace generic `SearchResult.error()` for failed node requests with:

```java
response.add(SearchResult.error(searchBatch.snapshotNames().size()));
```

For successful node requests, normalize missing IDs:

```java
SearchResult<LogMessage> result = searchBatch.subtask().get();
int missingSnapshotCount =
    searchBatch.snapshotNames().size() - result.successfulSnapshotIds.size();
if (missingSnapshotCount > 0) {
  response.add(
      new SearchResult<>(
          result.hits,
          result.tookMicros,
          result.failedNodes,
          result.totalNodes,
          result.totalSnapshots + missingSnapshotCount,
          result.snapshotsWithReplicas,
          result.failedSnapshots + missingSnapshotCount,
          result.skippedSnapshots,
          result.successfulSnapshotIds,
          result.internalAggregation));
} else {
  response.add(result);
}
```

- [ ] **Step 5: Run the distributed failure test**

Run:

```bash
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksUncoveredSnapshotAsFailedShard test
```

Expected: the test passes and reports one failed snapshot.

- [ ] **Step 6: Commit**

```bash
git add astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java
git commit -m "Mark uncovered distributed snapshots as failed shards"
```

## Task 6: Add Replica Fallback And Skipped Classification

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`
- Test: `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java`

- [ ] **Step 1: Write the failing replica-covered skipped test**

Add a test with one logical snapshot hosted by two search metadata entries. Configure the first stub to fail and the second stub to return a successful search result with the same snapshot ID:

```java
@Test
public void testDistributedSearchMarksReplicaCoveredFailureAsSkippedShard() {
  Instant endTime = Instant.now();
  Instant startTime = endTime.minus(1, ChronoUnit.HOURS);
  String dataset = "foo";
  String snapshot = "snapshot1";

  SearchMetadataStore searchMetadataStoreMock = mock(SearchMetadataStore.class);
  when(searchMetadataStoreMock.listSync())
      .thenReturn(
          List.of(
              new SearchMetadata("search-1", snapshot, "http://127.0.0.1"),
              new SearchMetadata("search-2", snapshot, "http://127.0.0.2")));
  SnapshotMetadataStore snapshotMetadataStoreMock = mock(SnapshotMetadataStore.class);
  when(snapshotMetadataStoreMock.listSync())
      .thenReturn(
          List.of(
              new SnapshotMetadata(
                  snapshot,
                  startTime.toEpochMilli(),
                  endTime.toEpochMilli(),
                  10,
                  "1",
                  0)));
  DatasetMetadataStore datasetMetadataStoreMock = mock(DatasetMetadataStore.class);
  when(datasetMetadataStoreMock.listSync())
      .thenReturn(
          List.of(
              new DatasetMetadata(
                  dataset,
                  dataset,
                  10,
                  List.of(
                      new DatasetPartitionMetadata(
                          startTime.minus(1, ChronoUnit.DAYS).toEpochMilli(),
                          Long.MAX_VALUE,
                          List.of("1"))),
                  dataset)));

  AstraDistributedQueryService service =
      new AstraDistributedQueryService(
          searchMetadataStoreMock,
          snapshotMetadataStoreMock,
          datasetMetadataStoreMock,
          metricsRegistry,
          Duration.ofSeconds(2),
          Duration.ofSeconds(2));

  AstraServiceGrpc.AstraServiceFutureStub failingStub = mock(AstraServiceGrpc.AstraServiceFutureStub.class);
  AstraServiceGrpc.AstraServiceFutureStub successfulStub = mock(AstraServiceGrpc.AstraServiceFutureStub.class);
  service.stubs.put("http://127.0.0.1", failingStub);
  service.stubs.put("http://127.0.0.2", successfulStub);

  when(failingStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(failingStub);
  when(failingStub.withInterceptors(any())).thenReturn(failingStub);
  when(failingStub.search(any(AstraSearch.SearchRequest.class)))
      .thenReturn(Futures.immediateFailedFuture(new RuntimeException("cache timeout")));

  when(successfulStub.withDeadlineAfter(anyLong(), any(TimeUnit.class))).thenReturn(successfulStub);
  when(successfulStub.withInterceptors(any())).thenReturn(successfulStub);
  when(successfulStub.search(any(AstraSearch.SearchRequest.class)))
      .thenReturn(
          Futures.immediateFuture(
              AstraSearch.SearchResult.newBuilder()
                  .setTotalSnapshots(1)
                  .setSnapshotsWithReplicas(1)
                  .addSuccessfulSnapshotIds(snapshot)
                  .build()));

  AstraSearch.SearchResult result =
      service.doSearch(
          AstraSearch.SearchRequest.newBuilder()
              .setDataset(dataset)
              .setStartTimeEpochMs(startTime.toEpochMilli())
              .setEndTimeEpochMs(endTime.toEpochMilli())
              .setHowMany(1)
              .setQuery("{\"match_all\":{}}")
              .build());

  assertThat(result.getTotalSnapshots()).isEqualTo(1);
  assertThat(result.getSnapshotsWithReplicas()).isEqualTo(1);
  assertThat(result.getFailedSnapshots()).isEqualTo(0);
  assertThat(result.getSkippedSnapshots()).isEqualTo(1);
  assertThat(result.getSuccessfulSnapshotIdsList()).containsExactly(snapshot);

  service.close();
}
```

- [ ] **Step 2: Run the failing replica-covered skipped test**

Run:

```bash
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksReplicaCoveredFailureAsSkippedShard test
```

Expected: the test fails because the coordinator does not retry another replica.

- [ ] **Step 3: Add replica planning records**

In `AstraDistributedQueryService`, add:

```java
private record SnapshotSearchPlan(String snapshotName, List<SearchMetadata> candidates) {}
```

Add a helper that preserves existing preferred selection order:

```java
private static List<SearchMetadata> orderedCandidates(List<SearchMetadata> candidates) {
  SearchMetadata first = pickSearchNodeToQuery(candidates);
  List<SearchMetadata> ordered = new ArrayList<>();
  ordered.add(first);
  candidates.stream().filter(candidate -> !candidate.equals(first)).forEach(ordered::add);
  return ordered;
}
```

- [ ] **Step 4: Build retryable search plans**

After `getMatchingSearchMetadata`, create:

```java
Map<String, SnapshotSearchPlan> searchPlansBySnapshot = new HashMap<>();
searchMetadataNodesMatchingQuery.forEach(
    (snapshotName, candidates) ->
        searchPlansBySnapshot.put(
            snapshotName, new SnapshotSearchPlan(snapshotName, orderedCandidates(candidates))));
```

- [ ] **Step 5: Add a batched query helper**

Extract the existing structured task fanout into a helper:

```java
private List<SearchResult<LogMessage>> querySnapshotBatches(
    AstraSearch.SearchRequest distribSearchReq,
    Map<String, List<String>> nodesAndSnapshotsToQuery,
    CurrentTraceContext currentTraceContext,
    Set<String> failedAttemptSnapshotIds) {
  try (var scope = new StructuredTaskScope<SearchResult<LogMessage>>()) {
    List<SearchBatch> searchBatches =
        nodesAndSnapshotsToQuery.entrySet().stream()
            .map(
                searchNode ->
                    new SearchBatch(
                        searchNode.getKey(),
                        searchNode.getValue(),
                        scope.fork(
                            currentTraceContext.wrap(
                                () -> {
                                  AstraServiceGrpc.AstraServiceFutureStub stub =
                                      getStub(searchNode.getKey());
                                  if (stub == null) {
                                    return null;
                                  }

                                  AstraSearch.SearchRequest localSearchReq =
                                      distribSearchReq.toBuilder()
                                          .addAllChunkIds(searchNode.getValue())
                                          .build();
                                  return SearchResultUtils.fromSearchResultProtoOrEmpty(
                                      stub.withDeadlineAfter(
                                              defaultQueryTimeout.toMillis(), TimeUnit.MILLISECONDS)
                                          .withInterceptors(
                                              GrpcTracing.newBuilder(Tracing.current())
                                                  .build()
                                                  .newClientInterceptor())
                                          .search(localSearchReq)
                                          .get());
                                }))))
            .toList();

    try {
      scope.joinUntil(Instant.now().plus(defaultQueryTimeout));
    } catch (TimeoutException timeoutException) {
      scope.shutdown();
      scope.join();
    }

    List<SearchResult<LogMessage>> response = new ArrayList<>(searchBatches.size());
    for (SearchBatch searchBatch : searchBatches) {
      try {
        if (searchBatch.subtask().state().equals(StructuredTaskScope.Subtask.State.SUCCESS)) {
          SearchResult<LogMessage> result = searchBatch.subtask().get();
          if (result == null) {
            failedAttemptSnapshotIds.addAll(searchBatch.snapshotNames());
            continue;
          }

          Set<String> successfulSnapshotIds = new HashSet<>(result.successfulSnapshotIds);
          searchBatch.snapshotNames().stream()
              .filter(snapshotName -> !successfulSnapshotIds.contains(snapshotName))
              .forEach(failedAttemptSnapshotIds::add);
          response.add(result);
        } else {
          failedAttemptSnapshotIds.addAll(searchBatch.snapshotNames());
          LOG.warn("Error fetching part of search result {}", searchBatch.subtask());
        }
      } catch (Exception e) {
        failedAttemptSnapshotIds.addAll(searchBatch.snapshotNames());
        LOG.error("Error fetching search result", e);
      }
    }
    return response;
  } catch (Exception e) {
    LOG.error("Search batch failed", e);
    nodesAndSnapshotsToQuery.values().forEach(failedAttemptSnapshotIds::addAll);
    return List.of();
  }
}
```

The helper must not permanently classify final `failedSnapshots`; it only records attempts that did not return.

- [ ] **Step 6: Retry unresolved snapshots on remaining replicas**

In `distributedSearch`, replace the one-shot query flow with:

```java
Set<String> fulfilledSnapshotIds = new HashSet<>();
Set<String> failedAttemptSnapshotIds = new HashSet<>();
List<SearchResult<LogMessage>> allSuccessfulResults = new ArrayList<>();
Map<String, Integer> nextCandidateIndexBySnapshot = new HashMap<>();

while (fulfilledSnapshotIds.size() < searchPlansBySnapshot.size()) {
  Map<String, List<String>> retryBatch = new HashMap<>();
  for (SnapshotSearchPlan plan : searchPlansBySnapshot.values()) {
    if (fulfilledSnapshotIds.contains(plan.snapshotName())) {
      continue;
    }
    int nextCandidateIndex = nextCandidateIndexBySnapshot.getOrDefault(plan.snapshotName(), 0);
    if (nextCandidateIndex >= plan.candidates().size()) {
      continue;
    }
    SearchMetadata candidate = plan.candidates().get(nextCandidateIndex);
    retryBatch.computeIfAbsent(candidate.url, ignored -> new ArrayList<>()).add(plan.snapshotName());
    nextCandidateIndexBySnapshot.put(plan.snapshotName(), nextCandidateIndex + 1);
  }

  if (retryBatch.isEmpty()) {
    break;
  }

  List<SearchResult<LogMessage>> attemptResults =
      querySnapshotBatches(distribSearchReq, retryBatch, currentTraceContext, failedAttemptSnapshotIds);
  for (SearchResult<LogMessage> result : attemptResults) {
    fulfilledSnapshotIds.addAll(result.successfulSnapshotIds);
    if (!result.successfulSnapshotIds.isEmpty()) {
      allSuccessfulResults.add(result);
    }
  }
}
```

- [ ] **Step 7: Apply final shard classification**

After attempts finish:

```java
Set<String> failedSnapshotIds = new HashSet<>(searchPlansBySnapshot.keySet());
failedSnapshotIds.removeAll(fulfilledSnapshotIds);

int skippedSnapshots = 0;
for (String failedAttemptSnapshotId : failedAttemptSnapshotIds) {
  if (fulfilledSnapshotIds.contains(failedAttemptSnapshotId)) {
    skippedSnapshots++;
  }
}

if (!failedSnapshotIds.isEmpty() || skippedSnapshots > 0) {
  allSuccessfulResults.add(
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
          null));
}

return allSuccessfulResults;
```

- [ ] **Step 8: Run replica fallback tests**

Run:

```bash
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksReplicaCoveredFailureAsSkippedShard test
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksUncoveredSnapshotAsFailedShard test
```

Expected: both tests pass.

- [ ] **Step 9: Commit**

```bash
git add astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java
git commit -m "Retry replicas and classify covered shard misses as skipped"
```

## Task 7: Emit Prometheus Counters

**Files:**
- Modify: `astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java`
- Test: `astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java`

- [ ] **Step 1: Write the failing metric assertions**

In the failed-shard test, assert:

```java
assertThat(metricsRegistry.counter("astra_query_failed_shards_total").count()).isEqualTo(1.0);
assertThat(metricsRegistry.counter("astra_query_skipped_shards_total").count()).isEqualTo(0.0);
```

In the replica-covered skipped test, assert:

```java
assertThat(metricsRegistry.counter("astra_query_failed_shards_total").count()).isEqualTo(0.0);
assertThat(metricsRegistry.counter("astra_query_skipped_shards_total").count()).isEqualTo(1.0);
```

- [ ] **Step 2: Run the failing metric tests**

Run:

```bash
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksUncoveredSnapshotAsFailedShard,AstraDistributedQueryServiceTest#testDistributedSearchMarksReplicaCoveredFailureAsSkippedShard test
```

Expected: metric assertions fail because the counters do not exist or remain zero.

- [ ] **Step 3: Add counter constants and fields**

In `AstraDistributedQueryService`, add:

```java
public static final String ASTRA_QUERY_SKIPPED_SHARDS_TOTAL =
    "astra_query_skipped_shards_total";
public static final String ASTRA_QUERY_FAILED_SHARDS_TOTAL =
    "astra_query_failed_shards_total";

private final Counter skippedShardsTotal;
private final Counter failedShardsTotal;
```

Initialize them in the constructor:

```java
this.skippedShardsTotal = meterRegistry.counter(ASTRA_QUERY_SKIPPED_SHARDS_TOTAL);
this.failedShardsTotal = meterRegistry.counter(ASTRA_QUERY_FAILED_SHARDS_TOTAL);
```

- [ ] **Step 4: Increment counters from final classification**

After `aggregatedResult` is available in `doSearch`, add:

```java
if (aggregatedResult.skippedSnapshots > 0) {
  skippedShardsTotal.increment(aggregatedResult.skippedSnapshots);
}
if (aggregatedResult.failedSnapshots > 0) {
  failedShardsTotal.increment(aggregatedResult.failedSnapshots);
}
```

- [ ] **Step 5: Run metric tests**

Run:

```bash
mvn -pl astra -Dtest=AstraDistributedQueryServiceTest#testDistributedSearchMarksUncoveredSnapshotAsFailedShard,AstraDistributedQueryServiceTest#testDistributedSearchMarksReplicaCoveredFailureAsSkippedShard test
```

Expected: metric assertions pass.

- [ ] **Step 6: Commit**

```bash
git add astra/src/main/java/com/slack/astra/logstore/search/AstraDistributedQueryService.java astra/src/test/java/com/slack/astra/logstore/search/AstraDistributedQueryServiceTest.java
git commit -m "Emit query skipped and failed shard counters"
```

## Task 8: Full Verification

**Files:**
- Verify changed files only.

- [ ] **Step 1: Run focused tests**

Run:

```bash
mvn -pl astra -Dtest=SearchResultTest,SearchResultAggregatorImplTest,AstraLocalQueryServiceTest,AstraDistributedQueryServiceTest,ElasticsearchApiServiceTest test
```

Expected: all focused tests pass.

- [ ] **Step 2: Run module tests**

Run:

```bash
mvn -pl astra test
```

Expected: the `astra` module test suite passes.

- [ ] **Step 3: Inspect final API response shape**

Use any passing `ElasticsearchApiServiceTest` response body and verify `_shards` has exactly this shape:

```json
{
  "total": 1,
  "successful": 1,
  "skipped": 0,
  "failed": 0
}
```

For a missing logical snapshot, verify:

```json
{
  "total": 1,
  "successful": 0,
  "skipped": 0,
  "failed": 1
}
```

For a replica-covered miss, verify:

```json
{
  "total": 2,
  "successful": 1,
  "skipped": 1,
  "failed": 0
}
```

- [ ] **Step 4: Review operational semantics before merge**

Confirm these points in the PR description:

- `rep=1` timeout or failed cache shard returns `_shards.failed > 0`.
- Replica fallback success returns `_shards.failed == 0` and `_shards.skipped > 0`.
- `astra_query_failed_shards_total` increments only for missing logical chunks.
- `astra_query_skipped_shards_total` increments only for covered replica misses.
- The OpenSearch response now includes `successful`, `skipped`, and `failed`.

- [ ] **Step 5: Commit final verification note if docs are updated**

If the implementation adds or updates docs, commit those docs with:

```bash
git add docs
git commit -m "Document shard failure and skipped semantics"
```

## Review Notes

The most important review decision is `_shards.total` for replica-covered misses. This plan uses concrete shard-attempt accounting for the OpenSearch response so the invariant remains:

```text
_shards.total == _shards.successful + _shards.skipped + _shards.failed
```

That means one logical chunk with a failed first replica and successful second replica returns:

```json
"_shards": {
  "total": 2,
  "successful": 1,
  "skipped": 1,
  "failed": 0
}
```

The internal `total_snapshots` field remains logical chunk count, so the same query has `total_snapshots == 1` and `snapshots_with_replicas == 1`.
