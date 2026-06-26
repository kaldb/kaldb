package com.slack.astra.clusterManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentSerializer;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheNodeSearchabilityServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;
  private AsyncCuratorFramework curatorFramework;
  private SearchMetadataStore searchMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private AstraConfigs.ManagerConfig managerConfig;
  private CacheNodeMetadataStore cacheNodeMetadataStore;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("CacheNodeAssignmentServiceTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    AstraConfigs.ManagerConfig.CacheNodeSearchabilityServiceConfig
        cacheNodeSearchabilityServiceConfig =
            AstraConfigs.ManagerConfig.CacheNodeSearchabilityServiceConfig.newBuilder()
                .setSchedulePeriodMins(1)
                .build();

    managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setCacheNodeSearchabilityServiceConfig(cacheNodeSearchabilityServiceConfig)
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    searchMetadataStore =
        spy(new SearchMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true));
    cacheNodeMetadataStore =
        spy(new CacheNodeMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    cacheNodeAssignmentStore =
        spy(new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry));
    snapshotMetadataStore =
        spy(new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
  }

  @AfterEach
  public void tearDown() throws IOException {
    meterRegistry.close();
    testingServer.close();
    snapshotMetadataStore.close();
    curatorFramework.unwrap().close();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoCacheNodes() throws Exception {
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoUnsearchableCacheNodes() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", true));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    assertThat(cacheNodeMetadata.searchable).isTrue();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoAssignments() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    assertThat(cacheNodeMetadata.searchable).isFalse();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithNoSearchMetadata() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    assertThat(cacheNodeMetadata.searchable).isTrue();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithSearchMetadata() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    searchMetadataStore.createSync(
        new SearchMetadata(
            "test-name", "snapshot-id", "snapshot-id", "test-url:testhostname", false, false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    SearchMetadata searchMetadata = searchMetadataStore.getSync("test-name");
    assertThat(cacheNodeMetadata.searchable).isTrue();
    assertThat(searchMetadata.isSearchable()).isTrue();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithSearchMetadataInLoadingState() throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING));
    searchMetadataStore.createSync(
        new SearchMetadata("test-name", "snapshot-id", "snapshot-id", "test-url", false, false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    SearchMetadata searchMetadata = searchMetadataStore.getSync("test-name");
    assertThat(cacheNodeMetadata.searchable).isFalse();
    assertThat(searchMetadata.isSearchable()).isFalse();
  }

  @Test
  public void testCacheNodeSearchabilityServiceWithSearchMetadataInEvictingState()
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", false));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "snapshot-id",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICTING));
    searchMetadataStore.createSync(
        new SearchMetadata("test-name", "snapshot-id", "snapshot-id", "test-url", false, false));
    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync("test-id");
    SearchMetadata searchMetadata = searchMetadataStore.getSync("test-name");
    assertThat(cacheNodeMetadata.searchable).isFalse();
    assertThat(searchMetadata.isSearchable()).isFalse();
  }

  @Test
  public void testCacheNodeSearchabilityServiceEvictsLiveAssignmentsWhenSealedSnapshotSearchable()
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", true));
    String chunkId = "snapshot-id";

    SnapshotMetadata liveSnapshot =
        new SnapshotMetadata(
            "LIVE_snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            1,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId);
    snapshotMetadataStore.createSync(liveSnapshot);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            "snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            10L,
            SnapshotMetadata.SnapshotType.SEALED,
            SnapshotMetadata.IndexType.LUCENE,
            "snapshot-id",
            0,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId));

    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            liveSnapshot.snapshotId,
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));

    searchMetadataStore.createSync(
        new SearchMetadata(
            "sealed-name-test-id",
            "snapshot-id",
            "snapshot-id",
            "test-url:testhostname",
            true,
            false));
    Awaitility.await()
        .untilAsserted(
            () -> {
              assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(2);
              assertThat(searchMetadataStore.listSync().size()).isEqualTo(1);
              assertThat(cacheNodeAssignmentStore.listSync().size()).isEqualTo(1);
            });

    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    Awaitility.await()
        .untilAsserted(
            () -> {
              CacheNodeAssignment cacheNodeAssignment =
                  new CacheNodeAssignmentSerializer()
                      .fromJsonStr(
                          new String(
                              curatorFramework
                                  .getData()
                                  .forPath("/cacheAssignment/test-id/assignment-id")
                                  .toCompletableFuture()
                                  .get(1, java.util.concurrent.TimeUnit.SECONDS),
                              StandardCharsets.UTF_8));
              assertThat(cacheNodeAssignment.state)
                  .isEqualTo(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
            });
  }

  @Test
  public void
      testCacheNodeSearchabilityServiceDoesNotEvictLiveAssignmentsWhenSealedSnapshotUnsearchable()
          throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", true));
    String chunkId = "snapshot-id";

    SnapshotMetadata liveSnapshot =
        new SnapshotMetadata(
            "LIVE_snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            1,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId);
    snapshotMetadataStore.createSync(liveSnapshot);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            "snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            10L,
            SnapshotMetadata.SnapshotType.SEALED,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            0,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            liveSnapshot.snapshotId,
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    searchMetadataStore.createSync(
        new SearchMetadata(
            "sealed-name-test-id",
            "snapshot-id",
            "snapshot-id",
            "test-url:testhostname",
            false,
            false));

    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeAssignment cacheNodeAssignment =
        cacheNodeAssignmentStore.getSync("test-id", "assignment-id");
    assertThat(cacheNodeAssignment.state)
        .isEqualTo(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
  }

  @Test
  public void testCacheNodeSearchabilityServiceDoesNotEvictLiveAssignmentsForDifferentSealedChunk()
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", true));

    SnapshotMetadata liveSnapshot =
        new SnapshotMetadata(
            "LIVE_snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            1,
            SnapshotMetadata.DEFAULT_VERSION,
            "snapshot-id");
    snapshotMetadataStore.createSync(liveSnapshot);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            "other-sealed",
            1L,
            2L,
            10L,
            "partition",
            10L,
            SnapshotMetadata.SnapshotType.SEALED,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            0,
            SnapshotMetadata.DEFAULT_VERSION,
            "other-chunk"));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            liveSnapshot.snapshotId,
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    searchMetadataStore.createSync(
        new SearchMetadata(
            "sealed-name-test-id",
            "other-sealed",
            "other-sealed",
            "test-url:testhostname",
            true,
            false));

    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeAssignment cacheNodeAssignment =
        cacheNodeAssignmentStore.getSync("test-id", "assignment-id");
    assertThat(cacheNodeAssignment.state)
        .isEqualTo(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
  }

  @Test
  public void testCacheNodeSearchabilityServiceEvictsMultipleLiveAssignmentsForSameChunk()
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id-a", "testhostname-a", 1, "rep1", true));
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id-b", "testhostname-b", 1, "rep1", true));
    String chunkId = "snapshot-id";

    SnapshotMetadata liveSnapshotA =
        new SnapshotMetadata(
            "LIVE_snapshot-id-a",
            1L,
            2L,
            10L,
            "partition",
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            1,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId);
    SnapshotMetadata liveSnapshotB =
        new SnapshotMetadata(
            "LIVE_snapshot-id-b",
            1L,
            2L,
            10L,
            "partition",
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            1,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId);
    snapshotMetadataStore.createSync(liveSnapshotA);
    snapshotMetadataStore.createSync(liveSnapshotB);
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            "snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            10L,
            SnapshotMetadata.SnapshotType.SEALED,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            0,
            SnapshotMetadata.DEFAULT_VERSION,
            chunkId));

    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id-a",
            "test-id-a",
            liveSnapshotA.snapshotId,
            "replica-id-a",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id-b",
            "test-id-b",
            liveSnapshotB.snapshotId,
            "replica-id-b",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));

    searchMetadataStore.createSync(
        new SearchMetadata(
            "sealed-name-test-id",
            "snapshot-id",
            "snapshot-id",
            "test-url:testhostname",
            true,
            false));

    Awaitility.await()
        .untilAsserted(
            () ->
                assertThat(
                        cacheNodeAssignmentStore.listSync().stream()
                            .map(cacheNodeAssignment -> cacheNodeAssignment.assignmentId)
                            .toList())
                    .containsExactlyInAnyOrder("assignment-id-a", "assignment-id-b"));

    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    Awaitility.await()
        .untilAsserted(
            () -> {
              CacheNodeAssignment assignmentA =
                  new CacheNodeAssignmentSerializer()
                      .fromJsonStr(
                          new String(
                              curatorFramework
                                  .getData()
                                  .forPath("/cacheAssignment/test-id-a/assignment-id-a")
                                  .toCompletableFuture()
                                  .get(1, java.util.concurrent.TimeUnit.SECONDS),
                              StandardCharsets.UTF_8));
              CacheNodeAssignment assignmentB =
                  new CacheNodeAssignmentSerializer()
                      .fromJsonStr(
                          new String(
                              curatorFramework
                                  .getData()
                                  .forPath("/cacheAssignment/test-id-b/assignment-id-b")
                                  .toCompletableFuture()
                                  .get(1, java.util.concurrent.TimeUnit.SECONDS),
                              StandardCharsets.UTF_8));
              assertThat(assignmentA.state)
                  .isEqualTo(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
              assertThat(assignmentB.state)
                  .isEqualTo(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.EVICT);
            });
  }

  @Test
  public void testCacheNodeSearchabilityServiceIgnoresLiveAssignmentsWithoutSnapshotMetadata()
      throws Exception {
    cacheNodeMetadataStore.createSync(
        new CacheNodeMetadata("test-id", "testhostname", 1, "rep1", true));
    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            "snapshot-id",
            1L,
            2L,
            10L,
            "partition",
            10L,
            SnapshotMetadata.SnapshotType.SEALED,
            SnapshotMetadata.IndexType.LUCENE,
            "",
            0,
            SnapshotMetadata.DEFAULT_VERSION,
            "snapshot-id"));
    cacheNodeAssignmentStore.createSync(
        new CacheNodeAssignment(
            "assignment-id",
            "test-id",
            "missing-live-snapshot",
            "replica-id",
            "rep1",
            1,
            Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE));
    searchMetadataStore.createSync(
        new SearchMetadata(
            "sealed-name-test-id",
            "snapshot-id",
            "snapshot-id",
            "test-url:testhostname",
            true,
            false));

    CacheNodeSearchabilityService cacheNodeSearchabilityService =
        new CacheNodeSearchabilityService(
            meterRegistry,
            cacheNodeMetadataStore,
            managerConfig,
            cacheNodeAssignmentStore,
            searchMetadataStore,
            snapshotMetadataStore);
    cacheNodeSearchabilityService.runOneIteration();

    CacheNodeAssignment cacheNodeAssignment =
        cacheNodeAssignmentStore.getSync("test-id", "assignment-id");
    assertThat(cacheNodeAssignment.state)
        .isEqualTo(Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE);
  }
}
