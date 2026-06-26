package com.slack.astra.clusterManager;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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
        new SearchMetadata("test-name", "snapshot-id", "test-url:testhostname", false));
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
        new SearchMetadata("test-name", "snapshot-id", "test-url", false));
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
        new SearchMetadata("test-name", "snapshot-id", "test-url", false));
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
            SnapshotMetadata.DEFAULT_VERSION);
    snapshotMetadataStore.createSync(liveSnapshot);

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
        new SearchMetadata("sealed-name-test-id", "snapshot-id", "test-url:testhostname", true));

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
}
