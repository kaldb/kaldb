package com.slack.astra.metadata.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionMetadataStoreTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private PartitionMetadataStore store;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("Test")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(500)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();
    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    store = new PartitionMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (store != null) store.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void testPartitionMetadataStore() {
    PartitionMetadata partitionMetadata = new PartitionMetadata("partition-a", 100);

    assertThat(partitionMetadata.name).isEqualTo("partition-a");
    assertThat(partitionMetadata.getPartitionID()).isEqualTo("partition-a");
    assertThat(partitionMetadata.getMaxCapacity()).isEqualTo(100);

    store.createSync(partitionMetadata);

    await().until(() -> store.listSync().size() == 1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(store)).containsExactly(partitionMetadata);
    assertThat(store.getSync("partition-a")).isEqualTo(partitionMetadata);

    store.deleteSync("partition-a");

    await().until(() -> store.listSync().isEmpty());
    assertThat(AstraMetadataTestUtils.listSyncUncached(store)).isEmpty();
  }
}
