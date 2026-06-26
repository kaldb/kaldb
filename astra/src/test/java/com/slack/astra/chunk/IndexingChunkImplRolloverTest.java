package com.slack.astra.chunk;

import static com.slack.astra.chunk.ReadWriteChunk.LIVE_SNAPSHOT_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import brave.Tracing;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LuceneIndexStoreImpl;
import com.slack.astra.logstore.schema.SchemaAwareLogDocumentBuilderImpl;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.SpanUtil;
import com.slack.service.murron.trace.Trace;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexingChunkImplRolloverTest {
  private static final String TEST_KAFKA_PARTITION_ID = "10";
  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = 34567;
  private static final String CHUNK_DATA_PREFIX = "testDataSet";
  private static final Duration COMMIT_INTERVAL = Duration.ofSeconds(5 * 60);
  private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(5 * 60);

  @TempDir private Path tmpPath;

  private MeterRegistry registry;
  private ReadWriteChunk<LogMessage> chunk;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private SnapshotMetadataStore snapshotMetadataStore;
  private SearchMetadataStore searchMetadataStore;

  @BeforeEach
  void setUp() throws Exception {
    Tracing.newBuilder().build();
    testingServer = new TestingServer();
    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("shouldHandleChunkLivecycle")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    registry = new SimpleMeterRegistry();
    curatorFramework = CuratorBuilder.build(registry, metadataStoreConfig.getZookeeperConfig());

    snapshotMetadataStore =
        new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, registry);
    searchMetadataStore =
        new SearchMetadataStore(curatorFramework, metadataStoreConfig, registry, true);

    LuceneIndexStoreImpl logStore =
        LuceneIndexStoreImpl.makeLogStore(
            tmpPath.toFile(),
            COMMIT_INTERVAL,
            REFRESH_INTERVAL,
            true,
            SchemaAwareLogDocumentBuilderImpl.FieldConflictPolicy.CONVERT_VALUE_AND_DUPLICATE_FIELD,
            registry);
    chunk =
        new IndexingChunkImpl<>(
            logStore,
            CHUNK_DATA_PREFIX,
            registry,
            searchMetadataStore,
            snapshotMetadataStore,
            new SearchContext(TEST_HOST, TEST_PORT),
            TEST_KAFKA_PARTITION_ID);
    chunk.postCreate();

    assertThat(AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore)).hasSize(1);
    assertThat(AstraMetadataTestUtils.listSyncUncached(searchMetadataStore)).hasSize(1);
  }

  @AfterEach
  void tearDown() throws Exception {
    chunk.close();
    searchMetadataStore.close();
    snapshotMetadataStore.close();
    curatorFramework.unwrap().close();
    testingServer.close();
    registry.close();
  }

  @Test
  void testPostSnapshotCreatesNewSealedSnapshotAndKeepsLiveSnapshot() throws Exception {
    List<Trace.Span> messages = SpanUtil.makeSpansWithTimeDifference(1, 10, 1, Instant.now());
    int offset = 1;
    for (Trace.Span message : messages) {
      chunk.addMessage(message, TEST_KAFKA_PARTITION_ID, offset++);
    }

    chunk.info().setSizeInBytesOnDisk(1);

    chunk.preSnapshot();
    chunk.postSnapshot();

    List<SnapshotMetadata> afterSnapshots =
        AstraMetadataTestUtils.listSyncUncached(snapshotMetadataStore);
    assertThat(afterSnapshots).hasSize(2);

    SnapshotMetadata liveSnapshot =
        afterSnapshots.stream().filter(SnapshotMetadata::isLive).findFirst().orElseThrow();
    SnapshotMetadata sealedSnapshot =
        afterSnapshots.stream().filter(snapshot -> !snapshot.isLive()).findFirst().orElseThrow();

    assertThat(liveSnapshot.name).startsWith(LIVE_SNAPSHOT_PREFIX);
    assertThat(liveSnapshot.snapshotType).isEqualTo(SnapshotMetadata.SnapshotType.LIVE);
    assertThat(liveSnapshot.snapshotGeneration).isZero();
    assertThat(liveSnapshot.maxOffset).isEqualTo(offset - 1);

    assertThat(sealedSnapshot.name).isEqualTo(chunk.info().chunkId);
    assertThat(sealedSnapshot.snapshotType).isEqualTo(SnapshotMetadata.SnapshotType.SEALED);
    assertThat(sealedSnapshot.isLive()).isFalse();
    assertThat(sealedSnapshot.snapshotGeneration).isZero();
    assertThat(sealedSnapshot.maxOffset).isEqualTo(offset - 1);
    assertThat(sealedSnapshot.snapshotPath).isEqualTo(chunk.info().chunkId);
  }
}
