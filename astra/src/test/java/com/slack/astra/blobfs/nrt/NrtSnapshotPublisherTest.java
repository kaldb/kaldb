package com.slack.astra.blobfs.nrt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class NrtSnapshotPublisherTest {
  private static final String SNAPSHOT_ID = "LIVE_chunk-1";
  private static final String PARTITION_ID = "0";
  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Test
  void testSuccessfulPublishUpdatesManifestAndSnapshotMetadata() throws Exception {
    TestContext context = testContext("", 0);

    SnapshotMetadata updated =
        context.publisher.publish(context.logStore, context.snapshotMetadata, 10);

    assertThat(updated.snapshotGeneration).isEqualTo(1);
    assertThat(updated.snapshotPath).isEqualTo(context.snapshotPath);
    assertThat(updated.sizeInBytesOnDisk).isEqualTo(236);
    verify(context.snapshotMetadataStore).updateSync(updated);
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_TOTAL)
                .counter()
                .count())
        .isEqualTo(1);
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_FAILED_TOTAL)
                .counter()
                .count())
        .isZero();

    ArgumentCaptor<String> manifestCaptor = ArgumentCaptor.forClass(String.class);
    verify(context.blobStore)
        .uploadData(eq(context.snapshotPath), manifestCaptor.capture(), eq(false));
    NrtBlobStore.NrtManifest manifest = NrtBlobStore.deserializeManifest(manifestCaptor.getValue());
    assertThat(manifest.manifestGeneration()).isEqualTo(1);
    assertThat(manifest.startOffsetInclusive()).isEqualTo(10);
    assertThat(manifest.sizeInBytesOnDisk()).isEqualTo(236);
    assertThat(manifest.schemaFile().key())
        .isEqualTo(context.filesPath + "/" + ReadWriteChunk.SCHEMA_FILE_NAME);
    assertThat(manifest.schemaFile().checksum()).hasSize(64);
    assertThat(manifest.files())
        .extracting(NrtBlobStore.FileEntry::name)
        .containsExactly("segments_1");
    assertThat(manifest.files())
        .extracting(NrtBlobStore.FileEntry::checksum)
        .allMatch(c -> c.length() == 64);
  }

  @Test
  void testFileUploadFailureDoesNotWriteManifestOrMetadata() throws Exception {
    TestContext context = testContext("", 0);
    doThrow(new RuntimeException("upload failed"))
        .when(context.blobStore)
        .upload(anyString(), any());

    assertThatThrownBy(
            () -> context.publisher.publish(context.logStore, context.snapshotMetadata, 0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("upload failed");

    verify(context.blobStore, never()).uploadData(anyString(), anyString(), anyBoolean());
    verify(context.snapshotMetadataStore, never()).updateSync(any());
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_TOTAL)
                .counter()
                .count())
        .isZero();
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_FAILED_TOTAL)
                .counter()
                .count())
        .isEqualTo(1);
  }

  @Test
  void testManifestUploadFailureDoesNotUpdateMetadata() throws Exception {
    TestContext context = testContext("", 0);
    doThrow(new RuntimeException("manifest failed"))
        .when(context.blobStore)
        .uploadData(anyString(), anyString(), anyBoolean());

    assertThatThrownBy(
            () -> context.publisher.publish(context.logStore, context.snapshotMetadata, 0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("manifest failed");

    verify(context.snapshotMetadataStore, never()).updateSync(any());
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_FAILED_TOTAL)
                .counter()
                .count())
        .isEqualTo(1);
  }

  @Test
  void testMetadataUpdateFailureLeavesManifestWritten() throws Exception {
    TestContext context = testContext("", 0);
    doThrow(new RuntimeException("metadata failed"))
        .when(context.snapshotMetadataStore)
        .updateSync(any());

    assertThatThrownBy(
            () -> context.publisher.publish(context.logStore, context.snapshotMetadata, 0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("metadata failed");

    verify(context.blobStore).uploadData(eq(context.snapshotPath), anyString(), eq(false));
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_FAILED_TOTAL)
                .counter()
                .count())
        .isEqualTo(1);
  }

  @Test
  void testExistingManifestDoesNotBlockSingleWriterPublish() throws Exception {
    TestContext context = testContext("previous-manifest.json", 1);
    NrtBlobStore.NrtManifest currentManifest = manifest(context.filesPath, 1);
    when(context.blobStore.readFileData("previous-manifest.json", false))
        .thenReturn(NrtBlobStore.serializeManifest(currentManifest));

    SnapshotMetadata updated =
        context.publisher.publish(context.logStore, context.snapshotMetadata, 0);

    assertThat(updated.snapshotGeneration).isEqualTo(2);
    verify(context.blobStore).upload(anyString(), any());
    verify(context.blobStore).uploadData(eq(context.snapshotPath), anyString(), eq(false));
    verify(context.snapshotMetadataStore).updateSync(updated);
    assertThat(
            meterRegistry
                .get(NrtSnapshotPublisher.NRT_LIVE_SNAPSHOT_PUBLISH_TOTAL)
                .counter()
                .count())
        .isEqualTo(1);
  }

  private TestContext testContext(String currentSnapshotPath, long generation) throws Exception {
    Path indexDirectory = Files.createTempDirectory("nrt-publisher-test");
    Files.writeString(indexDirectory.resolve("segments_1"), "segment data");

    BlobStore blobStore = mock(BlobStore.class);
    NrtBlobStore nrtBlobStore = new NrtBlobStore(blobStore);
    SnapshotMetadataStore snapshotMetadataStore = mock(SnapshotMetadataStore.class);
    LogStore logStore = mock(LogStore.class);
    IndexCommit indexCommit = mock(IndexCommit.class);
    FSDirectory directory = FSDirectory.open(indexDirectory);
    ConcurrentHashMap<String, LuceneFieldDef> schema = new ConcurrentHashMap<>();
    schema.put("message", new LuceneFieldDef("message", "keyword", true, true, false));

    when(logStore.getDirectory()).thenReturn(directory);
    when(logStore.getIndexCommit()).thenReturn(indexCommit);
    when(logStore.getSchema()).thenReturn(schema);
    when(indexCommit.getFileNames()).thenReturn(List.of("segments_1"));
    when(indexCommit.getGeneration()).thenReturn(7L);

    String snapshotPath =
        NrtBlobStore.manifestPath(PARTITION_ID, SNAPSHOT_ID, generation + 1, "indexer-1");
    String filesPath = NrtBlobStore.filesPath(PARTITION_ID, SNAPSHOT_ID);
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            SNAPSHOT_ID,
            1000,
            2000,
            25,
            PARTITION_ID,
            0,
            SnapshotMetadata.SnapshotType.LIVE,
            SnapshotMetadata.IndexType.LUCENE,
            currentSnapshotPath,
            generation,
            SnapshotMetadata.DEFAULT_VERSION);

    return new TestContext(
        blobStore,
        nrtBlobStore,
        snapshotMetadataStore,
        logStore,
        new NrtSnapshotPublisher(
            blobStore, nrtBlobStore, snapshotMetadataStore, "indexer-1", meterRegistry),
        snapshotMetadata,
        snapshotPath,
        filesPath);
  }

  private static NrtBlobStore.NrtManifest manifest(String filesPath, long manifestGeneration) {
    return new NrtBlobStore.NrtManifest(
        1,
        SNAPSHOT_ID,
        PARTITION_ID,
        "indexer-1",
        manifestGeneration,
        1000,
        7,
        0,
        25,
        236,
        1000,
        2000,
        new NrtBlobStore.FileEntry(
            ReadWriteChunk.SCHEMA_FILE_NAME,
            filesPath + "/" + ReadWriteChunk.SCHEMA_FILE_NAME,
            10,
            "10"),
        List.of(new NrtBlobStore.FileEntry("segments_1", filesPath + "/segments_1", 12, "12")));
  }

  private record TestContext(
      BlobStore blobStore,
      NrtBlobStore nrtBlobStore,
      SnapshotMetadataStore snapshotMetadataStore,
      LogStore logStore,
      NrtSnapshotPublisher publisher,
      SnapshotMetadata snapshotMetadata,
      String snapshotPath,
      String filesPath) {}
}
