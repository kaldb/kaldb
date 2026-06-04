package com.slack.astra.blobfs.nrt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.nrt.NrtBlobStore.FileEntry;
import com.slack.astra.blobfs.nrt.NrtBlobStore.NrtManifest;
import java.util.List;
import org.junit.jupiter.api.Test;

class NrtBlobStoreTest {
  @Test
  void testManifestSerialization() {
    NrtManifest manifest = manifest();

    String manifestJson = NrtBlobStore.serializeManifest(manifest);
    NrtManifest deserializedManifest = NrtBlobStore.deserializeManifest(manifestJson);

    assertThat(deserializedManifest).isEqualTo(manifest);
    assertThat(deserializedManifest.sizeInBytesOnDisk()).isEqualTo(300);
  }

  @Test
  void testManifestValidation() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new NrtManifest(
                    1,
                    "snapshot-1",
                    "0",
                    "indexer-1",
                    1,
                    100,
                    1,
                    10,
                    9,
                    0,
                    1000,
                    2000,
                    fileEntry("schema"),
                    List.of(fileEntry("segments_1"))));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new NrtManifest(
                    1,
                    "snapshot-1",
                    "0",
                    "indexer-1",
                    1,
                    100,
                    1,
                    10,
                    10,
                    0,
                    1000,
                    2000,
                    fileEntry("schema"),
                    List.of()));
  }

  @Test
  void testBlobKeyLayout() {
    NrtBlobStore nrtBlobStore = new NrtBlobStore(mock(BlobStore.class));

    assertThat(nrtBlobStore.writeManifest(manifest()))
        .isEqualTo(
            "nrt/v1/partitions/0/chunks/snapshot-1/manifests/00000000000000000002-indexer-1.json");
  }

  @Test
  void testMalformedKeyRejection() {
    NrtBlobStore nrtBlobStore = new NrtBlobStore(mock(BlobStore.class));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> nrtBlobStore.writeManifest(manifest("../0", "snapshot-1", 1, "indexer-1")));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> nrtBlobStore.writeManifest(manifest("0", "snapshot/1", 1, "indexer-1")));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> nrtBlobStore.writeManifest(manifest("0", "snapshot-1", 0, "indexer-1")));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> nrtBlobStore.writeManifest(manifest("0", "snapshot-1", 1, "indexer/1")));
  }

  private static NrtManifest manifest() {
    return manifest("0", "snapshot-1", 2, "indexer-1");
  }

  private static NrtManifest manifest(
      String partitionId, String snapshotId, long manifestGeneration, String writerNodeId) {
    return new NrtManifest(
        1,
        snapshotId,
        partitionId,
        writerNodeId,
        manifestGeneration,
        1700000060000L,
        4,
        1000,
        2300,
        300,
        1700000000000L,
        1700000059000L,
        fileEntry("schema"),
        List.of(fileEntry("_0.cfe"), fileEntry("segments_4")));
  }

  private static FileEntry fileEntry(String name) {
    return new FileEntry(
        name, "nrt/v1/partitions/0/chunks/snapshot-1/files/" + name, 100, "checksum-" + name);
  }
}
