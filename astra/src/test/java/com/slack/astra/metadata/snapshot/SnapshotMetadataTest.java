package com.slack.astra.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.astra.metadata.snapshot.SnapshotMetadata.IndexType;
import com.slack.astra.metadata.snapshot.SnapshotMetadata.SnapshotType;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class SnapshotMetadataTest {
  @Test
  public void testSnapshotMetadata() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 0);

    assertThat(snapshotMetadata.name).isEqualTo(name);
    assertThat(snapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(snapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(snapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(snapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(snapshotMetadata.partitionId).isEqualTo(partitionId);
    assertThat(snapshotMetadata.snapshotType).isEqualTo(SnapshotType.LIVE);
    assertThat(snapshotMetadata.indexType).isEqualTo(IndexType.LUCENE);
    assertThat(snapshotMetadata.snapshotPath).isEmpty();
    assertThat(snapshotMetadata.snapshotGeneration).isZero();
    assertThat(snapshotMetadata.version).isEqualTo(SnapshotMetadata.DEFAULT_VERSION);
  }

  @Test
  public void testSealedSnapshotDefaults() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 100);

    assertThat(snapshotMetadata.snapshotType).isEqualTo(SnapshotType.SEALED);
    assertThat(snapshotMetadata.indexType).isEqualTo(IndexType.LUCENE);
    assertThat(snapshotMetadata.snapshotPath).isEqualTo(name);
    assertThat(snapshotMetadata.snapshotGeneration).isZero();
    assertThat(snapshotMetadata.version).isEqualTo(SnapshotMetadata.DEFAULT_VERSION);
  }

  @Test
  public void testFullSnapshotMetadataConstructor() {
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            "snapshot-1",
            1,
            100,
            123,
            "1",
            0,
            SnapshotType.LIVE,
            IndexType.LUCENE,
            "nrt/v1/partitions/1/chunks/snapshot-1/manifest.json",
            7,
            "2");

    assertThat(snapshotMetadata.snapshotPath)
        .isEqualTo("nrt/v1/partitions/1/chunks/snapshot-1/manifest.json");
    assertThat(snapshotMetadata.snapshotGeneration).isEqualTo(7);
    assertThat(snapshotMetadata.version).isEqualTo("2");
  }

  @Test
  public void testExplicitSnapshotTypeControlsDefaultPath() {
    SnapshotMetadata sealedSnapshot =
        new SnapshotMetadata(
            "snapshot-1",
            1,
            100,
            123,
            "1",
            0,
            SnapshotType.SEALED,
            IndexType.LUCENE,
            "",
            0,
            SnapshotMetadata.DEFAULT_VERSION);

    assertThat(sealedSnapshot.snapshotPath).isEqualTo("snapshot-1");
    assertThat(sealedSnapshot.isLive()).isFalse();
  }

  @Test
  public void testEqualsAndHashCode() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 0;
    final String partitionId = "1";

    SnapshotMetadata snapshot1 =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 0);
    SnapshotMetadata snapshot2 =
        new SnapshotMetadata(name + "2", startTime, endTime, maxOffset, partitionId, 0);

    // Ensure the name field from super class is included.
    assertThat(snapshot1).isNotEqualTo(snapshot2);
    Set<SnapshotMetadata> set = new HashSet<>();
    set.add(snapshot1);
    set.add(snapshot2);
    assertThat(set.size()).isEqualTo(2);
    assertThat(Map.of("1", snapshot1, "2", snapshot2).size()).isEqualTo(2);
  }

  @Test
  public void ensureValidSnapshotData() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata("", startTime, endTime, maxOffset, partitionId, 0));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, 0, endTime, maxOffset, partitionId, 0));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, startTime, 0, maxOffset, partitionId, 0));

    // Start time < end time
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new SnapshotMetadata(name, endTime, startTime, maxOffset, partitionId, 0));

    // Start time same as end time.
    assertThat(
            new SnapshotMetadata(name, startTime, startTime, maxOffset, partitionId, 0)
                .endTimeEpochMs)
        .isEqualTo(startTime);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, startTime, endTime, -1, partitionId, 0));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SnapshotMetadata(name, startTime, endTime, maxOffset, "", 0));

    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new SnapshotMetadata(
                    name,
                    startTime,
                    endTime,
                    maxOffset,
                    partitionId,
                    0,
                    SnapshotType.LIVE,
                    IndexType.LUCENE,
                    "",
                    -1,
                    SnapshotMetadata.DEFAULT_VERSION));
  }

  @Test
  public void testLive() {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    SnapshotMetadata nonLiveSnapshot =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 100);
    assertThat(nonLiveSnapshot.isLive()).isFalse();

    SnapshotMetadata liveSnapshot =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, 0);
    assertThat(liveSnapshot.isLive()).isTrue();
  }
}
