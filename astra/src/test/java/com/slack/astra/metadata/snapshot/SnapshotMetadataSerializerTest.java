package com.slack.astra.metadata.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.metadata.snapshot.SnapshotMetadata.IndexType;
import com.slack.astra.metadata.snapshot.SnapshotMetadata.SnapshotType;
import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class SnapshotMetadataSerializerTest {
  private final SnapshotMetadataSerializer serDe = new SnapshotMetadataSerializer();

  @Test
  public void testSnapshotMetadataSerializer() throws InvalidProtocolBufferException {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";
    final long sizeInBytes = 0;

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(name, startTime, endTime, maxOffset, partitionId, sizeInBytes);

    String serializedSnapshot = serDe.toJsonStr(snapshotMetadata);
    assertThat(serializedSnapshot).isNotEmpty();

    SnapshotMetadata deserializedSnapshotMetadata = serDe.fromJsonStr(serializedSnapshot);
    assertThat(deserializedSnapshotMetadata).isEqualTo(snapshotMetadata);

    assertThat(deserializedSnapshotMetadata.name).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.chunkId).isNull();
    assertThat(deserializedSnapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(deserializedSnapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(deserializedSnapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(deserializedSnapshotMetadata.partitionId).isEqualTo(partitionId);
    assertThat(deserializedSnapshotMetadata.sizeInBytesOnDisk).isEqualTo(sizeInBytes);
    assertThat(deserializedSnapshotMetadata.snapshotType).isEqualTo(SnapshotType.LIVE);
    assertThat(deserializedSnapshotMetadata.indexType).isEqualTo(IndexType.LUCENE);
    assertThat(deserializedSnapshotMetadata.snapshotPath).isEmpty();
    assertThat(deserializedSnapshotMetadata.snapshotGeneration).isZero();
    assertThat(deserializedSnapshotMetadata.version).isEqualTo(SnapshotMetadata.DEFAULT_VERSION);
  }

  @Test
  public void testSnapshotMetadataSerializerWithNrtFields() throws InvalidProtocolBufferException {
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
            3,
            "2",
            "snapshot-1");

    String serializedSnapshot = serDe.toJsonStr(snapshotMetadata);

    SnapshotMetadata deserializedSnapshotMetadata = serDe.fromJsonStr(serializedSnapshot);
    assertThat(deserializedSnapshotMetadata).isEqualTo(snapshotMetadata);
    assertThat(deserializedSnapshotMetadata.chunkId).isEqualTo("snapshot-1");
    assertThat(deserializedSnapshotMetadata.snapshotPath)
        .isEqualTo("nrt/v1/partitions/1/chunks/snapshot-1/manifest.json");
    assertThat(deserializedSnapshotMetadata.snapshotGeneration).isEqualTo(3);
    assertThat(deserializedSnapshotMetadata.version).isEqualTo("2");
  }

  @Test
  public void testDeserializingWithoutSizeField() throws InvalidProtocolBufferException {
    final String name = "testSnapshotId";
    final long startTime = 1;
    final long endTime = 100;
    final long maxOffset = 123;
    final String partitionId = "1";

    Metadata.SnapshotMetadata protoSnapshotMetadata =
        Metadata.SnapshotMetadata.newBuilder()
            .setName(name)
            .setSnapshotId(name)
            .setStartTimeEpochMs(startTime)
            .setEndTimeEpochMs(endTime)
            .setMaxOffset(maxOffset)
            .setPartitionId(partitionId)
            // leaving out the `size` field
            .build();

    SnapshotMetadata deserializedSnapshotMetadata =
        serDe.fromJsonStr(serDe.printer.print(protoSnapshotMetadata));

    // Assert size is 0
    assertThat(deserializedSnapshotMetadata.sizeInBytesOnDisk).isEqualTo(0);
    assertThat(deserializedSnapshotMetadata.snapshotType).isEqualTo(SnapshotType.LIVE);
    assertThat(deserializedSnapshotMetadata.indexType).isEqualTo(IndexType.LUCENE);
    assertThat(deserializedSnapshotMetadata.chunkId).isNull();
    assertThat(deserializedSnapshotMetadata.snapshotPath).isEmpty();
    assertThat(deserializedSnapshotMetadata.version).isEqualTo(SnapshotMetadata.DEFAULT_VERSION);

    // Assert everything else is deserialized correctly
    assertThat(deserializedSnapshotMetadata.name).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.snapshotId).isEqualTo(name);
    assertThat(deserializedSnapshotMetadata.startTimeEpochMs).isEqualTo(startTime);
    assertThat(deserializedSnapshotMetadata.endTimeEpochMs).isEqualTo(endTime);
    assertThat(deserializedSnapshotMetadata.maxOffset).isEqualTo(maxOffset);
    assertThat(deserializedSnapshotMetadata.partitionId).isEqualTo(partitionId);
  }

  @Test
  public void testDeserializingOldSealedSnapshotDefaultsNewFields()
      throws InvalidProtocolBufferException {
    final String name = "testSnapshotId";

    Metadata.SnapshotMetadata protoSnapshotMetadata =
        Metadata.SnapshotMetadata.newBuilder()
            .setName(name)
            .setSnapshotId(name)
            .setStartTimeEpochMs(1)
            .setEndTimeEpochMs(100)
            .setMaxOffset(123)
            .setPartitionId("1")
            .setSizeInBytes(100)
            .build();

    SnapshotMetadata deserializedSnapshotMetadata =
        serDe.fromJsonStr(serDe.printer.print(protoSnapshotMetadata));

    assertThat(deserializedSnapshotMetadata.snapshotType).isEqualTo(SnapshotType.SEALED);
    assertThat(deserializedSnapshotMetadata.indexType).isEqualTo(IndexType.LUCENE);
    assertThat(deserializedSnapshotMetadata.chunkId).isNull();
    assertThat(deserializedSnapshotMetadata.snapshotPath).isEmpty();
    assertThat(deserializedSnapshotMetadata.snapshotGeneration).isZero();
    assertThat(deserializedSnapshotMetadata.version).isEqualTo(SnapshotMetadata.DEFAULT_VERSION);
  }

  @Test
  public void testDeserializingLegacyLiveSnapshotKeepsChunkIdUnset()
      throws InvalidProtocolBufferException {
    Metadata.SnapshotMetadata protoSnapshotMetadata =
        Metadata.SnapshotMetadata.newBuilder()
            .setName("LIVE_snapshot-1")
            .setSnapshotId("LIVE_snapshot-1")
            .setStartTimeEpochMs(1)
            .setEndTimeEpochMs(100)
            .setMaxOffset(123)
            .setPartitionId("1")
            .setSizeInBytes(0)
            .build();

    SnapshotMetadata deserializedSnapshotMetadata =
        serDe.fromJsonStr(serDe.printer.print(protoSnapshotMetadata));

    assertThat(deserializedSnapshotMetadata.chunkId).isNull();
    assertThat(deserializedSnapshotMetadata.snapshotPath).isEmpty();
  }

  @Test
  public void serializeNullObject() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> serDe.toJsonStr(null));
  }

  @Test
  public void deserializeNullObject() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> serDe.fromJsonStr(null));
  }

  @Test
  public void deserializeEmptyObject() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> serDe.fromJsonStr(""));
  }

  @Test
  public void deserializeTestString() {
    assertThatExceptionOfType(InvalidProtocolBufferException.class)
        .isThrownBy(() -> serDe.fromJsonStr("test"));
  }
}
