package com.slack.astra.metadata.snapshot;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class SnapshotMetadataSerializer implements MetadataSerializer<SnapshotMetadata> {
  private static Metadata.SnapshotMetadata toSnapshotMetadataProto(
      SnapshotMetadata snapshotMetadata) {
    Metadata.SnapshotMetadata.Builder builder =
        Metadata.SnapshotMetadata.newBuilder()
            .setName(snapshotMetadata.name)
            .setSnapshotId(snapshotMetadata.snapshotId)
            .setSnapshotPath(snapshotMetadata.snapshotPath)
            .setStartTimeEpochMs(snapshotMetadata.startTimeEpochMs)
            .setEndTimeEpochMs(snapshotMetadata.endTimeEpochMs)
            .setPartitionId(snapshotMetadata.partitionId)
            .setMaxOffset(snapshotMetadata.maxOffset)
            .setSizeInBytes(snapshotMetadata.sizeInBytesOnDisk)
            .setSnapshotType(toProtoSnapshotType(snapshotMetadata.snapshotType))
            .setIndexType(toProtoIndexType(snapshotMetadata.indexType))
            .setSnapshotGeneration(snapshotMetadata.snapshotGeneration)
            .setVersion(snapshotMetadata.version);
    if (snapshotMetadata.chunkId != null) {
      builder.setChunkId(snapshotMetadata.chunkId);
    }
    return builder.build();
  }

  private static SnapshotMetadata fromSnapshotMetadataProto(
      Metadata.SnapshotMetadata protoSnapshotMetadata) {
    SnapshotMetadata.SnapshotType snapshotType =
        fromProtoSnapshotType(
            protoSnapshotMetadata.getSnapshotType(), protoSnapshotMetadata.getSizeInBytes());
    return new SnapshotMetadata(
        protoSnapshotMetadata.getSnapshotId(),
        protoSnapshotMetadata.getStartTimeEpochMs(),
        protoSnapshotMetadata.getEndTimeEpochMs(),
        protoSnapshotMetadata.getMaxOffset(),
        protoSnapshotMetadata.getPartitionId(),
        protoSnapshotMetadata.getSizeInBytes(),
        snapshotType,
        fromProtoIndexType(protoSnapshotMetadata.getIndexType()),
        protoSnapshotMetadata.getSnapshotPath(),
        protoSnapshotMetadata.getSnapshotGeneration(),
        protoSnapshotMetadata.getVersion(),
        protoSnapshotMetadata.getChunkId().isBlank() ? null : protoSnapshotMetadata.getChunkId());
  }

  private static Metadata.SnapshotMetadata.SnapshotType toProtoSnapshotType(
      SnapshotMetadata.SnapshotType snapshotType) {
    return switch (snapshotType) {
      case LIVE -> Metadata.SnapshotMetadata.SnapshotType.LIVE;
      case SEALED -> Metadata.SnapshotMetadata.SnapshotType.SEALED;
    };
  }

  private static SnapshotMetadata.SnapshotType fromProtoSnapshotType(
      Metadata.SnapshotMetadata.SnapshotType snapshotType, long sizeInBytes) {
    return switch (snapshotType) {
      case LIVE -> SnapshotMetadata.SnapshotType.LIVE;
      case SEALED -> SnapshotMetadata.SnapshotType.SEALED;
      case SNAPSHOT_TYPE_UNSPECIFIED, UNRECOGNIZED ->
          sizeInBytes == 0
              ? SnapshotMetadata.SnapshotType.LIVE
              : SnapshotMetadata.SnapshotType.SEALED;
    };
  }

  private static Metadata.SnapshotMetadata.IndexType toProtoIndexType(
      SnapshotMetadata.IndexType indexType) {
    return switch (indexType) {
      case LUCENE -> Metadata.SnapshotMetadata.IndexType.LUCENE;
      case DUCKDB -> Metadata.SnapshotMetadata.IndexType.DUCKDB;
      case ROCKSDB -> Metadata.SnapshotMetadata.IndexType.ROCKSDB;
    };
  }

  private static SnapshotMetadata.IndexType fromProtoIndexType(
      Metadata.SnapshotMetadata.IndexType indexType) {
    return switch (indexType) {
      case LUCENE -> SnapshotMetadata.IndexType.LUCENE;
      case DUCKDB -> SnapshotMetadata.IndexType.DUCKDB;
      case ROCKSDB -> SnapshotMetadata.IndexType.ROCKSDB;
      case INDEX_TYPE_UNSPECIFIED, UNRECOGNIZED -> SnapshotMetadata.IndexType.LUCENE;
    };
  }

  @Override
  public String toJsonStr(SnapshotMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");

    return printer.print(toSnapshotMetadataProto(metadata));
  }

  @Override
  public SnapshotMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.SnapshotMetadata.Builder snapshotMetadataBuiler =
        Metadata.SnapshotMetadata.newBuilder();
    JsonFormat.parser().ignoringUnknownFields().merge(data, snapshotMetadataBuiler);
    return fromSnapshotMetadataProto(snapshotMetadataBuiler.build());
  }
}
