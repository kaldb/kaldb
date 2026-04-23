package com.slack.astra.metadata.partition;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.metadata.Metadata;

public class PartitionMetadataSerializer implements MetadataSerializer<PartitionMetadata> {

  public static PartitionMetadata fromPartitionMetadataProto(
      Metadata.PartitionMetadata partitionMetadataProto) {
    return new PartitionMetadata(
        partitionMetadataProto.getPartitionId(), partitionMetadataProto.getMaxCapacity());
  }

  public static Metadata.PartitionMetadata toPartitionMetadataProto(PartitionMetadata metadata) {
    return Metadata.PartitionMetadata.newBuilder()
        .setPartitionId(metadata.getPartitionId())
        .setMaxCapacity(metadata.getMaxCapacity())
        .build();
  }

  @Override
  public String toJsonStr(PartitionMetadata metadata) throws InvalidProtocolBufferException {
    if (metadata == null) throw new IllegalArgumentException("metadata object can't be null");
    return printer.print(toPartitionMetadataProto(metadata));
  }

  @Override
  public PartitionMetadata fromJsonStr(String data) throws InvalidProtocolBufferException {
    Metadata.PartitionMetadata.Builder partitionMetadataBuilder =
        Metadata.PartitionMetadata.newBuilder();
    parser.merge(data, partitionMetadataBuilder);
    return fromPartitionMetadataProto(partitionMetadataBuilder.build());
  }
}
