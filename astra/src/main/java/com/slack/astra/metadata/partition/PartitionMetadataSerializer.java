package com.slack.astra.metadata.partition;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.slack.astra.metadata.core.MetadataSerializer;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.metadata.Metadata;

public class PartitionMetadataSerializer implements MetadataSerializer<PartitionMetadata> {

  public static PartitionMetadata fromPartitionMetadataProto(
      Metadata.PartitionMetadata partitionMetadataProto) {
    return new PartitionMetadata(
        partitionMetadataProto.getPartitionId(), partitionMetadataProto.getMaxCapacity());
  }

  public static Metadata.PartitionMetadata toPartitionMetadataProto(PartitionMetadata metadata) {
    return Metadata.PartitionMetadata.newBuilder()
        .setPartitionId(metadata.getPartitionID())
        .setMaxCapacity(metadata.getMaxCapacity())
        .build();
  }

  public static CalculatedPartitionMetadata fromCalculatedPartitionMetadataProto(
      ManagerApi.CalculatedPartitionMetadata partitionMetadataProto) {
    PartitionOccupancy occupancy =
        switch (partitionMetadataProto.getOccupancyCase()) {
          case EMPTY -> new PartitionOccupancy.Empty();
          case SHARED ->
              new PartitionOccupancy.Shared(partitionMetadataProto.getShared().getDatasetsList());
          case DEDICATED ->
              new PartitionOccupancy.Dedicated(partitionMetadataProto.getDedicated().getDataset());
          case OCCUPANCY_NOT_SET -> new PartitionOccupancy.Empty();
        };
    return new CalculatedPartitionMetadata(
        partitionMetadataProto.getPartitionId(),
        partitionMetadataProto.getProvisionedCapacity(),
        partitionMetadataProto.getMaxCapacity(),
        occupancy);
  }

  public static ManagerApi.CalculatedPartitionMetadata toCalculatedPartitionMetadataProto(
      CalculatedPartitionMetadata metadata) {
    ManagerApi.CalculatedPartitionMetadata.Builder builder =
        ManagerApi.CalculatedPartitionMetadata.newBuilder()
            .setPartitionId(metadata.getPartitionID())
            .setProvisionedCapacity(metadata.getProvisionedCapacity())
            .setMaxCapacity(metadata.getMaxCapacity());

    PartitionOccupancy occupancy = metadata.getOccupancy();
    if (occupancy instanceof PartitionOccupancy.Empty) {
      builder.setEmpty(ManagerApi.EmptyPartitionOccupancy.newBuilder().build());
    } else if (occupancy instanceof PartitionOccupancy.Shared shared) {
      builder.setShared(
          ManagerApi.SharedPartitionOccupancy.newBuilder()
              .addAllDatasets(shared.datasets())
              .build());
    } else if (occupancy instanceof PartitionOccupancy.Dedicated dedicated) {
      builder.setDedicated(
          ManagerApi.DedicatedPartitionOccupancy.newBuilder()
              .setDataset(dedicated.dataset())
              .build());
    }
    return builder.build();
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
    JsonFormat.parser().ignoringUnknownFields().merge(data, partitionMetadataBuilder);
    return fromPartitionMetadataProto(partitionMetadataBuilder.build());
  }
}
