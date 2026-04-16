package com.slack.astra.metadata.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.metadata.Metadata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PartitionMetadataSerializerTest {
  private final PartitionMetadataSerializer serDe = new PartitionMetadataSerializer();

  @Test
  public void testPartitionMetadataSerializer() throws InvalidProtocolBufferException {
    PartitionMetadata partitionMetadata = new PartitionMetadata("partition-a", 100);

    String serializedPartitionMetadata = serDe.toJsonStr(partitionMetadata);
    assertThat(serializedPartitionMetadata).isNotEmpty();

    PartitionMetadata deserializedPartitionMetadata =
        serDe.fromJsonStr(serializedPartitionMetadata);
    assertThat(deserializedPartitionMetadata).isEqualTo(partitionMetadata);
    assertThat(deserializedPartitionMetadata.getPartitionID()).isEqualTo("partition-a");
    assertThat(deserializedPartitionMetadata.getMaxCapacity()).isEqualTo(100);
  }

  @Test
  public void testPartitionMetadataProtoConversion() {
    PartitionMetadata partitionMetadata = new PartitionMetadata("partition-a", 100);

    Metadata.PartitionMetadata partitionMetadataProto =
        PartitionMetadataSerializer.toPartitionMetadataProto(partitionMetadata);
    assertThat(partitionMetadataProto.getPartitionId()).isEqualTo("partition-a");
    assertThat(partitionMetadataProto.getMaxCapacity()).isEqualTo(100);

    assertThat(PartitionMetadataSerializer.fromPartitionMetadataProto(partitionMetadataProto))
        .isEqualTo(partitionMetadata);
  }

  @Test
  public void testCalculatedPartitionMetadataProtoConversion() {
    List<CalculatedPartitionMetadata> calculatedPartitionMetadata =
        List.of(
            new CalculatedPartitionMetadata(
                "partition-empty", 0, 100, new PartitionOccupancy.Empty()),
            new CalculatedPartitionMetadata(
                "partition-shared",
                25,
                100,
                new PartitionOccupancy.Shared(List.of("dataset-a", "dataset-b"))),
            new CalculatedPartitionMetadata(
                "partition-dedicated", 25, 100, new PartitionOccupancy.Dedicated("dataset-a")));

    List<ManagerApi.CalculatedPartitionMetadata> calculatedPartitionMetadataProto =
        calculatedPartitionMetadata.stream()
            .map(PartitionMetadataSerializer::toCalculatedPartitionMetadataProto)
            .toList();

    assertThat(calculatedPartitionMetadataProto.get(0).hasEmpty()).isTrue();
    assertThat(calculatedPartitionMetadataProto.get(1).getShared().getDatasetsList())
        .containsExactly("dataset-a", "dataset-b");
    assertThat(calculatedPartitionMetadataProto.get(2).getDedicated().getDataset())
        .isEqualTo("dataset-a");
    assertThat(
            calculatedPartitionMetadataProto.stream()
                .map(PartitionMetadataSerializer::fromCalculatedPartitionMetadataProto)
                .toList())
        .containsExactlyElementsOf(calculatedPartitionMetadata);
  }

  @Test
  public void testInvalidSerializations() {
    Throwable serializeNull = catchThrowable(() -> serDe.toJsonStr(null));
    assertThat(serializeNull).isInstanceOf(IllegalArgumentException.class);

    Throwable deserializeNull = catchThrowable(() -> serDe.fromJsonStr(null));
    assertThat(deserializeNull).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeEmpty = catchThrowable(() -> serDe.fromJsonStr(""));
    assertThat(deserializeEmpty).isInstanceOf(InvalidProtocolBufferException.class);

    Throwable deserializeCorrupt = catchThrowable(() -> serDe.fromJsonStr("test"));
    assertThat(deserializeCorrupt).isInstanceOf(InvalidProtocolBufferException.class);
  }
}
