package com.slack.astra.metadata.partition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.metadata.Metadata;
import org.junit.jupiter.api.Test;

public class PartitionMetadataSerializerTest {
  private final PartitionMetadataSerializer serDe = new PartitionMetadataSerializer();

  @Test
  public void testPartitionMetadataSerializer() throws InvalidProtocolBufferException {
    PartitionMetadata partitionMetadata = new PartitionMetadata("1", 100);

    String serializedPartitionMetadata = serDe.toJsonStr(partitionMetadata);
    assertThat(serializedPartitionMetadata).isNotEmpty();

    PartitionMetadata deserializedPartitionMetadata =
        serDe.fromJsonStr(serializedPartitionMetadata);
    assertThat(deserializedPartitionMetadata).isEqualTo(partitionMetadata);
    assertThat(deserializedPartitionMetadata.getPartitionId()).isEqualTo("1");
    assertThat(deserializedPartitionMetadata.getMaxCapacity()).isEqualTo(100);
  }

  @Test
  public void testPartitionMetadataProtoConversion() {
    PartitionMetadata partitionMetadata = new PartitionMetadata("1", 100);

    Metadata.PartitionMetadata partitionMetadataProto =
        PartitionMetadataSerializer.toPartitionMetadataProto(partitionMetadata);
    assertThat(partitionMetadataProto.getPartitionId()).isEqualTo("1");
    assertThat(partitionMetadataProto.getMaxCapacity()).isEqualTo(100);

    assertThat(PartitionMetadataSerializer.fromPartitionMetadataProto(partitionMetadataProto))
        .isEqualTo(partitionMetadata);
  }

  @Test
  public void testInvalidPartitionMetadata() {
    Throwable nonNumericPartitionId =
        catchThrowable(() -> new PartitionMetadata("partition-a", 100));
    assertThat(nonNumericPartitionId).isInstanceOf(IllegalArgumentException.class);

    Throwable nonPositiveCapacity = catchThrowable(() -> new PartitionMetadata("1", 0));
    assertThat(nonPositiveCapacity)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("maxCapacity must be greater than 0");
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
