package com.slack.astra.metadata.partition;

import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Objects;

/** Tracks the maximum provisionable capacity of a Kafka partition. */
public class PartitionMetadata extends AstraMetadata {
  public final String partitionId;
  public final long maxCapacity;

  public PartitionMetadata(String partitionId, long maxCapacity) {
    super(partitionId);
    this.partitionId = partitionId;
    this.maxCapacity = maxCapacity;
  }

  public String getPartitionID() {
    return partitionId;
  }

  public long getMaxCapacity() {
    return maxCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PartitionMetadata that)) return false;
    if (!super.equals(o)) return false;
    return maxCapacity == that.maxCapacity;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), maxCapacity);
  }

  @Override
  public String toString() {
    return "PartitionMetadata{"
        + "partitionId='"
        + partitionId
        + '\''
        + ", maxCapacity="
        + maxCapacity
        + '}';
  }
}
