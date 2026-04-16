package com.slack.astra.metadata.partition;

import java.util.Objects;

/** Partition capacity plus currently calculated exclusive owner usage. */
public class CalculatedPartitionMetadata {
  public final String partitionId;
  public final long provisionedCapacity;
  public final long maxCapacity;
  public final String ownerDataset;

  public CalculatedPartitionMetadata(
      String partitionId, long provisionedCapacity, long maxCapacity, String ownerDataset) {
    this.partitionId = partitionId;
    this.provisionedCapacity = provisionedCapacity;
    this.maxCapacity = maxCapacity;
    this.ownerDataset = ownerDataset == null ? "" : ownerDataset;
  }

  public String getPartitionID() {
    return partitionId;
  }

  public long getProvisionedCapacity() {
    return provisionedCapacity;
  }

  public long getMaxCapacity() {
    return maxCapacity;
  }

  public String getOwnerDataset() {
    return ownerDataset;
  }

  public boolean isUnassigned() {
    return ownerDataset.isEmpty();
  }

  public long getAvailableCapacity() {
    return maxCapacity - provisionedCapacity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CalculatedPartitionMetadata that)) return false;
    return provisionedCapacity == that.provisionedCapacity
        && maxCapacity == that.maxCapacity
        && Objects.equals(partitionId, that.partitionId)
        && Objects.equals(ownerDataset, that.ownerDataset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, provisionedCapacity, maxCapacity, ownerDataset);
  }

  @Override
  public String toString() {
    return "CalculatedPartitionMetadata{"
        + "partitionId='"
        + partitionId
        + '\''
        + ", provisionedCapacity="
        + provisionedCapacity
        + ", maxCapacity="
        + maxCapacity
        + ", ownerDataset='"
        + ownerDataset
        + '\''
        + '}';
  }
}
