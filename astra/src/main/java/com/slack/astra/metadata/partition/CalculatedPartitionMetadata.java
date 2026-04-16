package com.slack.astra.metadata.partition;

import java.util.List;
import java.util.Objects;

/** Partition capacity plus currently calculated usage from dataset assignments. */
public class CalculatedPartitionMetadata {
  public final String partitionId;
  public final long provisionedCapacity;
  public final long maxCapacity;
  public final PartitionOccupancy occupancy;

  public CalculatedPartitionMetadata(
      String partitionId,
      long provisionedCapacity,
      long maxCapacity,
      PartitionOccupancy occupancy) {
    this.partitionId = partitionId;
    this.provisionedCapacity = provisionedCapacity;
    this.maxCapacity = maxCapacity;
    this.occupancy = Objects.requireNonNull(occupancy, "occupancy");
  }

  public static CalculatedPartitionMetadata fromDatasetAssignments(
      String partitionId,
      long provisionedCapacity,
      long maxCapacity,
      List<String> datasets,
      List<String> dedicatedDatasets) {
    return new CalculatedPartitionMetadata(
        partitionId,
        provisionedCapacity,
        maxCapacity,
        PartitionOccupancy.from(datasets, dedicatedDatasets));
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

  public PartitionOccupancy getOccupancy() {
    return occupancy;
  }

  public boolean isEmpty() {
    return occupancy.isEmpty();
  }

  public boolean canUseForSharedAssignment(String datasetName) {
    return occupancy.canUseForSharedAssignment(datasetName);
  }

  public boolean isDedicatedOnlyTo(String datasetName) {
    return occupancy.isDedicatedOnlyTo(datasetName);
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
        && Objects.equals(occupancy, that.occupancy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, provisionedCapacity, maxCapacity, occupancy);
  }

  @Override
  public String toString() {
    return "CalculatedPartitionMetadata{"
        + "partitionId='"
        + partitionId
        + "'"
        + ", provisionedCapacity="
        + provisionedCapacity
        + ", maxCapacity="
        + maxCapacity
        + ", occupancy="
        + occupancy
        + '}';
  }
}
