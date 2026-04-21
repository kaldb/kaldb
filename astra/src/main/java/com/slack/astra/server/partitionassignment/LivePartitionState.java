package com.slack.astra.server.partitionassignment;

import java.util.Objects;

/** Partition capacity plus currently calculated usage from dataset assignments. */
public class LivePartitionState {
  public final String partitionId;
  public final long provisionedCapacity;
  public final long maxCapacity;
  public final PartitionOccupancy occupancy;

  public LivePartitionState(
      String partitionId,
      long provisionedCapacity,
      long maxCapacity,
      PartitionOccupancy occupancy) {
    this.partitionId = partitionId;
    this.provisionedCapacity = provisionedCapacity;
    this.maxCapacity = maxCapacity;
    this.occupancy = Objects.requireNonNull(occupancy, "occupancy");
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
    if (!(o instanceof LivePartitionState that)) return false;
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
    return "LivePartitionState{"
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
