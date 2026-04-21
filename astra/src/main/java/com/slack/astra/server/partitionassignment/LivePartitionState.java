package com.slack.astra.server.partitionassignment;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Partition capacity plus currently calculated usage from dataset assignments. */
public class LivePartitionState {
  private static final Logger LOG = LoggerFactory.getLogger(LivePartitionState.class);
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

  public static List<LivePartitionState> fromMetadata(
      List<DatasetMetadata> datasetMetadataList, List<PartitionMetadata> partitionMetadataList) {
    final Map<String, List<String>> partitionDatasets = new HashMap<>();
    final Map<String, Long> partitionProvisioning = new HashMap<>();
    final Map<String, String> partitionDedicatedOwner = new HashMap<>();
    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      partitionProvisioning.put(partitionMetadata.getPartitionID(), 0L);
      partitionDatasets.put(partitionMetadata.getPartitionID(), new ArrayList<>());
    }

    for (DatasetMetadata datasetMetadata : datasetMetadataList) {
      Optional<DatasetPartitionMetadata> activePartitionMetadata =
          datasetMetadata.getActivePartitionMetadata();
      long perPartitionValue = datasetMetadata.getActivePerPartitionThroughput();
      boolean useDedicatedPartition = datasetMetadata.isUsingDedicatedPartitions();
      for (String partitionId :
          activePartitionMetadata
              .map(DatasetPartitionMetadata::getPartitions)
              .orElse(ImmutableList.of())) {
        if (!partitionProvisioning.containsKey(partitionId)) {
          LOG.warn(
              "Dataset {} references partition {} that is not in the partition catalog",
              datasetMetadata.getName(),
              partitionId);
          continue;
        }
        partitionProvisioning.put(
            partitionId, perPartitionValue + partitionProvisioning.getOrDefault(partitionId, 0L));
        partitionDatasets.get(partitionId).add(datasetMetadata.getName());
        if (useDedicatedPartition) {
          String existingDedicatedOwner =
              partitionDedicatedOwner.putIfAbsent(partitionId, datasetMetadata.getName());
          Preconditions.checkArgument(
              existingDedicatedOwner == null
                  || existingDedicatedOwner.equals(datasetMetadata.getName()),
              "partition %s cannot be dedicated to multiple datasets".formatted(partitionId));
        }
      }
    }

    return partitionMetadataList.stream()
        .sorted(Comparator.comparing(PartitionMetadata::getPartitionID))
        .map(
            partitionMetadata ->
                fromCurrentAssignments(
                    partitionMetadata,
                    partitionProvisioning.get(partitionMetadata.getPartitionID()),
                    partitionDatasets.get(partitionMetadata.getPartitionID()),
                    partitionDedicatedOwner.get(partitionMetadata.getPartitionID())))
        .toList();
  }

  private static LivePartitionState fromCurrentAssignments(
      PartitionMetadata partitionMetadata,
      long provisionedCapacity,
      List<String> datasets,
      String dedicatedOwner) {
    PartitionOccupancy occupancy;
    if (datasets.isEmpty()) {
      occupancy = new PartitionOccupancy.Empty();
    } else if (dedicatedOwner == null) {
      occupancy = new PartitionOccupancy.Shared(datasets);
    } else {
      Preconditions.checkArgument(
          datasets.size() == 1 && datasets.get(0).equals(dedicatedOwner),
          "partition occupancy must be empty, shared, or dedicated to exactly one dataset");
      occupancy = new PartitionOccupancy.Dedicated(dedicatedOwner);
    }

    return new LivePartitionState(
        partitionMetadata.getPartitionID(),
        provisionedCapacity,
        partitionMetadata.getMaxCapacity(),
        occupancy);
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
