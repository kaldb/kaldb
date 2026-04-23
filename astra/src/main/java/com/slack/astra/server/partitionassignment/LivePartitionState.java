package com.slack.astra.server.partitionassignment;

import com.google.common.collect.ImmutableList;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Partition capacity plus currently calculated usage from dataset assignments. */
public record LivePartitionState(
    String partitionId, long provisionedCapacity, long maxCapacity, PartitionOccupancy occupancy) {
  private static final Logger LOG = LoggerFactory.getLogger(LivePartitionState.class);

  public LivePartitionState {
    Objects.requireNonNull(occupancy, "occupancy");
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

  public boolean isExclusivelyUsedBy(String datasetName) {
    return occupancy.isExclusivelyUsedBy(datasetName);
  }

  public long getAvailableCapacity() {
    return maxCapacity - provisionedCapacity;
  }

  static List<LivePartitionState> loadAll(
      DatasetMetadataStore datasetMetadataStore, PartitionMetadataStore partitionMetadataStore) {
    // TODO(shard-autoassignment): listSync() is cache-backed in the ZooKeeper path. Rapid
    // back-to-back manager updates can therefore compute placement or render ListPartition from a
    // stale global view even though manager RPC entrypoints are synchronized. Revisit whether this
    // path needs a direct read or stronger coordination for correctness-sensitive callers.
    return fromMetadata(datasetMetadataStore.listSync(), partitionMetadataStore.listSync());
  }

  public static List<LivePartitionState> fromMetadata(
      List<DatasetMetadata> datasetMetadataList, List<PartitionMetadata> partitionMetadataList) {
    final Map<String, List<String>> partitionDatasets = new HashMap<>();
    final Map<String, Long> partitionProvisioning = new HashMap<>();
    final Map<String, String> partitionDedicatedOwner = new HashMap<>();
    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      partitionProvisioning.put(partitionMetadata.getPartitionId(), 0L);
      partitionDatasets.put(partitionMetadata.getPartitionId(), new ArrayList<>());
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
          if (existingDedicatedOwner != null
              && !existingDedicatedOwner.equals(datasetMetadata.getName())) {
            // TODO(shard-autoassignment): with independently refreshed cached metadata lists, a
            // rapid sequence of updates can transiently make this derived view look impossible even
            // when the underlying writes are converging. Decide whether ListPartition /
            // UpdatePartitionAssignment should keep failing closed here or degrade this into an
            // operator-visible inconsistent-state marker.
            throw new InvalidPartitionAssignmentStateException(
                "partition %s cannot be dedicated to multiple datasets".formatted(partitionId));
          }
        }
      }
    }

    return partitionMetadataList.stream()
        .map(
            partitionMetadata ->
                fromCurrentAssignments(
                    partitionMetadata,
                    partitionProvisioning.get(partitionMetadata.getPartitionId()),
                    partitionDatasets.get(partitionMetadata.getPartitionId()),
                    partitionDedicatedOwner.get(partitionMetadata.getPartitionId())))
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
      if (datasets.size() != 1 || !datasets.get(0).equals(dedicatedOwner)) {
        throw new InvalidPartitionAssignmentStateException(
            "partition %s has inconsistent occupancy: dedicated owner %s but datasets %s"
                .formatted(partitionMetadata.getPartitionId(), dedicatedOwner, datasets));
      }
      occupancy = new PartitionOccupancy.Dedicated(dedicatedOwner);
    }

    return new LivePartitionState(
        partitionMetadata.getPartitionId(),
        provisionedCapacity,
        partitionMetadata.getMaxCapacity(),
        occupancy);
  }
}
