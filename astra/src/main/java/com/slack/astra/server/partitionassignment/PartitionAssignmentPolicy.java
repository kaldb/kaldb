package com.slack.astra.server.partitionassignment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Shared feasibility rules for validating partition assignments against the current live state. */
public final class PartitionAssignmentPolicy {
  private PartitionAssignmentPolicy() {}

  public static void validateManualAssignment(
      DatasetMetadata datasetMetadata,
      long throughputBytes,
      boolean requireDedicatedPartition,
      List<String> requestedPartitionIds,
      List<LivePartitionState> livePartitionStates,
      long minNumberOfPartitions) {
    if (requestedPartitionIds.size() < minNumberOfPartitions) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed at least %d partitions, found %d: %s"
                  .formatted(
                      minNumberOfPartitions, requestedPartitionIds.size(), requestedPartitionIds))
          .asRuntimeException();
    }

    Set<String> currentIds = ImmutableSet.copyOf(datasetMetadata.getActivePartitionIds());
    Map<String, LivePartitionState> statesById =
        liveStatesWithoutSelfContribution(datasetMetadata, currentIds, livePartitionStates).stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    LivePartitionState::getPartitionID, partitionState -> partitionState));
    long demandPerPartition = perPartitionDemand(throughputBytes, requestedPartitionIds.size());

    List<String> invalidPartitionIds =
        requestedPartitionIds.stream()
            .filter(
                partitionId ->
                    !supportsManualAssignment(
                        statesById.get(partitionId),
                        datasetMetadata.getName(),
                        requireDedicatedPartition,
                        demandPerPartition))
            .toList();
    if (!invalidPartitionIds.isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "%s assignment requires each selected partition to be eligible and have at least %d capacity; invalid partitions: %s"
                  .formatted(
                      requireDedicatedPartition ? "Dedicated" : "Shared",
                      demandPerPartition,
                      invalidPartitionIds))
          .asRuntimeException();
    }
  }

  static long perPartitionDemand(long totalThroughput, long partitionCount) {
    return Math.ceilDiv(totalThroughput, partitionCount);
  }

  static List<LivePartitionState> liveStatesWithoutSelfContribution(
      DatasetMetadata datasetMetadata,
      Set<String> currentIds,
      List<LivePartitionState> livePartitionStates) {
    long currentPerPartitionThroughput = datasetMetadata.getActivePerPartitionThroughput();
    return livePartitionStates.stream()
        .map(
            partition -> {
              if (currentIds.contains(partition.getPartitionID())) {
                return new LivePartitionState(
                    partition.getPartitionID(),
                    Math.max(0, partition.getProvisionedCapacity() - currentPerPartitionThroughput),
                    partition.getMaxCapacity(),
                    partition.getOccupancy());
              }
              return partition;
            })
        .toList();
  }

  static boolean canUseForDedicatedAssignment(LivePartitionState partition, String datasetName) {
    return partition.isExclusivelyUsedBy(datasetName) || partition.isEmpty();
  }

  private static boolean supportsManualAssignment(
      LivePartitionState partition,
      String datasetName,
      boolean requireDedicatedPartition,
      long demandPerPartition) {
    return partition != null
        && partition.getAvailableCapacity() >= demandPerPartition
        && (requireDedicatedPartition
            ? canUseForDedicatedAssignment(partition, datasetName)
            : partition.canUseForSharedAssignment(datasetName));
  }
}
