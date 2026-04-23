package com.slack.astra.server.partitionassignment;

import com.google.common.collect.ImmutableMap;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import io.grpc.Status;
import java.util.List;
import java.util.Map;

/**
 * Validates explicit partition selections supplied by manual assignment requests.
 *
 * <p>This class is manual-assignment only. It answers "can the user-requested partition list be
 * applied right now?" but does not choose partitions or persist metadata.
 */
public final class ManualPartitionAssignmentValidator {
  private ManualPartitionAssignmentValidator() {}

  public static void validateManualSelection(
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

    Map<String, LivePartitionState> statesById =
        PartitionAssignmentConstraints.liveStatesWithoutSelfContribution(
                datasetMetadata, livePartitionStates)
            .stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    LivePartitionState::partitionId, partitionState -> partitionState));
    long demandPerPartition =
        PartitionAssignmentConstraints.perPartitionDemand(
            throughputBytes, requestedPartitionIds.size());

    List<String> invalidPartitionIds =
        requestedPartitionIds.stream()
            .filter(
                partitionId ->
                    !supportsManualSelection(
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

  private static boolean supportsManualSelection(
      LivePartitionState partition,
      String datasetName,
      boolean requireDedicatedPartition,
      long demandPerPartition) {
    return partition != null
        && partition.getAvailableCapacity() >= demandPerPartition
        && (requireDedicatedPartition
            ? PartitionAssignmentConstraints.canUseForDedicatedAssignment(partition, datasetName)
            : partition.canUseForSharedAssignment(datasetName));
  }
}
