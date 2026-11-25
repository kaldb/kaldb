package com.slack.astra.server.partitionassignment;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableSet;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import java.util.List;
import java.util.Set;

/**
 * Shared feasibility helpers used by both manual validation and auto-assignment planning.
 *
 * <p>This class contains the low-level calculations and predicates that both paths depend on. It
 * does not validate a full manual request, choose partitions, or persist anything.
 */
public final class PartitionAssignmentConstraints {
  private PartitionAssignmentConstraints() {}

  /** Returns the even-share per-partition throughput for a positive partition count. */
  static long perPartitionDemand(long totalThroughput, long partitionCount) {
    checkArgument(partitionCount > 0, "partitionCount must be positive, got %s", partitionCount);
    return Math.ceilDiv(totalThroughput, partitionCount);
  }

  static List<LivePartitionState> liveStatesWithoutSelfContribution(
      DatasetMetadata datasetMetadata, List<LivePartitionState> livePartitionStates) {
    Set<String> currentIds = ImmutableSet.copyOf(datasetMetadata.getActivePartitionIds());
    long currentPerPartitionThroughput = datasetMetadata.getActivePerPartitionThroughput();
    return livePartitionStates.stream()
        .map(
            partition -> {
              if (currentIds.contains(partition.partitionId())) {
                return new LivePartitionState(
                    partition.partitionId(),
                    Math.max(0, partition.provisionedCapacity() - currentPerPartitionThroughput),
                    partition.maxCapacity(),
                    partition.occupancy());
              }
              return partition;
            })
        .toList();
  }

  static boolean canUseForDedicatedAssignment(LivePartitionState partition, String datasetName) {
    return partition.isExclusivelyUsedBy(datasetName) || partition.isEmpty();
  }
}
