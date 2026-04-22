package com.slack.astra.server.partitionassignment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import io.grpc.Status;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assignment policy for selecting partitions from the current live partition state. */
public final class PartitionAutoAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAutoAssigner.class);

  private static final String DEDICATED_BRANCH_LABEL = "dedicated proposal";
  private static final String SHARED_BRANCH_LABEL = "proposal";

  private PartitionAutoAssigner() {}

  public static ImmutableList<String> autoAssign(
      DatasetMetadata datasetMetadata,
      long throughputBytes,
      boolean requireDedicatedPartition,
      List<LivePartitionState> livePartitionStates,
      long minNumberOfPartitions) {
    if (livePartitionStates.isEmpty()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed %d partitions with enough capacity, found 0".formatted(minNumberOfPartitions))
          .asRuntimeException();
    }

    Set<String> currentIds = ImmutableSet.copyOf(datasetMetadata.getActivePartitionIds());
    List<LivePartitionState> statesWithoutSelf =
        withSelfContributionRemoved(datasetMetadata, currentIds, livePartitionStates);

    List<LivePartitionState> sortedCandidates;
    String branchLabel;
    if (requireDedicatedPartition) {
      sortedCandidates = dedicatedCandidates(datasetMetadata, statesWithoutSelf);
      branchLabel = DEDICATED_BRANCH_LABEL;
    } else {
      sortedCandidates = sharedCandidates(datasetMetadata, currentIds, statesWithoutSelf);
      branchLabel = SHARED_BRANCH_LABEL;
    }

    return findSmallestSatisfyingAssignment(
        sortedCandidates,
        throughputBytes,
        minNumberOfPartitions,
        minNumberOfPartitions,
        branchLabel);
  }

  private static long perPartitionDemand(long totalThroughput, long partitionCount) {
    return Math.ceilDiv(totalThroughput, partitionCount);
  }

  private static List<LivePartitionState> dedicatedCandidates(
      DatasetMetadata datasetMetadata, List<LivePartitionState> statesWithoutSelf) {
    Comparator<LivePartitionState> byAvailableCapacityThenId =
        Comparator.comparing(LivePartitionState::getAvailableCapacity)
            .thenComparing(
                LivePartitionState::getPartitionID, PartitionIdOrdering.numericComparator());
    List<LivePartitionState> reusable =
        statesWithoutSelf.stream()
            .filter(p -> p.isExclusivelyUsedBy(datasetMetadata.getName()))
            .sorted(byAvailableCapacityThenId)
            .toList();
    List<LivePartitionState> empty =
        statesWithoutSelf.stream()
            .filter(LivePartitionState::isEmpty)
            .sorted(byAvailableCapacityThenId)
            .toList();
    List<LivePartitionState> sortedCandidates =
        Stream.concat(reusable.stream(), empty.stream()).toList();
    LOG.debug(
        "current empty partitions: {}, list to pull from {}",
        empty.stream().map(LivePartitionState::getPartitionID).toList(),
        sortedCandidates.stream().map(LivePartitionState::getPartitionID).toList());
    return sortedCandidates;
  }

  private static List<LivePartitionState> sharedCandidates(
      DatasetMetadata datasetMetadata,
      Set<String> currentIds,
      List<LivePartitionState> statesWithoutSelf) {
    List<LivePartitionState> sortedCandidates =
        statesWithoutSelf.stream()
            .filter(partition -> partition.canUseForSharedAssignment(datasetMetadata.getName()))
            .sorted(
                preferCurrentAssignment(currentIds)
                    .thenComparing(LivePartitionState::getAvailableCapacity)
                    .thenComparing(
                        LivePartitionState::getPartitionID,
                        PartitionIdOrdering.numericComparator()))
            .toList();
    LOG.debug(
        "partitions sorted: {}",
        sortedCandidates.stream().map(LivePartitionState::getPartitionID).toList());
    return sortedCandidates;
  }

  private static Comparator<LivePartitionState> preferCurrentAssignment(Set<String> currentIds) {
    return Comparator.comparing(
            (LivePartitionState partition) -> currentIds.contains(partition.getPartitionID()))
        .reversed();
  }

  private static ImmutableList<String> findSmallestSatisfyingAssignment(
      List<LivePartitionState> sortedCandidates,
      long throughputBytes,
      long startCount,
      long minAcceptable,
      String branchLabel) {
    ImmutableList<String> bestAttempt = ImmutableList.of();
    for (long targetPartitionCount = startCount;
        targetPartitionCount <= sortedCandidates.size();
        targetPartitionCount++) {
      final long demandPerPartition = perPartitionDemand(throughputBytes, targetPartitionCount);
      final long targetCount = targetPartitionCount;
      bestAttempt =
          sortedCandidates.stream()
              .filter(p -> p.getAvailableCapacity() >= demandPerPartition)
              .limit(targetCount)
              .map(LivePartitionState::getPartitionID)
              .collect(ImmutableList.toImmutableList());
      LOG.debug(
          "{} for partition count: {}, per partition throughput: {}, proposal: {}",
          branchLabel,
          targetPartitionCount,
          demandPerPartition,
          bestAttempt);
      if (bestAttempt.size() == targetPartitionCount && targetPartitionCount >= minAcceptable) {
        return bestAttempt;
      }
    }
    throw Status.FAILED_PRECONDITION
        .withDescription(
            "Needed %d partitions with enough capacity, found %d: %s"
                .formatted(minAcceptable, bestAttempt.size(), bestAttempt))
        .asRuntimeException();
  }

  private static List<LivePartitionState> withSelfContributionRemoved(
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
}
