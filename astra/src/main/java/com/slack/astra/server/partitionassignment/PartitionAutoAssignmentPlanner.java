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

/**
 * Chooses partitions for auto-assignment from the current live partition state.
 *
 * <p>This class is auto-assignment only. It does not validate explicit user-selected partitions or
 * persist metadata updates.
 */
public final class PartitionAutoAssignmentPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAutoAssignmentPlanner.class);

  private static final String DEDICATED_BRANCH_LABEL = "dedicated proposal";
  private static final String SHARED_BRANCH_LABEL = "proposal";

  private PartitionAutoAssignmentPlanner() {}

  public static ImmutableList<String> planAutoAssignment(
      DatasetMetadata datasetMetadata,
      long throughputBytes,
      boolean requireDedicatedPartition,
      List<LivePartitionState> livePartitionStates,
      long minNumberOfPartitions) {
    Set<String> currentIds = ImmutableSet.copyOf(datasetMetadata.getActivePartitionIds());
    List<LivePartitionState> statesWithoutSelf =
        PartitionAssignmentConstraints.liveStatesWithoutSelfContribution(
            datasetMetadata, livePartitionStates);
    validateCandidatePartitionIds(datasetMetadata.getName(), statesWithoutSelf);

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
        sortedCandidates, throughputBytes, minNumberOfPartitions, branchLabel);
  }

  private static List<LivePartitionState> dedicatedCandidates(
      DatasetMetadata datasetMetadata, List<LivePartitionState> statesWithoutSelf) {
    Comparator<LivePartitionState> byAvailableCapacityThenId =
        Comparator.comparing(LivePartitionState::getAvailableCapacity)
            .thenComparing(
                LivePartitionState::partitionId, PartitionIdOrdering.numericComparator());
    List<LivePartitionState> reusable =
        statesWithoutSelf.stream()
            .filter(partition -> partition.isExclusivelyUsedBy(datasetMetadata.getName()))
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
        empty.stream().map(LivePartitionState::partitionId).toList(),
        sortedCandidates.stream().map(LivePartitionState::partitionId).toList());
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
                        LivePartitionState::partitionId, PartitionIdOrdering.numericComparator()))
            .toList();
    LOG.debug(
        "partitions sorted: {}",
        sortedCandidates.stream().map(LivePartitionState::partitionId).toList());
    return sortedCandidates;
  }

  private static Comparator<LivePartitionState> preferCurrentAssignment(Set<String> currentIds) {
    return Comparator.comparing(
            (LivePartitionState partition) -> currentIds.contains(partition.partitionId()))
        .reversed();
  }

  private static void validateCandidatePartitionIds(
      String datasetName, List<LivePartitionState> livePartitionStates) {
    for (LivePartitionState livePartitionState : livePartitionStates) {
      try {
        PartitionIdOrdering.parseNumericPartitionId(livePartitionState.partitionId());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Dataset %s cannot use invalid partition ID: %s"
                .formatted(datasetName, livePartitionState.partitionId()),
            e);
      }
    }
  }

  private static ImmutableList<String> findSmallestSatisfyingAssignment(
      List<LivePartitionState> sortedCandidates,
      long throughputBytes,
      long minPartitionCount,
      String branchLabel) {
    if (sortedCandidates.size() < minPartitionCount) {
      List<String> candidateIds =
          sortedCandidates.stream().map(LivePartitionState::partitionId).toList();
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "%s not enough candidate partitions: needed %d, available %d: %s"
                  .formatted(branchLabel, minPartitionCount, sortedCandidates.size(), candidateIds))
          .asRuntimeException();
    }

    ImmutableList<String> bestAttempt = ImmutableList.of();
    for (long targetPartitionCount = minPartitionCount;
        targetPartitionCount <= sortedCandidates.size();
        targetPartitionCount++) {
      final long demandPerPartition =
          PartitionAssignmentConstraints.perPartitionDemand(throughputBytes, targetPartitionCount);
      final long targetCount = targetPartitionCount;
      bestAttempt =
          sortedCandidates.stream()
              .filter(p -> p.getAvailableCapacity() >= demandPerPartition)
              .limit(targetCount)
              .map(LivePartitionState::partitionId)
              .collect(ImmutableList.toImmutableList());
      LOG.debug(
          "{} for partition count: {}, per partition throughput: {}, proposal: {}",
          branchLabel,
          targetPartitionCount,
          demandPerPartition,
          bestAttempt);
      if (bestAttempt.size() == targetPartitionCount) {
        return bestAttempt;
      }
    }
    throw Status.FAILED_PRECONDITION
        .withDescription(
            "Needed %d partitions with enough capacity, found %d: %s"
                .formatted(minPartitionCount, bestAttempt.size(), bestAttempt))
        .asRuntimeException();
  }
}
