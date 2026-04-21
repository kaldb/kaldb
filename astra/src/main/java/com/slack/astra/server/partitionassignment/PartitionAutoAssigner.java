package com.slack.astra.server.partitionassignment;

import com.google.common.collect.ImmutableList;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import io.grpc.Status;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assignment policy for selecting partitions from the current live partition state. */
public final class PartitionAutoAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAutoAssigner.class);

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

    List<String> currentPartitions =
        datasetMetadata
            .getActivePartitionMetadata()
            .map(DatasetPartitionMetadata::getPartitions)
            .orElseGet(ImmutableList::of);

    List<LivePartitionState> adjustedLivePartitionStates =
        withoutDatasetContribution(datasetMetadata, livePartitionStates);
    if (requireDedicatedPartition) {
      List<LivePartitionState> reusablePartitions =
          adjustedLivePartitionStates.stream()
              .filter(p -> p.isDedicatedOnlyTo(datasetMetadata.getName()))
              .toList();
      Comparator<LivePartitionState> compareByAvailableCapacityThenId =
          Comparator.comparing(LivePartitionState::getAvailableCapacity)
              .thenComparing(LivePartitionState::getPartitionID);
      List<LivePartitionState> emptyPartitions =
          livePartitionStates.stream().filter(LivePartitionState::isEmpty).toList();
      List<LivePartitionState> sortedPartitions =
          Stream.concat(
                  reusablePartitions.stream().sorted(compareByAvailableCapacityThenId),
                  emptyPartitions.stream().sorted(compareByAvailableCapacityThenId))
              .toList();
      LOG.debug(
          "current empty partitions: {}, list to pull from {}",
          adjustedLivePartitionStates.stream()
              .filter(LivePartitionState::isEmpty)
              .map(LivePartitionState::getPartitionID)
              .toList(),
          sortedPartitions.stream().map(LivePartitionState::getPartitionID).toList());

      ImmutableList<String> lastProposal = ImmutableList.of();
      for (long proposedPartitionCt = minNumberOfPartitions;
          proposedPartitionCt <= sortedPartitions.size();
          proposedPartitionCt++) {
        long nextPerPartitionThroughput = Math.ceilDiv(throughputBytes, proposedPartitionCt);
        lastProposal =
            sortedPartitions.stream()
                .filter(p -> p.getAvailableCapacity() >= nextPerPartitionThroughput)
                .limit(proposedPartitionCt)
                .map(LivePartitionState::getPartitionID)
                .collect(ImmutableList.toImmutableList());
        LOG.debug(
            "dedicated proposal for partition count: {}, per partition throughput: {}, proposal: {}",
            proposedPartitionCt,
            nextPerPartitionThroughput,
            lastProposal);
        if (lastProposal.size() == proposedPartitionCt) {
          return lastProposal;
        }
      }

      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed %d partitions with enough capacity, found %d: %s"
                  .formatted(minNumberOfPartitions, lastProposal.size(), lastProposal))
          .asRuntimeException();
    }

    List<LivePartitionState> partitionsSorted =
        adjustedLivePartitionStates.stream()
            .filter(partition -> partition.canUseForSharedAssignment(datasetMetadata.getName()))
            .sorted(
                Comparator.comparing(
                        (LivePartitionState partition) ->
                            currentPartitions.contains(partition.getPartitionID()))
                    .reversed()
                    .thenComparing(LivePartitionState::getAvailableCapacity)
                    .thenComparing(LivePartitionState::getPartitionID))
            .toList();
    LOG.debug(
        "partitions sorted: {}",
        partitionsSorted.stream().map(LivePartitionState::getPartitionID).toList());

    ImmutableList<String> lastProposal = ImmutableList.of();
    for (long proposedPartitionCt = 1;
        proposedPartitionCt <= partitionsSorted.size();
        proposedPartitionCt++) {
      long nextPerPartitionThroughput = Math.ceilDiv(throughputBytes, proposedPartitionCt);
      lastProposal =
          partitionsSorted.stream()
              .filter(p -> p.getAvailableCapacity() >= nextPerPartitionThroughput)
              .limit(proposedPartitionCt)
              .map(LivePartitionState::getPartitionID)
              .collect(ImmutableList.toImmutableList());
      LOG.debug(
          "proposal for partition count: {}, per partition throughput: {}, proposal: {}",
          proposedPartitionCt,
          nextPerPartitionThroughput,
          lastProposal);
      if (lastProposal.size() == proposedPartitionCt
          && proposedPartitionCt >= minNumberOfPartitions) {
        return lastProposal;
      }
    }
    throw Status.FAILED_PRECONDITION
        .withDescription(
            "Needed %d partitions with enough capacity, found %d: %s"
                .formatted(minNumberOfPartitions, lastProposal.size(), lastProposal))
        .asRuntimeException();
  }

  private static List<LivePartitionState> withoutDatasetContribution(
      DatasetMetadata datasetMetadata, List<LivePartitionState> livePartitionStates) {
    long currentPerPartitionThroughput = datasetMetadata.getActivePerPartitionThroughput();
    ImmutableList<String> currentIds =
        datasetMetadata
            .getActivePartitionMetadata()
            .map(DatasetPartitionMetadata::getPartitions)
            .orElse(ImmutableList.of());

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
