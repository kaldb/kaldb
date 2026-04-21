package com.slack.astra.server.partitionassignment;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Application workflow for reading and updating dataset partition assignments. */
public class PartitionAssignmentService {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAssignmentService.class);

  /** End-time sentinel marking the still-active (open-ended) assignment window. */
  public static final long OPEN_ENDED_END_TIME = Long.MAX_VALUE;

  /** Tri-state override for preserving or explicitly changing dedicated partition mode. */
  public enum DedicatedPartitionModeOverride {
    PRESERVE_EXISTING,
    REQUIRE_DEDICATED,
    REQUIRE_SHARED
  }

  private final DatasetMetadataStore datasetMetadataStore;
  private final PartitionMetadataStore partitionMetadataStore;
  private final int minNumberOfPartitions;

  public PartitionAssignmentService(
      DatasetMetadataStore datasetMetadataStore,
      PartitionMetadataStore partitionMetadataStore,
      int minNumberOfPartitions) {
    Preconditions.checkArgument(
        minNumberOfPartitions > 0, "minNumberOfPartitions must be greater than 0");
    this.datasetMetadataStore = datasetMetadataStore;
    this.partitionMetadataStore = partitionMetadataStore;
    this.minNumberOfPartitions = minNumberOfPartitions;
  }

  public List<LivePartitionState> listLivePartitionStates() {
    return LivePartitionState.fromMetadata(
        datasetMetadataStore.listSync(), partitionMetadataStore.listSync());
  }

  public ImmutableList<String> updateAssignment(
      String datasetName,
      long requestedThroughputBytes,
      List<String> requestedPartitionIds,
      DedicatedPartitionModeOverride dedicatedPartitionModeOverride) {
    validateUpdateInputs(datasetName, requestedPartitionIds);

    DatasetMetadata existingDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    long effectiveThroughputBytes =
        resolveThroughput(existingDatasetMetadata, requestedThroughputBytes);
    boolean useDedicatedPartitions =
        resolveUsingDedicatedPartitions(existingDatasetMetadata, dedicatedPartitionModeOverride);

    List<String> assignedPartitionIds =
        assignPartitions(
            existingDatasetMetadata,
            effectiveThroughputBytes,
            useDedicatedPartitions,
            requestedPartitionIds);

    persistAssignment(
        existingDatasetMetadata,
        effectiveThroughputBytes,
        useDedicatedPartitions,
        assignedPartitionIds);

    return ImmutableList.copyOf(assignedPartitionIds);
  }

  private static void validateUpdateInputs(String datasetName, List<String> requestedPartitionIds) {
    Preconditions.checkArgument(!datasetName.isBlank(), "Dataset name must not be blank");
    Preconditions.checkArgument(
        requestedPartitionIds.stream().noneMatch(String::isBlank),
        "PartitionIds list must not contain blank strings");
  }

  private static long resolveThroughput(DatasetMetadata existing, long requestedThroughputBytes) {
    return requestedThroughputBytes < 0 ? existing.getThroughputBytes() : requestedThroughputBytes;
  }

  private static boolean resolveUsingDedicatedPartitions(
      DatasetMetadata existing, DedicatedPartitionModeOverride override) {
    return switch (Objects.requireNonNull(override, "dedicatedPartitionModeOverride")) {
      case PRESERVE_EXISTING -> existing.isUsingDedicatedPartitions();
      case REQUIRE_DEDICATED -> true;
      case REQUIRE_SHARED -> false;
    };
  }

  private List<String> assignPartitions(
      DatasetMetadata existingDatasetMetadata,
      long effectiveThroughputBytes,
      boolean useDedicatedPartitions,
      List<String> requestedPartitionIds) {
    if (requestedPartitionIds.isEmpty()) {
      List<String> autoAssigned =
          PartitionAutoAssigner.autoAssign(
              existingDatasetMetadata,
              effectiveThroughputBytes,
              useDedicatedPartitions,
              listLivePartitionStates(),
              minNumberOfPartitions);
      LOG.info(
          "Auto-assigning partitions for {} to : {}",
          existingDatasetMetadata.getName(),
          autoAssigned);
      return autoAssigned;
    }
    LOG.info(
        "Manually assigning partitions for {} to : {}",
        existingDatasetMetadata.getName(),
        requestedPartitionIds);
    validateManualPartitionIds(requestedPartitionIds);
    return requestedPartitionIds;
  }

  private void persistAssignment(
      DatasetMetadata existingDatasetMetadata,
      long effectiveThroughputBytes,
      boolean useDedicatedPartitions,
      List<String> assignedPartitionIds) {
    List<String> persistedPartitionIds = List.copyOf(assignedPartitionIds);
    ImmutableList<DatasetPartitionMetadata> updatedDatasetPartitionMetadata =
        rolloverActivePartitionAssignment(existingDatasetMetadata, persistedPartitionIds);

    DatasetMetadata updatedDatasetMetadata =
        new DatasetMetadata(
            existingDatasetMetadata.getName(),
            existingDatasetMetadata.getOwner(),
            effectiveThroughputBytes,
            updatedDatasetPartitionMetadata,
            existingDatasetMetadata.getServiceNamePattern(),
            useDedicatedPartitions);
    datasetMetadataStore.updateSync(updatedDatasetMetadata);

    LOG.info(
        "Updated partition assignment for dataset: {}, throughput: {} -> {} partitions: {} -> {}",
        existingDatasetMetadata.getName(),
        existingDatasetMetadata.getThroughputBytes(),
        effectiveThroughputBytes,
        existingDatasetMetadata.getActivePartitionMetadata(),
        persistedPartitionIds);
  }

  private void validateManualPartitionIds(List<String> requestedPartitionIds) {
    Set<String> configuredPartitionIds =
        partitionMetadataStore.listSync().stream()
            .map(PartitionMetadata::getPartitionID)
            .collect(ImmutableSet.toImmutableSet());
    List<String> nonExistentRequestedPartitionIds =
        requestedPartitionIds.stream()
            .filter(id -> !configuredPartitionIds.contains(id))
            .sorted()
            .toList();
    Preconditions.checkArgument(
        nonExistentRequestedPartitionIds.isEmpty(),
        "Requested partition IDs do not exist: %s".formatted(nonExistentRequestedPartitionIds));
  }

  /**
   * Closes the current active assignment window at now and opens a new one at now + 1 with the
   * provided partition IDs. Inactive (already-closed) windows are preserved unchanged.
   */
  private static ImmutableList<DatasetPartitionMetadata> rolloverActivePartitionAssignment(
      DatasetMetadata datasetMetadata, List<String> newPartitionIdsList) {
    ImmutableList<DatasetPartitionMetadata> existingPartitions =
        datasetMetadata.getPartitionConfigs();

    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
        datasetMetadata.getActivePartitionMetadata();
    List<DatasetPartitionMetadata> remainingDatasetPartitions =
        datasetMetadata.getInactivePartitionMetadata();

    if (previousActiveDatasetPartition.isPresent()
        && previousActiveDatasetPartition.get().getPartitions().equals(newPartitionIdsList)) {
      return ImmutableList.copyOf(existingPartitions);
    }

    // TODO: Consider adding some padding to this cutover time; this may complicate validation
    // because you would need to consider what happens when there's a future cutover already
    // scheduled.
    // TODO: If introducing optional padding, it should likely be added as a method parameter to
    // the cutover-time calculation.
    long partitionCutoverTime = Instant.now().toEpochMilli();

    ImmutableList.Builder<DatasetPartitionMetadata> builder =
        ImmutableList.<DatasetPartitionMetadata>builder().addAll(remainingDatasetPartitions);

    if (previousActiveDatasetPartition.isPresent()) {
      DatasetPartitionMetadata updatedPreviousActivePartition =
          new DatasetPartitionMetadata(
              previousActiveDatasetPartition.get().getStartTimeEpochMs(),
              partitionCutoverTime,
              previousActiveDatasetPartition.get().getPartitions());
      builder.add(updatedPreviousActivePartition);
    }

    DatasetPartitionMetadata newPartitionMetadata =
        new DatasetPartitionMetadata(
            partitionCutoverTime + 1, OPEN_ENDED_END_TIME, newPartitionIdsList);
    return builder.add(newPartitionMetadata).build();
  }
}
