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
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Application workflow for reading and updating dataset partition assignments. */
public class PartitionAssignmentService {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionAssignmentService.class);
  public static final long MAX_TIME = Long.MAX_VALUE;

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
    Preconditions.checkArgument(
        requestedPartitionIds.stream().noneMatch(String::isBlank),
        "PartitionIds list must not contain blank strings");
    Preconditions.checkArgument(!datasetName.isBlank(), "Dataset name must not be blank");

    DatasetMetadata existingDatasetMetadata = getDatasetOrThrow(datasetName);
    long updatedThroughputBytes =
        requestedThroughputBytes < 0
            ? existingDatasetMetadata.getThroughputBytes()
            : requestedThroughputBytes;
    boolean requireDedicatedPartition =
        switch (
            Objects.requireNonNull(
                dedicatedPartitionModeOverride, "dedicatedPartitionModeOverride")) {
          case PRESERVE_EXISTING -> existingDatasetMetadata.isUsingDedicatedPartitions();
          case REQUIRE_DEDICATED -> true;
          case REQUIRE_SHARED -> false;
        };

    List<String> assignedPartitionIds;
    if (requestedPartitionIds.isEmpty()) {
      assignedPartitionIds =
          PartitionAutoAssigner.autoAssign(
              existingDatasetMetadata,
              updatedThroughputBytes,
              requireDedicatedPartition,
              listLivePartitionStates(),
              minNumberOfPartitions);
      LOG.info("Auto-assigning partitions for {} to : {}", datasetName, assignedPartitionIds);
    } else {
      assignedPartitionIds = requestedPartitionIds;
      LOG.info("Manually assigning partitions for {} to : {}", datasetName, assignedPartitionIds);
      validateManualPartitionIds(assignedPartitionIds);
    }

    List<String> persistedPartitionIds = List.copyOf(assignedPartitionIds);
    ImmutableList<DatasetPartitionMetadata> updatedDatasetPartitionMetadata =
        withActivePartitionAssignment(existingDatasetMetadata, persistedPartitionIds);

    DatasetMetadata updatedDatasetMetadata =
        new DatasetMetadata(
            existingDatasetMetadata.getName(),
            existingDatasetMetadata.getOwner(),
            updatedThroughputBytes,
            updatedDatasetPartitionMetadata,
            existingDatasetMetadata.getServiceNamePattern(),
            requireDedicatedPartition);
    datasetMetadataStore.updateSync(updatedDatasetMetadata);

    LOG.info(
        "Updated partition assignment for dataset: {}, throughput: {} -> {} partitions: {} -> {}",
        datasetName,
        existingDatasetMetadata.getThroughputBytes(),
        updatedThroughputBytes,
        existingDatasetMetadata.getActivePartitionMetadata(),
        persistedPartitionIds);

    return ImmutableList.copyOf(persistedPartitionIds);
  }

  private DatasetMetadata getDatasetOrThrow(String datasetName) {
    try {
      return datasetMetadataStore.getSync(datasetName);
    } catch (Exception e) {
      String msg = "No dataset named, '%s'. Please create it first.".formatted(datasetName);
      LOG.error(msg, e);
      throw Status.NOT_FOUND.withDescription(msg).asRuntimeException();
    }
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
   * Returns a new list of dataset partition metadata, with the provided partition IDs as the
   * current active assignment. This finds the current active assignment (end time of max long),
   * sets it to the current time, and then appends a new dataset partition assignment starting from
   * current time + 1 to max long.
   */
  private static ImmutableList<DatasetPartitionMetadata> withActivePartitionAssignment(
      DatasetMetadata datasetMetadata, List<String> newPartitionIdsList) {
    ImmutableList<DatasetPartitionMetadata> existingPartitions =
        datasetMetadata.getPartitionConfigs();
    if (newPartitionIdsList.isEmpty()) {
      return ImmutableList.copyOf(existingPartitions);
    }

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
        new DatasetPartitionMetadata(partitionCutoverTime + 1, MAX_TIME, newPartitionIdsList);
    return builder.add(newPartitionMetadata).build();
  }
}
