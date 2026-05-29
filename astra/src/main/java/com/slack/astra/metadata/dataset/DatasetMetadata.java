package com.slack.astra.metadata.dataset;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.slack.astra.metadata.core.AstraMetadata;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Contains configurations for use in the pre-processor and query service - including rate limits,
 * and partition mapping.
 */
public class DatasetMetadata extends AstraMetadata {

  public static final String MATCH_ALL_SERVICE = "_all";
  public static final String MATCH_STAR_SERVICE = "*";

  public final String owner;
  public final String serviceNamePattern;
  public final long throughputBytes;
  public final ImmutableList<DatasetPartitionMetadata> partitionConfigs;
  public final boolean usingDedicatedPartitions;

  public DatasetMetadata(
      String name,
      String owner,
      long throughputBytes,
      List<DatasetPartitionMetadata> partitionConfigs,
      String serviceNamePattern) {
    this(name, owner, throughputBytes, partitionConfigs, serviceNamePattern, false);
  }

  public DatasetMetadata(
      String name,
      String owner,
      long throughputBytes,
      List<DatasetPartitionMetadata> partitionConfigs,
      String serviceNamePattern,
      boolean usingDedicatedPartitions) {
    super(name);
    checkArgument(name.length() <= 256, "name must be no longer than 256 chars");
    checkArgument(name.matches("^[a-zA-Z0-9_-]*$"), "name must contain only [a-zA-Z0-9_-]");
    checkArgument(partitionConfigs != null, "partitionConfigs must not be null");
    checkArgument(owner != null && !owner.isBlank(), "owner must not be null or blank");
    checkArgument(throughputBytes >= 0, "throughputBytes must be greater than or equal to 0");
    checkPartitions(partitionConfigs, "partitionConfigs must not overlap start and end times");

    // back compat - make this into a null check in the future?
    if (serviceNamePattern != null && !serviceNamePattern.isBlank()) {
      checkArgument(
          serviceNamePattern.length() <= 256,
          "serviceNamePattern must be no longer than 256 chars");
    }

    this.owner = owner;
    this.serviceNamePattern = serviceNamePattern;
    this.throughputBytes = throughputBytes;
    this.partitionConfigs = ImmutableList.copyOf(partitionConfigs);
    this.usingDedicatedPartitions = usingDedicatedPartitions;
  }

  public DatasetMetadata getDataset() {
    return this;
  }

  public String getOwner() {
    return owner;
  }

  public long getThroughputBytes() {
    return throughputBytes;
  }

  public ImmutableList<DatasetPartitionMetadata> getPartitionConfigs() {
    return partitionConfigs;
  }

  public String getServiceNamePattern() {
    return serviceNamePattern;
  }

  public boolean isUsingDedicatedPartitions() {
    return usingDedicatedPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DatasetMetadata that)) return false;
    if (!super.equals(o)) return false;
    return throughputBytes == that.throughputBytes
        && usingDedicatedPartitions == that.usingDedicatedPartitions
        && name.equals(that.name)
        && owner.equals(that.owner)
        && serviceNamePattern.equals(that.serviceNamePattern)
        && partitionConfigs.equals(that.partitionConfigs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        name,
        owner,
        serviceNamePattern,
        throughputBytes,
        partitionConfigs,
        usingDedicatedPartitions);
  }

  @Override
  public String toString() {
    return "DatasetMetadata{"
        + "name='"
        + name
        + '\''
        + ", owner='"
        + owner
        + '\''
        + ", serviceNamePattern='"
        + serviceNamePattern
        + '\''
        + ", throughputBytes="
        + throughputBytes
        + ", partitionConfigs="
        + partitionConfigs
        + ", usingDedicatedPartitions="
        + usingDedicatedPartitions
        + '}';
  }

  /**
   * Check that the list of partitionConfigs do not overlap start and end times. This sorts the list
   * by start of partitions by start time, and then ensures that the end of a given item does not
   * overlap with the start of the next item in the list.
   */
  private void checkPartitions(
      List<DatasetPartitionMetadata> partitionConfig, String errorMessage) {
    List<DatasetPartitionMetadata> sortedConfigsByStartTime =
        partitionConfig.stream()
            .sorted(Comparator.comparingLong(DatasetPartitionMetadata::getStartTimeEpochMs))
            .collect(Collectors.toList());

    for (int i = 0; i < sortedConfigsByStartTime.size(); i++) {
      if (i + 1 != sortedConfigsByStartTime.size()) {
        checkArgument(
            sortedConfigsByStartTime.get(i).endTimeEpochMs
                < sortedConfigsByStartTime.get(i + 1).startTimeEpochMs,
            errorMessage);
      }
    }
  }

  public Optional<DatasetPartitionMetadata> getActivePartitionMetadata() {
    return getPartitionConfigs().stream().filter(DatasetPartitionMetadata::isActive).findFirst();
  }

  public ImmutableList<String> getActivePartitionIds() {
    return getActivePartitionMetadata()
        .map(DatasetPartitionMetadata::getPartitions)
        .orElseGet(ImmutableList::of);
  }

  public List<DatasetPartitionMetadata> getInactivePartitionMetadata() {
    return getPartitionConfigs().stream()
        .filter(datasetPartitionMetadata -> !datasetPartitionMetadata.isActive())
        .toList();
  }

  public long getActivePerPartitionThroughput() {
    int partitionCount = getActivePartitionIds().size();
    if (partitionCount == 0) {
      return 0;
    }
    return Math.ceilDiv(throughputBytes, partitionCount);
  }
}
