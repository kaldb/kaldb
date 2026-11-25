package com.slack.astra.server.partitionassignment;

import java.util.List;
import java.util.Objects;

/** Describes what kind of assignment currently occupies a partition. */
public sealed interface PartitionOccupancy
    permits PartitionOccupancy.Empty, PartitionOccupancy.Shared, PartitionOccupancy.Dedicated {

  List<String> datasets();

  boolean isEmpty();

  boolean canUseForSharedAssignment(String datasetName);

  boolean isDedicatedOnlyTo(String datasetName);

  boolean isExclusivelyUsedBy(String datasetName);

  record Empty() implements PartitionOccupancy {
    @Override
    public List<String> datasets() {
      return List.of();
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean canUseForSharedAssignment(String datasetName) {
      return true;
    }

    @Override
    public boolean isDedicatedOnlyTo(String datasetName) {
      return false;
    }

    @Override
    public boolean isExclusivelyUsedBy(String datasetName) {
      return false;
    }
  }

  record Shared(List<String> datasets) implements PartitionOccupancy {
    public Shared {
      datasets = List.copyOf(datasets);
      if (datasets.isEmpty()) {
        throw new IllegalArgumentException("shared partition occupancy requires datasets");
      }
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean canUseForSharedAssignment(String datasetName) {
      return true;
    }

    @Override
    public boolean isDedicatedOnlyTo(String datasetName) {
      return false;
    }

    @Override
    public boolean isExclusivelyUsedBy(String datasetName) {
      return datasets.size() == 1 && datasets.get(0).equals(datasetName);
    }
  }

  record Dedicated(String dataset) implements PartitionOccupancy {
    public Dedicated {
      Objects.requireNonNull(dataset, "dataset");
      if (dataset.isBlank()) {
        throw new IllegalArgumentException("dedicated partition occupancy requires a dataset");
      }
    }

    @Override
    public List<String> datasets() {
      return List.of(dataset);
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean canUseForSharedAssignment(String datasetName) {
      return dataset.equals(datasetName);
    }

    @Override
    public boolean isDedicatedOnlyTo(String datasetName) {
      return dataset.equals(datasetName);
    }

    @Override
    public boolean isExclusivelyUsedBy(String datasetName) {
      return dataset.equals(datasetName);
    }
  }
}
