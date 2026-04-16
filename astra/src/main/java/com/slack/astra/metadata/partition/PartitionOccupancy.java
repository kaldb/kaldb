package com.slack.astra.metadata.partition;

import java.util.List;
import java.util.Objects;

/** Describes what kind of assignment currently occupies a partition. */
public sealed interface PartitionOccupancy
    permits PartitionOccupancy.Empty, PartitionOccupancy.Shared, PartitionOccupancy.Dedicated {

  static PartitionOccupancy from(List<String> datasets, List<String> dedicatedDatasets) {
    List<String> datasetCopy = List.copyOf(datasets);
    List<String> dedicatedDatasetCopy = List.copyOf(dedicatedDatasets);
    if (datasetCopy.isEmpty() && dedicatedDatasetCopy.isEmpty()) {
      return new Empty();
    }
    if (dedicatedDatasetCopy.isEmpty()) {
      return new Shared(datasetCopy);
    }
    if (datasetCopy.size() == 1
        && dedicatedDatasetCopy.size() == 1
        && datasetCopy.get(0).equals(dedicatedDatasetCopy.get(0))) {
      return new Dedicated(datasetCopy.get(0));
    }
    throw new IllegalArgumentException(
        "partition occupancy must be empty, shared, or dedicated to exactly one dataset");
  }

  List<String> datasets();

  default boolean isEmpty() {
    return this instanceof Empty;
  }

  default boolean canUseForSharedAssignment(String datasetName) {
    return !(this instanceof Dedicated dedicated) || dedicated.dataset().equals(datasetName);
  }

  default boolean isDedicatedOnlyTo(String datasetName) {
    return this instanceof Dedicated dedicated && dedicated.dataset().equals(datasetName);
  }

  record Empty() implements PartitionOccupancy {
    @Override
    public List<String> datasets() {
      return List.of();
    }
  }

  record Shared(List<String> datasets) implements PartitionOccupancy {
    public Shared {
      datasets = List.copyOf(datasets);
      if (datasets.isEmpty()) {
        throw new IllegalArgumentException("shared partition occupancy requires datasets");
      }
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
  }
}
