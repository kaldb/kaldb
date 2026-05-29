package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class LivePartitionStateTest {
  @Test
  public void shouldProjectPartitionMetadataIntoEmptyLivePartitionStates() {
    assertThat(
            LivePartitionState.fromMetadata(
                List.of(),
                List.of(new PartitionMetadata("2", 100), new PartitionMetadata("10", 100))))
        .containsExactlyInAnyOrder(
            new LivePartitionState("2", 0, 100, new PartitionOccupancy.Empty()),
            new LivePartitionState("10", 0, 100, new PartitionOccupancy.Empty()));
  }

  @Test
  public void shouldCalculateProvisioningAndOccupancyFromDatasetAssignments() {
    DatasetMetadata sharedDataset =
        new DatasetMetadata(
            "shared-dataset",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1", "2"))),
            "",
            false);
    DatasetMetadata dedicatedDataset =
        new DatasetMetadata(
            "dedicated-dataset",
            "owner",
            75,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("3"))),
            "",
            true);

    assertThat(
            LivePartitionState.fromMetadata(
                List.of(sharedDataset, dedicatedDataset),
                List.of(
                    new PartitionMetadata("1", 100),
                    new PartitionMetadata("2", 100),
                    new PartitionMetadata("3", 100))))
        .containsExactly(
            new LivePartitionState(
                "1", 50, 100, new PartitionOccupancy.Shared(List.of("shared-dataset"))),
            new LivePartitionState(
                "2", 50, 100, new PartitionOccupancy.Shared(List.of("shared-dataset"))),
            new LivePartitionState(
                "3", 75, 100, new PartitionOccupancy.Dedicated("dedicated-dataset")));
  }

  @Test
  public void shouldIgnoreDatasetsWithoutActiveAssignments() {
    DatasetMetadata historicalOnlyDataset =
        new DatasetMetadata(
            "historical-only",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, 2, List.of("1"))),
            "",
            false);

    assertThat(
            LivePartitionState.fromMetadata(
                List.of(historicalOnlyDataset), List.of(new PartitionMetadata("1", 100))))
        .containsExactly(new LivePartitionState("1", 0, 100, new PartitionOccupancy.Empty()));
  }

  @Test
  public void shouldSkipAssignmentsThatReferenceMissingPartitionCatalogEntries() {
    DatasetMetadata datasetWithMissingPartitionReference =
        new DatasetMetadata(
            "missing-partition-dataset",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("9"))),
            "",
            false);

    assertThat(
            LivePartitionState.fromMetadata(
                List.of(datasetWithMissingPartitionReference),
                List.of(new PartitionMetadata("1", 100))))
        .containsExactly(new LivePartitionState("1", 0, 100, new PartitionOccupancy.Empty()));
  }

  @Test
  public void shouldProjectMultipleSharedDatasetsIntoSharedOccupancy() {
    DatasetMetadata firstSharedDataset =
        new DatasetMetadata(
            "shared-a",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            "",
            false);
    DatasetMetadata secondSharedDataset =
        new DatasetMetadata(
            "shared-b",
            "owner",
            50,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            "",
            false);

    assertThat(
            LivePartitionState.fromMetadata(
                List.of(firstSharedDataset, secondSharedDataset),
                List.of(new PartitionMetadata("1", 200))))
        .containsExactly(
            new LivePartitionState(
                "1", 150, 200, new PartitionOccupancy.Shared(List.of("shared-a", "shared-b"))));
  }

  @Test
  public void shouldRejectPartitionsUsedByDedicatedAndSharedDatasetsAtTheSameTime() {
    DatasetMetadata dedicatedDataset =
        new DatasetMetadata(
            "dedicated-dataset",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            "",
            true);
    DatasetMetadata sharedDataset =
        new DatasetMetadata(
            "shared-dataset",
            "owner",
            50,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1"))),
            "",
            false);

    assertThatThrownBy(
            () ->
                LivePartitionState.fromMetadata(
                    List.of(dedicatedDataset, sharedDataset),
                    List.of(new PartitionMetadata("1", 200))))
        .isInstanceOf(InvalidPartitionAssignmentStateException.class)
        .hasMessage(
            "partition 1 has inconsistent occupancy: dedicated owner dedicated-dataset but datasets [dedicated-dataset, shared-dataset]");
  }
}
