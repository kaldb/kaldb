package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class LivePartitionStateTest {
  @Test
  public void shouldPreservePartitionMetadataInputOrder() {
    assertThat(
            LivePartitionState.fromMetadata(
                List.of(),
                List.of(new PartitionMetadata("2", 100), new PartitionMetadata("10", 100))))
        .containsExactly(
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
}
