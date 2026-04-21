package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PartitionAutoAssignerTest {
  @Test
  public void shouldChooseMinimumSharedPartitionsWithEnoughCapacity() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata("shared-dataset", "owner", 0, List.of(), "", false);

    assertThat(
            PartitionAutoAssigner.autoAssign(
                datasetMetadata,
                100,
                false,
                List.of(
                    new LivePartitionState("2", 0, 60, new PartitionOccupancy.Empty()),
                    new LivePartitionState("1", 0, 60, new PartitionOccupancy.Empty()),
                    new LivePartitionState("3", 0, 40, new PartitionOccupancy.Empty())),
                2))
        .containsExactly("1", "2");
  }

  @Test
  public void shouldReuseDedicatedPartitionsBeforeTakingEmptyOnes() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            "dedicated-dataset",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, Long.MAX_VALUE, List.of("1", "2"))),
            "",
            true);

    assertThat(
            PartitionAutoAssigner.autoAssign(
                datasetMetadata,
                100,
                true,
                List.of(
                    new LivePartitionState(
                        "1", 50, 100, new PartitionOccupancy.Dedicated("dedicated-dataset")),
                    new LivePartitionState(
                        "2", 50, 100, new PartitionOccupancy.Dedicated("dedicated-dataset")),
                    new LivePartitionState("3", 0, 100, new PartitionOccupancy.Empty())),
                2))
        .containsExactly("1", "2");
  }
}
