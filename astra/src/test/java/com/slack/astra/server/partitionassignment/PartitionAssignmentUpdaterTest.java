package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PartitionAssignmentUpdaterTest {
  @Test
  public void shouldRejectNonPositiveMinimumPartitionCount() {
    assertThatThrownBy(() -> new PartitionAssignmentUpdater(null, null, null, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minNumberOfPartitions must be greater than 0");
  }

  @Test
  public void shouldUseMonotonicCutoverTimesWhenCurrentTimeLagsActiveStart() {
    DatasetMetadata existingDatasetMetadata =
        new DatasetMetadata(
            "dataset",
            "owner",
            10,
            List.of(DatasetPartitionMetadata.createActive(1001, List.of("2"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    assertThat(
            PartitionAssignmentUpdater.rolloverActivePartitionAssignment(
                existingDatasetMetadata, List.of("3"), 1000))
        .containsExactly(
            new DatasetPartitionMetadata(1001, 1002, List.of("2")),
            DatasetPartitionMetadata.createActive(1003, List.of("3")));
  }
}
