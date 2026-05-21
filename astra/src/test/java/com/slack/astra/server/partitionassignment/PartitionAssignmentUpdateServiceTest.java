package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PartitionAssignmentUpdateServiceTest {
  @Test
  public void shouldRejectNonPositiveMinimumPartitionCount() {
    assertThatThrownBy(() -> new PartitionAssignmentUpdateService(null, null, 0))
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
            PartitionAssignmentUpdateService.rolloverActivePartitionAssignment(
                existingDatasetMetadata, List.of("3"), 1000))
        .containsExactly(
            new DatasetPartitionMetadata(1001, 1002, List.of("2")),
            DatasetPartitionMetadata.createActive(1003, List.of("3")));
  }

  @Test
  public void shouldPreservePartitionHistoryWhenAssignmentDoesNotChange() {
    DatasetMetadata existingDatasetMetadata =
        new DatasetMetadata(
            "dataset",
            "owner",
            10,
            List.of(DatasetPartitionMetadata.createActive(1001, List.of("2", "3"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    assertThat(
            PartitionAssignmentUpdateService.rolloverActivePartitionAssignment(
                existingDatasetMetadata, List.of("2", "3"), 2000))
        .isEqualTo(existingDatasetMetadata.getPartitionConfigs());
  }

  @Test
  public void shouldPreservePartitionHistoryWhenAssignmentMembershipOnlyReorders() {
    DatasetMetadata existingDatasetMetadata =
        new DatasetMetadata(
            "dataset",
            "owner",
            10,
            List.of(DatasetPartitionMetadata.createActive(1001, List.of("2", "3"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    assertThat(
            PartitionAssignmentUpdateService.rolloverActivePartitionAssignment(
                existingDatasetMetadata, List.of("3", "2"), 2000))
        .isEqualTo(existingDatasetMetadata.getPartitionConfigs());
  }
}
