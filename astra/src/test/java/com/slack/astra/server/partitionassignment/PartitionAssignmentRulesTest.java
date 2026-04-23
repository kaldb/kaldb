package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PartitionAssignmentRulesTest {
  @Test
  public void shouldRejectManualAssignmentWhenBelowMinimumPartitionCount() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata("dataset", "owner", 20, List.of(), DatasetMetadata.MATCH_ALL_SERVICE);

    assertThatThrownBy(
            () ->
                PartitionAssignmentRules.validateManualSelection(
                    datasetMetadata,
                    20,
                    false,
                    List.of("1"),
                    List.of(
                        new LivePartitionState("1", 0, 100, new PartitionOccupancy.Empty()),
                        new LivePartitionState("2", 0, 100, new PartitionOccupancy.Empty())),
                    2))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            throwable -> {
              assertThat(throwable.getStatus().getCode())
                  .isEqualTo(Status.FAILED_PRECONDITION.getCode());
              assertThat(throwable.getStatus().getDescription())
                  .contains("Needed at least 2 partitions, found 1: [1]");
            });
  }

  @Test
  public void shouldRejectManualSharedAssignmentWhenSelectedPartitionLacksCapacity() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata("dataset", "owner", 0, List.of(), DatasetMetadata.MATCH_ALL_SERVICE);

    assertThatThrownBy(
            () ->
                PartitionAssignmentRules.validateManualSelection(
                    datasetMetadata,
                    100,
                    false,
                    List.of("1", "2"),
                    List.of(
                        new LivePartitionState(
                            "1", 60, 100, new PartitionOccupancy.Shared(List.of("other"))),
                        new LivePartitionState("2", 0, 100, new PartitionOccupancy.Empty())),
                    2))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            throwable -> {
              assertThat(throwable.getStatus().getCode())
                  .isEqualTo(Status.FAILED_PRECONDITION.getCode());
              assertThat(throwable.getStatus().getDescription())
                  .contains("Shared assignment requires each selected partition to be eligible")
                  .contains("[1]");
            });
  }

  @Test
  public void shouldRejectManualDedicatedAssignmentWhenSelectedPartitionIsNotDedicatedEligible() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata("payments", "owner", 0, List.of(), DatasetMetadata.MATCH_ALL_SERVICE);

    assertThatThrownBy(
            () ->
                PartitionAssignmentRules.validateManualSelection(
                    datasetMetadata,
                    20,
                    true,
                    List.of("1", "3"),
                    List.of(
                        new LivePartitionState(
                            "1",
                            100,
                            100,
                            new PartitionOccupancy.Shared(List.of("payments", "other"))),
                        new LivePartitionState("3", 0, 100, new PartitionOccupancy.Empty())),
                    2))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            throwable -> {
              assertThat(throwable.getStatus().getCode())
                  .isEqualTo(Status.FAILED_PRECONDITION.getCode());
              assertThat(throwable.getStatus().getDescription())
                  .contains("Dedicated assignment requires each selected partition to be eligible")
                  .contains("[1]");
            });
  }

  @Test
  public void shouldAllowManualDedicatedUpgradeOnSelfOwnedSharedPartitions() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            "payments",
            "owner",
            100,
            List.of(DatasetPartitionMetadata.createActive(1, List.of("1", "2"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    assertThatCode(
            () ->
                PartitionAssignmentRules.validateManualSelection(
                    datasetMetadata,
                    100,
                    true,
                    List.of("1", "2"),
                    List.of(
                        new LivePartitionState(
                            "1", 50, 100, new PartitionOccupancy.Shared(List.of("payments"))),
                        new LivePartitionState(
                            "2", 50, 100, new PartitionOccupancy.Shared(List.of("payments")))),
                    2))
        .doesNotThrowAnyException();
  }

  @Test
  public void shouldAllowManualSharedAssignmentByRemovingCurrentDatasetContribution() {
    DatasetMetadata datasetMetadata =
        new DatasetMetadata(
            "payments",
            "owner",
            100,
            List.of(DatasetPartitionMetadata.createActive(1, List.of("1", "2"))),
            DatasetMetadata.MATCH_ALL_SERVICE);

    assertThatCode(
            () ->
                PartitionAssignmentRules.validateManualSelection(
                    datasetMetadata,
                    100,
                    false,
                    List.of("1", "2"),
                    List.of(
                        new LivePartitionState(
                            "1",
                            100,
                            100,
                            new PartitionOccupancy.Shared(List.of("payments", "other-a"))),
                        new LivePartitionState(
                            "2",
                            100,
                            100,
                            new PartitionOccupancy.Shared(List.of("payments", "other-b")))),
                    2))
        .doesNotThrowAnyException();
  }
}
