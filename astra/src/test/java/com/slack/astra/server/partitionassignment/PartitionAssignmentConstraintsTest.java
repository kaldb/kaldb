package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class PartitionAssignmentConstraintsTest {
  @Test
  public void shouldRejectNonPositivePartitionCounts() {
    assertThatThrownBy(() -> PartitionAssignmentConstraints.perPartitionDemand(100, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("partitionCount must be positive, got 0");
  }
}
