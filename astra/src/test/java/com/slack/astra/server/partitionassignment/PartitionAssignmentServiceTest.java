package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class PartitionAssignmentServiceTest {
  @Test
  public void shouldRejectNonPositiveMinimumPartitionCount() {
    assertThatThrownBy(() -> new PartitionAssignmentService(null, null, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minNumberOfPartitions must be greater than 0");
  }
}
