package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class PartitionAssignmentUpdaterTest {
  @Test
  public void shouldRejectNonPositiveMinimumPartitionCount() {
    assertThatThrownBy(() -> new PartitionAssignmentUpdater(null, null, null, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minNumberOfPartitions must be greater than 0");
  }
}
