package com.slack.astra.server.partitionassignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

public class PartitionIdOrderingTest {
  @Test
  public void shouldParseCanonicalNonNegativePartitionIds() {
    assertThat(PartitionIdOrdering.parseNumericPartitionId("0")).isZero();
    assertThat(PartitionIdOrdering.parseNumericPartitionId("1")).isEqualTo(1);
    assertThat(PartitionIdOrdering.parseNumericPartitionId("123")).isEqualTo(123);
  }

  @Test
  public void shouldRejectNonCanonicalPartitionIds() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> PartitionIdOrdering.parseNumericPartitionId("01"))
        .withMessage("Partition ID must be a canonical non-negative integer: 01");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> PartitionIdOrdering.parseNumericPartitionId("+1"))
        .withMessage("Partition ID must be a canonical non-negative integer: +1");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> PartitionIdOrdering.parseNumericPartitionId("-1"))
        .withMessage("Partition ID must be a canonical non-negative integer: -1");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> PartitionIdOrdering.parseNumericPartitionId("partition-a"))
        .withMessage("Partition ID must be a canonical non-negative integer: partition-a");
  }

  @Test
  public void shouldDetectCanonicalNonNegativePartitionIds() {
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("0")).isTrue();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("123")).isTrue();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId(null)).isFalse();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("01")).isFalse();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("+1")).isFalse();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("-1")).isFalse();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("partition-a")).isFalse();
    assertThat(PartitionIdOrdering.isCanonicalNonNegativePartitionId("999999999999999999999"))
        .isFalse();
  }

  @Test
  public void shouldOrderPartitionIdsNumerically() {
    assertThat(
            java.util.stream.Stream.of("10", "2", "1", "100", "0")
                .sorted(PartitionIdOrdering.numericComparator())
                .toList())
        .containsExactly("0", "1", "2", "10", "100");
  }
}
