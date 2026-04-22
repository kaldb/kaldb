package com.slack.astra.server.partitionassignment;

import java.util.Comparator;
import java.util.Objects;

/** Numeric ordering for partition IDs with fail-fast validation for non-numeric values. */
public final class PartitionIdOrdering {
  private static final Comparator<String> NUMERIC_COMPARATOR =
      Comparator.comparingLong(PartitionIdOrdering::parseNumericPartitionId);

  private PartitionIdOrdering() {}

  public static Comparator<String> numericComparator() {
    return NUMERIC_COMPARATOR;
  }

  public static long parseNumericPartitionId(String partitionId) {
    Objects.requireNonNull(partitionId, "partitionId");
    try {
      return Long.parseLong(partitionId);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Partition ID must be numeric: " + partitionId, e);
    }
  }
}
