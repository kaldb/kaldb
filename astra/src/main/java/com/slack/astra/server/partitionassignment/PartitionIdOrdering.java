package com.slack.astra.server.partitionassignment;

import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Pattern;

/** Numeric ordering for canonical non-negative partition IDs. */
public final class PartitionIdOrdering {
  private static final Pattern CANONICAL_PARTITION_ID_PATTERN = Pattern.compile("0|[1-9][0-9]*");
  private static final Comparator<String> NUMERIC_COMPARATOR =
      Comparator.comparingLong(PartitionIdOrdering::parseNumericPartitionId);

  private PartitionIdOrdering() {}

  public static Comparator<String> numericComparator() {
    return NUMERIC_COMPARATOR;
  }

  public static boolean isCanonicalNonNegativePartitionId(String partitionId) {
    if (partitionId == null || !CANONICAL_PARTITION_ID_PATTERN.matcher(partitionId).matches()) {
      return false;
    }
    try {
      Long.parseLong(partitionId);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static long parseNumericPartitionId(String partitionId) {
    Objects.requireNonNull(partitionId, "partitionId");
    if (!CANONICAL_PARTITION_ID_PATTERN.matcher(partitionId).matches()) {
      throw new IllegalArgumentException(
          "Partition ID must be a canonical non-negative integer: " + partitionId);
    }
    try {
      return Long.parseLong(partitionId);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Partition ID must be a canonical non-negative integer: " + partitionId, e);
    }
  }
}
