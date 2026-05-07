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

  /** Returns a comparator that orders canonical partition IDs by numeric value. */
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

  /**
   * Parses a canonical non-negative integer partition ID.
   *
   * @param partitionId partition ID string in canonical decimal form, such as {@code 0} or {@code
   *     12}
   * @return the parsed numeric partition ID
   * @throws NullPointerException if {@code partitionId} is null
   * @throws IllegalArgumentException if {@code partitionId} is not a canonical non-negative integer
   *     or does not fit in a {@code long}
   */
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
