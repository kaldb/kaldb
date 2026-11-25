package com.slack.astra.server.partitionassignment;

/** Thrown when persisted partition assignment metadata violates required invariants. */
public class InvalidPartitionAssignmentStateException extends RuntimeException {
  /**
   * Creates a new exception describing an invalid persisted partition-assignment state.
   *
   * @param message human-readable detail about the violated invariant
   */
  public InvalidPartitionAssignmentStateException(String message) {
    super(message);
  }
}
