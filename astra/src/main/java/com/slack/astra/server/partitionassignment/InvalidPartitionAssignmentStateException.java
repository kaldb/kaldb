package com.slack.astra.server.partitionassignment;

/** Thrown when persisted partition assignment metadata violates required invariants. */
public class InvalidPartitionAssignmentStateException extends RuntimeException {
  public InvalidPartitionAssignmentStateException(String message) {
    super(message);
  }
}
