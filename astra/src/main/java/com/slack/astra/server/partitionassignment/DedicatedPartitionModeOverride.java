package com.slack.astra.server.partitionassignment;

/** Tri-state override for preserving or explicitly changing dedicated partition mode. */
public enum DedicatedPartitionModeOverride {
  PRESERVE_EXISTING,
  REQUIRE_DEDICATED,
  REQUIRE_SHARED
}
