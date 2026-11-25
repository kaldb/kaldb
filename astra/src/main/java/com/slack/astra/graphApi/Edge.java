package com.slack.astra.graphApi;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;
import java.util.SortedMap;

/**
 * Represents a directed connection between two Nodes.
 *
 * <p>Equality is based on sourceNodeId, targetNodeId, and metadata. The observationCount field is
 * mutable and excluded from equality, allowing edges to be accumulated in collections while
 * tracking how many times the same logical edge is observed.
 */
public class Edge {
  private final String sourceNodeId;
  private final String targetNodeId;
  private final SortedMap<String, String> metadata;
  private int observedCount = 0;

  public Edge(String sourceNodeId, String targetNodeId, SortedMap<String, String> metadata) {
    this.sourceNodeId = checkNotNull(sourceNodeId, "sourceNodeId cannot be null");
    this.targetNodeId = checkNotNull(targetNodeId, "targetNodeId cannot be null");
    this.metadata = metadata;
  }

  static String generateKey(
      String sourceNodeId, String targetNodeId, SortedMap<String, String> metadata) {
    return sourceNodeId + targetNodeId + (metadata == null ? 0 : metadata.hashCode());
  }

  public String getSourceNodeId() {
    return sourceNodeId;
  }

  public String getTargetNodeId() {
    return targetNodeId;
  }

  public SortedMap<String, String> getMetadata() {
    return metadata;
  }

  public int getObservedCount() {
    return observedCount;
  }

  public void incrementObservedCount() {
    observedCount++;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Edge edge)) return false;
    return Objects.equals(sourceNodeId, edge.sourceNodeId)
        && Objects.equals(targetNodeId, edge.targetNodeId)
        && Objects.equals(metadata, edge.metadata);
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= sourceNodeId.hashCode();
    h *= 1000003;
    h ^= targetNodeId.hashCode();
    h *= 1000003;
    h ^= (metadata == null) ? 0 : metadata.hashCode();
    return h;
  }
}
