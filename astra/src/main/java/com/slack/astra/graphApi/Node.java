package com.slack.astra.graphApi;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.SortedMap;

public class Node {
  private final String id;
  private final SortedMap<String, String> metadata;

  public Node(SortedMap<String, String> metadata) {
    if (metadata == null) throw new NullPointerException("metadata == null");
    this.metadata = metadata;
    // We use a SortedMap for metadata so that ID generation is deterministic.
    this.id = generateIdFromMetadata(metadata);
  }

  public static String generateIdFromMetadata(SortedMap<String, String> map) {
    return Hashing.sha256().hashString(map.toString(), StandardCharsets.UTF_8).toString();
  }

  public String getId() {
    return this.id;
  }

  public SortedMap<String, String> getMetadata() {
    return this.metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Node that)) return false;
    return this.id.equals(that.getId());
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= id.hashCode();

    return h;
  }
}
