package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

public class NodeTest {
  @Test
  void constructor_validParameters_createsNode() {
    SortedMap<String, String> metadata =
        new TreeMap<String, String>(
            Map.of("app", "app", "namespace", "namespace", "resource", "resource"));
    Node node = new Node(metadata);

    assertThat(node.getId()).isEqualTo(Node.generateIdFromMetadata(metadata));
  }

  @Test
  void equals_differentType_returnsFalse() {
    SortedMap<String, String> metadata =
        new TreeMap<String, String>(
            Map.of("app", "app", "namespace", "namespace", "resource", "resource"));
    Node node = new Node(metadata);

    assertThat(node.equals("not a node")).isFalse();
  }

  @Test
  void equals_sameValues_returnsTrue() {
    SortedMap<String, String> metadata1 =
        new TreeMap<String, String>(
            Map.of("app", "app", "namespace", "namespace", "resource", "resource"));

    SortedMap<String, String> metadata2 =
        new TreeMap<String, String>(
            Map.of("app", "app", "namespace", "namespace", "resource", "resource"));

    Node node1 = new Node(metadata1);
    Node node2 = new Node(metadata2);

    assertThat(node1).isEqualTo(node2);
  }

  @Test
  void equals_differentApp_returnsFalse() {
    SortedMap<String, String> metadata1 =
        new TreeMap<String, String>(
            Map.of("app1", "app", "namespace", "namespace", "resource", "resource"));

    SortedMap<String, String> metadata2 =
        new TreeMap<String, String>(
            Map.of("app2", "app", "namespace", "namespace", "resource", "resource"));

    Node node1 = new Node(metadata1);
    Node node2 = new Node(metadata2);

    assertThat(node1).isNotEqualTo(node2);
  }

  @Test
  void hashCode_sameValues_returnsSameHashCode() {
    SortedMap<String, String> metadata1 =
        new TreeMap<String, String>(
            Map.of("app", "app", "namespace", "namespace", "resource", "resource"));

    SortedMap<String, String> metadata2 =
        new TreeMap<String, String>(
            Map.of("app", "app", "namespace", "namespace", "resource", "resource"));

    Node node1 = new Node(metadata1);
    Node node2 = new Node(metadata2);

    assertThat(node1.hashCode()).isEqualTo(node2.hashCode());
  }
}
