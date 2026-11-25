package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

public class EdgeTest {

  @Test
  void builder_sourceNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge(null, "target", null))
        .withMessage("sourceNodeId cannot be null");
  }

  @Test
  void builder_targetNull_throwsNullPointerException() {
    assertThatExceptionOfType(NullPointerException.class)
        .isThrownBy(() -> new Edge("source", null, null))
        .withMessage("targetNodeId cannot be null");
  }

  @Test
  void build_withValidSourceAndTarget_succeeds() {
    Edge e = new Edge("source", "target", null);

    assertThat(e.getSourceNodeId()).isEqualTo("source");
    assertThat(e.getTargetNodeId()).isEqualTo("target");
  }

  @Test
  void build_withValidSourceTargetAndMetadata_succeeds() {
    Edge e = new Edge("source", "target", new TreeMap<>(Map.of("operation", "default")));

    assertThat(e.getSourceNodeId()).isEqualTo("source");
    assertThat(e.getTargetNodeId()).isEqualTo("target");
    assertThat(e.getMetadata()).isNotEmpty();
  }
}
