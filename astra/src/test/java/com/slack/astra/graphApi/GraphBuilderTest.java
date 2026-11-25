package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GraphBuilderTest {
  private GraphBuilder defaultGraphBuilder;
  private GraphBuilder configuredGraphBuilder;

  @BeforeEach
  void setUp() throws IOException {
    defaultGraphBuilder = new GraphBuilder(GraphConfig.DEFAULT);

    // Load custom config from YAML file
    Path configPath =
        new File(
                Objects.requireNonNull(
                        getClass()
                            .getClassLoader()
                            .getResource("test-dependency-graph-config.yaml"))
                    .getFile())
            .toPath();
    GraphConfig customConfig = GraphConfig.load(configPath);
    configuredGraphBuilder = new GraphBuilder(customConfig);
  }

  @Test
  void buildFromSpans_emptyList_returnsEmptyGraph() {
    List<ZipkinSpanResponse> spans = new ArrayList<>();
    Graph graph = defaultGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).isEmpty();
    assertThat(graph.edges()).isEmpty();
  }

  @Test
  void buildFromSpans_parentChildSpans_createsNodesWithEdge() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app",
                    "app2",
                    "kube.namespace",
                    "ns2",
                    "operation_name",
                    "http.request",
                    "resource",
                    "res2",
                    "tag.http.target.canonical_path",
                    "/v2/res2",
                    "tag.http.target.host",
                    "app2.ns2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    // Generate expected node IDs using the static method
    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app1.ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app2.ns2",
                "resource", "/v2/res2"));

    // uses canonical path as resource
    SortedMap<String, String> edgeMetadata = new TreeMap<>(Map.of("operation", "http.request"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);
    Edge edge = graph.edges().iterator().next();
    assertThat(edge.getSourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.getTargetNodeId()).isEqualTo(expectedChildId);
    assertThat(edge.getMetadata()).isEqualTo(edgeMetadata);
    assertThat(edge.getObservedCount()).isEqualTo(1);
  }

  @Test
  void buildFromSpans_spanWithNullId_skipsSpan() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // valid span
            TestUtils.createSpanWithTags(
                "span1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            // invalid span
            TestUtils.createSpanWithTags(
                null,
                "trace1",
                null,
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.edges()).hasSize(0);
    // No nodes because there are no edges
    assertThat(graph.nodes()).hasSize(0);
  }

  @Test
  void buildFromSpans_multipleChildrenSameParent_createsMultipleEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "op3",
                    "resource", "res3")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).hasSize(3);
    assertThat(graph.edges()).hasSize(2);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app1.ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "service", "app2.ns2",
                "resource", "res2"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> child2Metadata =
        new TreeMap<>(
            Map.of(
                "service", "app3.ns3",
                "resource", "res3"));
    String expectedChild2Id = Node.generateIdFromMetadata(child2Metadata);

    // verify both edges have the same parent
    List<Edge> edges = graph.edges();
    assertThat(edges.stream().allMatch(edge -> edge.getSourceNodeId().equals(expectedParentId)))
        .isTrue();

    // metadata of the edges
    assertThat(edges.get(0).getMetadata()).isEqualTo(new TreeMap<>(Map.of("operation", "op2")));
    assertThat(edges.get(0).getObservedCount()).isEqualTo(1);
    assertThat(edges.get(1).getMetadata()).isEqualTo(new TreeMap<>(Map.of("operation", "op3")));
    assertThat(edges.get(1).getObservedCount()).isEqualTo(1);

    // verify different children
    List<String> childIds =
        List.of(edges.stream().map(Edge::getTargetNodeId).toArray(String[]::new));
    assertThat(childIds).containsExactlyInAnyOrder(expectedChild1Id, expectedChild2Id);
  }

  @Test
  void buildFromSpans_duplicateEdges_deduplicatesEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            // two different child spans that reference the same parent
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            // second span with same child node ID but different span ID - should create deduplicate
            // edge
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    // should have 2 nodes (parent and child - child nodes are deduplicated)
    assertThat(graph.nodes()).hasSize(2);

    // should have only 1 edge despite multiple spans creating the same parent-child relationship
    assertThat(graph.edges()).hasSize(1);
    assertThat(graph.edges().get(0).getMetadata())
        .isEqualTo(new TreeMap<>(Map.of("operation", "op2")));
    assertThat(graph.edges().get(0).getObservedCount()).isEqualTo(2);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app1.ns1",
                "resource", "res1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app2.ns2",
                "resource", "res2"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);

    Edge edge = graph.edges().getFirst();
    assertThat(edge.getSourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.getTargetNodeId()).isEqualTo(expectedChildId);
  }

  @Test
  void buildFromSpans_complexHierarchy_buildsCorrectGraph() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // root span
            TestUtils.createSpanWithTags(
                "root",
                "trace1",
                null,
                Map.of(
                    "kube.app", "root_app",
                    "kube.namespace", "root_ns",
                    "operation_name", "root_op",
                    "resource", "root_res")),
            // first level children
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "operation_name", "child1_op",
                    "resource", "child1_res")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child2_app",
                    "kube.namespace", "child2_ns",
                    "operation_name", "child2_op",
                    "resource", "child2_res")),
            // second level child
            TestUtils.createSpanWithTags(
                "grandchild",
                "trace1",
                "child1",
                Map.of(
                    "kube.app",
                    "gc_app",
                    "kube.namespace",
                    "gc_ns",
                    "operation_name",
                    "gc_op",
                    "resource",
                    "gc_res")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    assertThat(graph.nodes()).hasSize(4);
    assertThat(graph.edges()).hasSize(3);

    SortedMap<String, String> rootMetadata =
        new TreeMap<>(
            Map.of(
                "service", "root_app.root_ns",
                "resource", "root_res"));
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "service", "child1_app.child1_ns",
                "resource", "child1_res"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> child2Metadata =
        new TreeMap<>(
            Map.of(
                "service", "child2_app.child2_ns",
                "resource", "child2_res"));
    String expectedChild2Id = Node.generateIdFromMetadata(child2Metadata);

    SortedMap<String, String> grandchildMetadata =
        new TreeMap<>(
            Map.of(
                "service", "gc_app.gc_ns",
                "resource", "gc_res"));
    String expectedGrandchildId = Node.generateIdFromMetadata(grandchildMetadata);

    List<Edge> edges = graph.edges();

    assertThat(edges)
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedRootId)
                    && edge.getTargetNodeId().equals(expectedChild1Id)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "child1_op")))
                    && edge.getObservedCount() == 1);

    assertThat(edges)
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedRootId)
                    && edge.getTargetNodeId().equals(expectedChild2Id)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "child2_op")))
                    && edge.getObservedCount() == 1);

    assertThat(edges)
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedChild1Id)
                    && edge.getTargetNodeId().equals(expectedGrandchildId)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "gc_op")))
                    && edge.getObservedCount() == 1);
  }

  @Test
  void buildFromSpans_noFilterWithCycles_buildCorrectGraph() {
    // Topology: A -> spanB1 -> C
    //           D -> spanB2 -> E
    //           E -> spanB3 (creates cycle back to node B)
    // where spanB1, spanB2, spanB3 all represent the same logical node
    List<ZipkinSpanResponse> spans =
        List.of(
            // Root span A
            TestUtils.createSpanWithTags(
                "spanA",
                "trace1",
                null,
                Map.of(
                    "kube.app", "appA",
                    "kube.namespace", "nsA",
                    "operation_name", "opA",
                    "resource", "resA")),
            // Span B1 - child of A
            TestUtils.createSpanWithTags(
                "spanB1",
                "trace1",
                "spanA",
                Map.of(
                    "kube.app", "appB",
                    "kube.namespace", "nsB",
                    "operation_name", "opB",
                    "resource", "resB")),
            // Span C - child of B1
            TestUtils.createSpanWithTags(
                "spanC",
                "trace1",
                "spanB1",
                Map.of(
                    "kube.app", "appC",
                    "kube.namespace", "nsC",
                    "operation_name", "opC",
                    "resource", "resC")),
            // Span D - another root
            TestUtils.createSpanWithTags(
                "spanD",
                "trace1",
                null,
                Map.of(
                    "kube.app", "appD",
                    "kube.namespace", "nsD",
                    "operation_name", "opD",
                    "resource", "resD")),
            // Span B2 - child of D
            TestUtils.createSpanWithTags(
                "spanB2",
                "trace1",
                "spanD",
                Map.of(
                    "kube.app", "appB",
                    "kube.namespace", "nsB",
                    "operation_name", "opB",
                    "resource", "resB")),
            // Span E - child of B2
            TestUtils.createSpanWithTags(
                "spanE",
                "trace1",
                "spanB2",
                Map.of(
                    "kube.app", "appE",
                    "kube.namespace", "nsE",
                    "operation_name", "opE",
                    "resource", "resE")),
            // Span B3 - child of E, creates cycle back to node B
            TestUtils.createSpanWithTags(
                "spanB3",
                "trace1",
                "spanE",
                Map.of(
                    "kube.app", "appB",
                    "kube.namespace", "nsB",
                    "operation_name", "opB",
                    "resource", "resB")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    // Should have 5 nodes: A, B, C, D, E
    assertThat(graph.nodes()).hasSize(5);

    // Should have edges: A->B, B->C, D->B, B->E, E->B (cycle)
    assertThat(graph.edges()).hasSize(5);

    SortedMap<String, String> nodeAMetadata =
        new TreeMap<>(
            Map.of(
                "service", "appA.nsA",
                "resource", "resA"));
    String expectedNodeAId = Node.generateIdFromMetadata(nodeAMetadata);

    SortedMap<String, String> nodeBMetadata =
        new TreeMap<>(
            Map.of(
                "service", "appB.nsB",
                "resource", "resB"));
    String expectedNodeBId = Node.generateIdFromMetadata(nodeBMetadata);

    SortedMap<String, String> nodeCMetadata =
        new TreeMap<>(
            Map.of(
                "service", "appC.nsC",
                "resource", "resC"));
    String expectedNodeCId = Node.generateIdFromMetadata(nodeCMetadata);

    SortedMap<String, String> nodeDMetadata =
        new TreeMap<>(
            Map.of(
                "service", "appD.nsD",
                "resource", "resD"));
    String expectedNodeDId = Node.generateIdFromMetadata(nodeDMetadata);

    SortedMap<String, String> nodeEMetadata =
        new TreeMap<>(
            Map.of(
                "service", "appE.nsE",
                "resource", "resE"));
    String expectedNodeEId = Node.generateIdFromMetadata(nodeEMetadata);

    // A -> B
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeAId)
                    && edge.getTargetNodeId().equals(expectedNodeBId)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "opB")))
                    && edge.getObservedCount() == 1);

    // B -> C
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeBId)
                    && edge.getTargetNodeId().equals(expectedNodeCId)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "opC")))
                    && edge.getObservedCount() == 1);

    // D -> B
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeDId)
                    && edge.getTargetNodeId().equals(expectedNodeBId)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "opB")))
                    && edge.getObservedCount() == 1);

    // B -> E
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeBId)
                    && edge.getTargetNodeId().equals(expectedNodeEId)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "opE")))
                    && edge.getObservedCount() == 1);

    // E -> B
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeEId)
                    && edge.getTargetNodeId().equals(expectedNodeBId)
                    && edge.getMetadata().equals(new TreeMap<>(Map.of("operation", "opB")))
                    && edge.getObservedCount() == 1);
  }

  @Test
  void buildFromSpans_withFilter_includesOnlyMatchingNodesWithEdges() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app",
                    "app1",
                    "kube.namespace",
                    "ns1",
                    "operation_name",
                    "http.request",
                    "resource",
                    "res1",
                    "tag.http.target.canonical_path",
                    "/v2/target1",
                    "tag.http.target.host",
                    "target_app1.target_ns1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app",
                    "app2",
                    "kube.namespace",
                    "ns2",
                    "operation_name",
                    "http.request",
                    "resource",
                    "res2",
                    "tag.http.target.canonical_path",
                    "/v2/target2",
                    "tag.http.target.host",
                    "target_app2.target_ns2")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "op3",
                    "resource", "res3")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("http.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should only include parent1 and child1 nodes (both match filter and have edge between them)
    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app1.target_ns1",
                "resource", "/v2/target1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app2.target_ns2",
                "resource", "/v2/target2"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    Edge edge = graph.edges().get(0);
    assertThat(edge.getSourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.getTargetNodeId()).isEqualTo(expectedChild1Id);
    assertThat(edge.getMetadata()).isEqualTo(new TreeMap<>(Map.of("operation", "http.request")));
    assertThat(edge.getObservedCount()).isEqualTo(1);
  }

  @Test
  void buildFromSpans_withFilter_skipsIntermediateNonMatchingNodes() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // root - operation: http.request
            TestUtils.createSpanWithTags(
                "root",
                "trace1",
                null,
                Map.of(
                    "kube.app",
                    "root_app",
                    "kube.namespace",
                    "root_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "root_res",
                    "tag.http.target.canonical_path",
                    "/v2/target1",
                    "tag.http.target.host",
                    "target_app1.target_ns1")),
            // intermediate child - operation: op2 (doesn't match filter)
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app", "child1_app",
                    "kube.namespace", "child1_ns",
                    "operation_name", "op2",
                    "resource", "child1_res")),
            // grandchild - operation: http.request (matches filter)
            TestUtils.createSpanWithTags(
                "grandchild",
                "trace1",
                "child1",
                Map.of(
                    "kube.app",
                    "gc_app",
                    "kube.namespace",
                    "gc_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "gc_res",
                    "tag.http.target.canonical_path",
                    "/v2/target2",
                    "tag.http.target.host",
                    "target_app2.target_ns2")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("http.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include root and grandchild nodes, skipping intermediate child1
    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> rootMetadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app1.target_ns1",
                "resource", "/v2/target1"));
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    SortedMap<String, String> grandchildMetadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app2.target_ns2",
                "resource", "/v2/target2"));
    String expectedGrandchildId = Node.generateIdFromMetadata(grandchildMetadata);

    // Should have edge directly from root to grandchild (skipping intermediate)
    Edge edge = graph.edges().get(0);
    assertThat(edge.getSourceNodeId()).isEqualTo(expectedRootId);
    assertThat(edge.getTargetNodeId()).isEqualTo(expectedGrandchildId);
    assertThat(edge.getMetadata()).isEqualTo(new TreeMap<>(Map.of("operation", "http.request")));
    assertThat(edge.getObservedCount()).isEqualTo(1);
  }

  @Test
  void buildFromSpans_withFilter_noMatchingNodes_returnsEmptyGraph() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    // Filter that doesn't match any nodes
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("nonexistent.operation")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    assertThat(graph.nodes()).isEmpty();
    assertThat(graph.edges()).isEmpty();
  }

  @Test
  void buildFromSpans_withFilter_multipleMatchingPaths() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // root - http.request
            TestUtils.createSpanWithTags(
                "root",
                "trace1",
                null,
                Map.of(
                    "kube.app",
                    "root_app",
                    "kube.namespace",
                    "root_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "root_res",
                    "tag.http.target.canonical_path",
                    "/v2/target1",
                    "tag.http.target.host",
                    "target_app1.target_ns1")),
            // child1 - http.request
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "root",
                Map.of(
                    "kube.app",
                    "child1_app",
                    "kube.namespace",
                    "child1_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "child1_res",
                    "tag.http.target.canonical_path",
                    "/v2/target2",
                    "tag.http.target.host",
                    "target_app2.target_app2")),
            // child2 - http.request
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "root",
                Map.of(
                    "kube.app",
                    "child2_app",
                    "kube.namespace",
                    "child2_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "child2_res",
                    "tag.http.target.canonical_path",
                    "/v2/target3",
                    "tag.http.target.host",
                    "target_app3.target+_ns3")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("http.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include all 3 nodes since they all match
    assertThat(graph.nodes()).hasSize(3);
    // Should have 2 edges: root->child1 and root->child2
    assertThat(graph.edges()).hasSize(2);

    SortedMap<String, String> rootMetadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app1.target_ns1",
                "resource", "/v2/target1"));
    String expectedRootId = Node.generateIdFromMetadata(rootMetadata);

    // Both edges should originate from root
    assertThat(
            graph.edges().stream().allMatch(edge -> edge.getSourceNodeId().equals(expectedRootId)))
        .isTrue();
  }

  @Test
  void buildFromSpans_withFilter_multipleDisconnectedSubtreesWithDifferentDepths() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // First subtree: intermediate1 (parent missing) -> child1 -> grandchild1
            TestUtils.createSpanWithTags(
                "intermediate1",
                "trace1",
                "missingParent1", // parent doesn't exist in span list
                Map.of(
                    "kube.app", "int1_app",
                    "kube.namespace", "int1_ns",
                    "operation_name", "op1",
                    "resource", "int1_res")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "intermediate1",
                Map.of(
                    "kube.app",
                    "child1_app",
                    "kube.namespace",
                    "child1_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "child1_res",
                    "tag.http.target.canonical_path",
                    "/v2/target1",
                    "tag.http.target.host",
                    "target_app1.target_ns1")),
            TestUtils.createSpanWithTags(
                "grandchild1",
                "trace1",
                "child1",
                Map.of(
                    "kube.app",
                    "gc1_app",
                    "kube.namespace",
                    "gc1_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "gc1_res",
                    "tag.http.target.canonical_path",
                    "/v2/target2",
                    "tag.http.target.host",
                    "target_app2.target_ns2")),
            // Second subtree: non-matching nodes -> matching -> more non-matching -> matching leaf
            // intermediate2a (parent missing) -> intermediate2b -> intermediate2c -> matching1 ->
            // intermediate2d -> intermediate2e -> matching2
            TestUtils.createSpanWithTags(
                "intermediate2a",
                "trace1",
                "missingParent2", // parent doesn't exist in span list
                Map.of(
                    "kube.app", "int2a_app",
                    "kube.namespace", "int2a_ns",
                    "operation_name", "op2a",
                    "resource", "int2a_res")),
            TestUtils.createSpanWithTags(
                "intermediate2b",
                "trace1",
                "intermediate2a",
                Map.of(
                    "kube.app", "int2b_app",
                    "kube.namespace", "int2b_ns",
                    "operation_name", "op2b",
                    "resource", "int2b_res")),
            TestUtils.createSpanWithTags(
                "intermediate2c",
                "trace1",
                "intermediate2b",
                Map.of(
                    "kube.app", "int2c_app",
                    "kube.namespace", "int2c_ns",
                    "operation_name", "op2c",
                    "resource", "int2c_res")),
            TestUtils.createSpanWithTags(
                "matching1",
                "trace1",
                "intermediate2c",
                Map.of(
                    "kube.app",
                    "match1_app",
                    "kube.namespace",
                    "match1_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "match1_res",
                    "tag.http.target.canonical_path",
                    "/v2/target3",
                    "tag.http.target.host",
                    "target_app3.target_ns3")),
            TestUtils.createSpanWithTags(
                "intermediate2d",
                "trace1",
                "matching1",
                Map.of(
                    "kube.app", "int2d_app",
                    "kube.namespace", "int2d_ns",
                    "operation_name", "op2d",
                    "resource", "int2d_res")),
            TestUtils.createSpanWithTags(
                "intermediate2e",
                "trace1",
                "intermediate2d",
                Map.of(
                    "kube.app", "int2e_app",
                    "kube.namespace", "int2e_ns",
                    "operation_name", "op2e",
                    "resource", "int2e_res")),
            TestUtils.createSpanWithTags(
                "matching2",
                "trace1",
                "intermediate2e",
                Map.of(
                    "kube.app",
                    "match2_app",
                    "kube.namespace",
                    "match2_ns",
                    "operation_name",
                    "http.request",
                    "resource",
                    "match2_res",
                    "tag.http.target.canonical_path",
                    "/v2/target4",
                    "tag.http.target.host",
                    "target_app4.target_ns4")));

    // Filter to only include nodes with operation "http.request"
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("http.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include 4 matching nodes from both disconnected subtrees
    assertThat(graph.nodes()).hasSize(4);
    // Should have 2 edges: child1->grandchild1 and matching2->leaf2 (skipping multiple non-matching
    // nodes)
    assertThat(graph.edges()).hasSize(2);

    SortedMap<String, String> child1Metadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app1.target_ns1",
                "resource", "/v2/target1"));
    String expectedChild1Id = Node.generateIdFromMetadata(child1Metadata);

    SortedMap<String, String> grandchild1Metadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app2.target_ns2",
                "resource", "/v2/target2"));
    String expectedGrandchild1Id = Node.generateIdFromMetadata(grandchild1Metadata);

    SortedMap<String, String> matching1Metadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app3.target_ns3",
                "resource", "/v2/target3"));
    String expectedMatching1Id = Node.generateIdFromMetadata(matching1Metadata);

    SortedMap<String, String> matching2Metadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app4.target_ns4",
                "resource", "/v2/target4"));
    String expectedMatching2Id = Node.generateIdFromMetadata(matching2Metadata);

    // Verify both disconnected edges exist
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedChild1Id)
                    && edge.getTargetNodeId().equals(expectedGrandchild1Id));

    // This edge should skip multiple non-matching intermediate nodes
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedMatching1Id)
                    && edge.getTargetNodeId().equals(expectedMatching2Id));
  }

  @Test
  void buildFromSpans_withMultipleFilterOptions_matchesAnyOption() {
    List<ZipkinSpanResponse> spans =
        List.of(
            // Parent with operation: http.request
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app",
                    "app1",
                    "kube.namespace",
                    "ns1",
                    "operation_name",
                    "http.request",
                    "resource",
                    "res1",
                    "tag.http.target.canonical_path",
                    "/v2/target1",
                    "tag.http.target.host",
                    "target_app1.target_ns1")),
            // Child with operation: grpc.request
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                // this should use the default key since there are no rules for grpc.request
                // operations
                Map.of(
                    "kube.app",
                    "app2",
                    "kube.namespace",
                    "ns2",
                    "operation_name",
                    "grpc.request",
                    "resource",
                    "res2",
                    "tag.http.target.canonical_path",
                    "/v2/target2",
                    "tag.http.target.host",
                    "target_app2.target_ns2")),
            // Grandchild with operation: other (doesn't match either filter option)
            TestUtils.createSpanWithTags(
                "grandchild1",
                "trace1",
                "child1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "other.operation",
                    "resource", "res3")));

    // Filter with two options: matches nodes with either http.request OR grpc.request
    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("http.request", "grpc.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include parent1 and child1 (both match at least one filter option)
    assertThat(graph.nodes()).hasSize(2);
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> parentMetadata =
        new TreeMap<>(
            Map.of(
                "service", "target_app1.target_ns1",
                "resource", "/v2/target1"));
    String expectedParentId = Node.generateIdFromMetadata(parentMetadata);

    SortedMap<String, String> childMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app2.ns2",
                "resource", "res2"));
    String expectedChildId = Node.generateIdFromMetadata(childMetadata);

    Edge edge = graph.edges().get(0);
    assertThat(edge.getSourceNodeId()).isEqualTo(expectedParentId);
    assertThat(edge.getTargetNodeId()).isEqualTo(expectedChildId);
    assertThat(edge.getMetadata()).isEqualTo(new TreeMap<>(Map.of("operation", "grpc.request")));
    assertThat(edge.getObservedCount()).isEqualTo(1);
  }

  @Test
  void buildFromSpans_withEmptyFilter_returnsAllNodes() {
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app3",
                    "kube.namespace", "ns3",
                    "operation_name", "op3",
                    "resource", "res3")));

    // Empty filter - should match all nodes
    GraphBuilder.Filter emptyFilter = new GraphBuilder.Filter(Map.of());
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(emptyFilter));

    // Should include all nodes and edges (same as no filter)
    assertThat(graph.nodes()).hasSize(3);
    assertThat(graph.edges()).hasSize(2);
  }

  @Test
  void buildFromSpans_withFilterAndCycles_buildsCorrectGraph() {
    // Combined test demonstrating both forward and backward transitive dependencies with cycles:
    //
    // Topology:
    // A (http.request) -> B (op2) -> C (http.request) -> D (op2) -> E (http.request), B -> G
    // Add cycles: E -> B (backward), C -> F (http.request) -> B (backward)
    //
    // This demonstrates:
    // 1. Forward transitive deps: A -> C (through B), A -> G (through B),
    // C -> E (through D), C -> F (direct)
    // 2. Backward transitive deps via cycles: E -> C (backward through B),
    // F -> C (backward through B)
    // 3. Handles cycles without infinite loops
    List<ZipkinSpanResponse> spans =
        new ArrayList<>(
            List.of(
                // Root span A - matches filter
                TestUtils.createSpanWithTags(
                    "spanA",
                    "trace1",
                    null,
                    Map.of(
                        "kube.app",
                        "appA",
                        "kube.namespace",
                        "nsA",
                        "operation_name",
                        "http.request",
                        "resource",
                        "resA",
                        "tag.http.target.canonical_path",
                        "/v2/targetA",
                        "tag.http.target.host",
                        "targetA.nsA")),
                // Span B - doesn't match filter, child of A
                TestUtils.createSpanWithTags(
                    "spanB",
                    "trace1",
                    "spanA",
                    Map.of(
                        "kube.app", "appB",
                        "kube.namespace", "nsB",
                        "operation_name", "op2",
                        "resource", "resB")),
                // Span C - matches filter, child of B
                TestUtils.createSpanWithTags(
                    "spanC",
                    "trace1",
                    "spanB",
                    Map.of(
                        "kube.app",
                        "appC",
                        "kube.namespace",
                        "nsC",
                        "operation_name",
                        "http.request",
                        "resource",
                        "resC",
                        "tag.http.target.canonical_path",
                        "/v2/targetC",
                        "tag.http.target.host",
                        "targetC.nsC")),
                // Span D - doesn't match filter, child of C
                TestUtils.createSpanWithTags(
                    "spanD",
                    "trace1",
                    "spanC",
                    Map.of(
                        "kube.app", "appD",
                        "kube.namespace", "nsD",
                        "operation_name", "op2",
                        "resource", "resD")),
                // Span E - matches filter, child of D
                TestUtils.createSpanWithTags(
                    "spanE",
                    "trace1",
                    "spanD",
                    Map.of(
                        "kube.app",
                        "appE",
                        "kube.namespace",
                        "nsE",
                        "operation_name",
                        "http.request",
                        "resource",
                        "resE",
                        "tag.http.target.canonical_path",
                        "/v2/targetE",
                        "tag.http.target.host",
                        "targetE.nsE")),
                // Span F - matches filter, child of C (parallel to D)
                TestUtils.createSpanWithTags(
                    "spanF",
                    "trace1",
                    "spanC",
                    Map.of(
                        "kube.app",
                        "appF",
                        "kube.namespace",
                        "nsF",
                        "operation_name",
                        "http.request",
                        "resource",
                        "resF",
                        "tag.http.target.canonical_path",
                        "/v2/targetF",
                        "tag.http.target.host",
                        "targetF.nsF")),
                // Create backward cycle: E -> B
                TestUtils.createSpanWithTags(
                    "spanB_from_E",
                    "trace1",
                    "spanE",
                    Map.of(
                        "kube.app", "appB",
                        "kube.namespace", "nsB",
                        "operation_name", "op2",
                        "resource", "resB")),
                // Create another backward cycle: F -> B (another path back)
                TestUtils.createSpanWithTags(
                    "spanB_from_F",
                    "trace1",
                    "spanF",
                    Map.of(
                        "kube.app", "appB",
                        "kube.namespace", "nsB",
                        "operation_name", "op2",
                        "resource", "resB")),
                // Add a child from spanB_from_E to demonstrate that children of sibling spans
                // are discovered even when visitedNodes skips the sibling span itself
                TestUtils.createSpanWithTags(
                    "spanG",
                    "trace1",
                    "spanB_from_E",
                    Map.of(
                        "kube.app",
                        "appG",
                        "kube.namespace",
                        "nsG",
                        "operation_name",
                        "http.request",
                        "resource",
                        "resG",
                        "tag.http.target.canonical_path",
                        "/v2/targetG",
                        "tag.http.target.host",
                        "targetG.nsG"))));

    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("http.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include A, C, E, F, G (all match filter)
    assertThat(graph.nodes()).hasSize(5);

    // Expect 8 edges (original 5 + A->G + E->G + F->G)
    // The key insight: when traversing from A through node B, ALL sibling spans of B
    // (spanB, spanB_from_E, spanB_from_F) are processed together, discovering:
    //   - C (child of spanB)
    //   - G (child of spanB_from_E)
    // So we get: A->C, A->G, C->E, C->F, E->C, F->C, E->G, F->G
    assertThat(graph.edges()).hasSize(8);

    SortedMap<String, String> nodeAMetadata =
        new TreeMap<>(
            Map.of(
                "service", "targetA.nsA",
                "resource", "/v2/targetA"));
    String expectedNodeAId = Node.generateIdFromMetadata(nodeAMetadata);

    SortedMap<String, String> nodeCMetadata =
        new TreeMap<>(
            Map.of(
                "service", "targetC.nsC",
                "resource", "/v2/targetC"));
    String expectedNodeCId = Node.generateIdFromMetadata(nodeCMetadata);

    SortedMap<String, String> nodeEMetadata =
        new TreeMap<>(
            Map.of(
                "service", "targetE.nsE",
                "resource", "/v2/targetE"));
    String expectedNodeEId = Node.generateIdFromMetadata(nodeEMetadata);

    SortedMap<String, String> nodeFMetadata =
        new TreeMap<>(
            Map.of(
                "service", "targetF.nsF",
                "resource", "/v2/targetF"));
    String expectedNodeFId = Node.generateIdFromMetadata(nodeFMetadata);

    SortedMap<String, String> nodeGMetadata =
        new TreeMap<>(
            Map.of(
                "service", "targetG.nsG",
                "resource", "/v2/targetG"));
    String expectedNodeGId = Node.generateIdFromMetadata(nodeGMetadata);

    // A -> C
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeAId)
                    && edge.getTargetNodeId().equals(expectedNodeCId));

    // A -> G
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeAId)
                    && edge.getTargetNodeId().equals(expectedNodeGId));

    // C -> E
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeCId)
                    && edge.getTargetNodeId().equals(expectedNodeEId));

    // C -> F
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeCId)
                    && edge.getTargetNodeId().equals(expectedNodeFId));

    // E -> C
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeEId)
                    && edge.getTargetNodeId().equals(expectedNodeCId));

    // F -> C
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeFId)
                    && edge.getTargetNodeId().equals(expectedNodeCId));

    // E -> G
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeEId)
                    && edge.getTargetNodeId().equals(expectedNodeGId));

    // F -> G
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeFId)
                    && edge.getTargetNodeId().equals(expectedNodeGId));
  }

  @Test
  void buildFromSpans_withFilterSelfLoopSameLogicalNode_buildsCorrectGraph() {
    List<ZipkinSpanResponse> spans =
        new ArrayList<>(
            List.of(
                TestUtils.createSpanWithTags(
                    "spanA",
                    "trace1",
                    null,
                    Map.of(
                        "kube.app",
                        "app1",
                        "kube.namespace",
                        "ns1",
                        "operation_name",
                        "dropwizard.request",
                        "resource",
                        "res1")),
                TestUtils.createSpanWithTags(
                    "spanB",
                    "trace1",
                    "spanA",
                    Map.of(
                        "tag.http.target.host",
                        "app2.ns2",
                        "tag.http.target.canonical_path",
                        "/v1/targetB",
                        "operation_name",
                        "http.request")),
                // spanB and spanC point to the same logical node, but have different operations
                TestUtils.createSpanWithTags(
                    "spanC",
                    "trace1",
                    "spanB",
                    Map.of(
                        "kube.app",
                        "app2",
                        "kube.namespace",
                        "ns2",
                        "operation_name",
                        "dropwizard.request",
                        "resource",
                        "/v1/targetB"))));

    GraphBuilder.Filter filter =
        new GraphBuilder.Filter(Map.of("operation_name", List.of("dropwizard.request")));
    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.of(filter));

    // Should include A, B (all match filter)
    assertThat(graph.nodes()).hasSize(2);

    // Expect 1 edge A -> B (dropwizard.request)
    assertThat(graph.edges()).hasSize(1);

    SortedMap<String, String> nodeAMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app1.ns1",
                "resource", "res1"));
    String expectedNodeAId = Node.generateIdFromMetadata(nodeAMetadata);

    SortedMap<String, String> nodeBMetadata =
        new TreeMap<>(
            Map.of(
                "service", "app2.ns2",
                "resource", "/v1/targetB"));
    String expectedNodeBId = Node.generateIdFromMetadata(nodeBMetadata);

    // A -> B
    assertThat(graph.edges())
        .anyMatch(
            edge ->
                edge.getSourceNodeId().equals(expectedNodeAId)
                    && edge.getTargetNodeId().equals(expectedNodeBId));

    Edge edge = graph.edges().get(0);
    assertThat(edge.getMetadata())
        .isEqualTo(new TreeMap<>(Map.of("operation", "dropwizard.request")));
    assertThat(edge.getObservedCount()).isEqualTo(1);
  }

  @Test
  void buildFromSpans_multipleSpansSameEdge_incrementsObservationCount() {
    // Create 3 spans that all create the same logical edge (same parent node -> same child node)
    List<ZipkinSpanResponse> spans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                "trace1",
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "ns1",
                    "operation_name", "op1",
                    "resource", "res1")),
            // First child span - creates edge from parent to child node
            TestUtils.createSpanWithTags(
                "child1",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            // Second child span - same node metadata, different span ID
            TestUtils.createSpanWithTags(
                "child2",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")),
            // Third child span - same node metadata, different span ID
            TestUtils.createSpanWithTags(
                "child3",
                "trace1",
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "ns2",
                    "operation_name", "op2",
                    "resource", "res2")));

    Graph graph = configuredGraphBuilder.buildFromSpans(spans, Optional.empty());

    // Should have 2 nodes (parent and child - all child spans map to same logical node)
    assertThat(graph.nodes()).hasSize(2);

    // Should have 1 edge with observation count of 3
    assertThat(graph.edges()).hasSize(1);
    Edge edge = graph.edges().get(0);
    assertThat(edge.getMetadata()).isEqualTo(new TreeMap<>(Map.of("operation", "op2")));
    assertThat(edge.getObservedCount()).isEqualTo(3);
  }
}
