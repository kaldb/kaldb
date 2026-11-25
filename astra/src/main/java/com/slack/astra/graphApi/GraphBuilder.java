package com.slack.astra.graphApi;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GraphBuilder constructs service dependency graphs from Zipkin span data.
 *
 * <p>This class processes distributed tracing spans to build a graph representation showing
 * relationships between operations. It creates nodes representing service operations and edges
 * representing parent-child relationships between spans.
 *
 * <p>The builder supports configurable node and edge metadata extraction through GraphConfig,
 * allowing customization of which span tags are used to populate each entity's metadata.
 */
public class GraphBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(GraphBuilder.class);
  private final GraphConfig config;

  /**
   * Constructs a GraphBuilder with the specified configuration.
   *
   * @param config The GraphConfig to use for node metadata extraction. If GraphConfig.DEFAULT, uses
   *     service name from a span's remote endpoint.
   */
  GraphBuilder(GraphConfig config) {
    this.config = config;
  }

  /**
   * Filter for selecting nodes/edges in the graph based on metadata criteria.
   *
   * <p>The filter uses OR logic: a span matches if ANY of the filter criteria match. Each filter
   * option is a field name (e.g., "operation", "service") mapped to a list of allowed values for
   * that field.
   *
   * <p>Examples: {"operation": ["http.request"]} - matches spans with tag operation="http.request"
   * {"operation": ["http.request", "grpc.request"]} - matches spans with either operation tag
   * {"operation": ["http.request"], "kube.namespace": ["test-app-prod"]} - matches spans with
   * operation="http.request" OR kube.namespace="test-app-prod" {} or null - empty filter matches
   * all spans (no filtering)
   *
   * @param options Map of field names to lists of allowed values. If null or empty, all spans
   *     match.
   */
  public record Filter(Map<String, List<String>> options) {
    public boolean matches(ZipkinSpanResponse span) {
      // Empty or null filter means match all spans
      if (options == null || options.isEmpty()) {
        return true;
      }

      // Returns true if ANY filter matches
      return options.entrySet().stream()
          .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
          .anyMatch(
              entry -> {
                String actualValue = span.getTags().get(entry.getKey());
                return actualValue != null && entry.getValue().contains(actualValue);
              });
    }
  }

  /**
   * Builds an (optionally filtered) dependency graph from a list of Zipkin spans.
   *
   * <p>This method processes spans to create nodes and edges representing operation dependencies,
   * filtered by any specified criteria. If a filter is specified, it collects all filtered spans
   * first, then for each filtered span, finds its transitive matching children (filtered spans
   * reachable through non-filtered intermediate spans) and creates edges between them.
   *
   * @param spans List of Zipkin spans to process
   * @param filter Optional filter to apply when building the graph. If empty or null, returns every
   *     connection.
   * @return Graph containing nodes and edges representing operation dependencies
   */
  public Graph buildFromSpans(List<ZipkinSpanResponse> spans, Optional<Filter> filter) {
    // Build all lookup structures
    Map<String, ZipkinSpanResponse> spanIdToSpan = new HashMap<>(); // Lookup a span by span ID
    Map<String, Node> spanIdToNode = new HashMap<>(); // Lookup a span's logical node by span ID
    Map<String, Node> nodeIdToNode = new HashMap<>(); // Lookup a node by node ID
    Set<String> matchingSpanIds = new HashSet<>(); // Spans that match the given filter

    // Convert spans to nodes, creating logical groupings.
    // Multiple spans may map to the same logical node if their metadata is identical.
    spans.stream()
        .filter(span -> span.getId() != null && !span.getId().equals("-1"))
        .forEach(
            span -> {
              spanIdToSpan.put(span.getId(), span);

              Node node =
                  new Node(config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE));
              spanIdToNode.put(span.getId(), node);
              nodeIdToNode.putIfAbsent(node.getId(), node);

              // Add span to matching set if no filter exists or if filter matches
              if (!filter.isPresent() || filter.get().matches(span)) {
                matchingSpanIds.add(span.getId());
              }
            });

    // Build parent-child relationships at the node level
    Map<String, List<Map.Entry<String, ZipkinSpanResponse>>> parentNodeIdToChildNodeIds =
        buildParentChildConnections(spans, spanIdToNode);

    // Determine which nodes to include based on the filter if provided, otherwise include all nodes
    Set<String> nodesToProcess =
        matchingSpanIds.stream()
            .map(spanId -> spanIdToNode.get(spanId).getId())
            .collect(Collectors.toSet());

    return traverseAndBuildGraph(
        matchingSpanIds, nodesToProcess, nodeIdToNode, parentNodeIdToChildNodeIds);
  }

  /**
   * Builds a map of parent-child relationships at the node level.
   *
   * <p>This method aggregates span relationships into node relationships. Multiple spans may
   * represent the same logical node, so this aggregation is crucial for handling siblings. For
   * example, if span S1 and S2 both map to node A, and S1 has child S3 (node B) while S2 has child
   * S4 (node C), the result will be: node A -> [node B, node C].
   *
   * <p>Edge metadata is preserved from the original span connection, representing the actual traced
   * operation that created the relationship.
   *
   * @param spans List of all spans to process
   * @param spanIdToNode Map from span ID to its logical node representation
   * @return Map from parent node ID to list of (child node ID, reference span) pairs
   */
  private Map<String, List<Map.Entry<String, ZipkinSpanResponse>>> buildParentChildConnections(
      List<ZipkinSpanResponse> spans, Map<String, Node> spanIdToNode) {

    Map<String, List<Map.Entry<String, ZipkinSpanResponse>>> parentNodeIdToChildNodeIds =
        new HashMap<>();

    for (ZipkinSpanResponse span : spans) {
      if (span.getId() == null
          || span.getId().equals("-1")
          || span.getParentId() == null
          || span.getParentId().equals("-1")) continue;

      Node parent = spanIdToNode.get(span.getParentId());
      Node child = spanIdToNode.get(span.getId());
      if (parent == null || child == null) continue;

      // Keep a reference to the span that produced this edge. This is used later during traversal
      // to decide which edges to retain when a filter is applied. Without it, if the filter depends
      // on span tags that also define edge metadata, we could end up creating incorrect or
      // missing relationships.
      parentNodeIdToChildNodeIds
          .computeIfAbsent(parent.getId(), k -> new ArrayList<>())
          .add(Map.entry(child.getId(), span));
    }
    return parentNodeIdToChildNodeIds;
  }

  /**
   * Traverses the node graph to build the final filtered graph with transitive edges.
   *
   * <p>For each filtered node, this method performs a depth-first traversal to find all filtered
   * descendant nodes, skipping through non-filtered intermediate nodes. When a filtered descendant
   * is found, an edge is created directly from the starting filtered node to the descendant,
   * preserving the edge metadata from the original connection path.
   *
   * <p>Example: If we have Root (filtered) -> Intermediate (not filtered) -> Leaf (filtered), this
   * will create a direct edge: Root -> Leaf, skipping the intermediate node.
   *
   * @param matchingSpanIds Pre-computed set of span IDs that match the filter (or all span IDs if
   *     no filter)
   * @param nodesToProcess Set of node IDs that match the filter (or all nodes if no filter)
   * @param nodeIdToNode Map from node ID to Node object
   * @param parentNodeIdToChildNodeIds Map of parent-child relationships with their reference span
   * @return Graph containing only filtered nodes and their transitive connections
   */
  private Graph traverseAndBuildGraph(
      Set<String> matchingSpanIds,
      Set<String> nodesToProcess,
      Map<String, Node> nodeIdToNode,
      Map<String, List<Map.Entry<String, ZipkinSpanResponse>>> parentNodeIdToChildNodeIds) {

    Set<Node> nodes = new HashSet<>();
    Map<String, Edge> edges = new HashMap<>();

    // Process each filtered node as a potential parent
    for (String parentNodeId : nodesToProcess) {
      Deque<String> work = new ArrayDeque<>();
      Set<String> visitedNodes = new HashSet<>();

      work.push(parentNodeId);

      while (!work.isEmpty()) {
        String currentNodeId = work.pop();
        if (!visitedNodes.add(currentNodeId)) continue;

        List<Map.Entry<String, ZipkinSpanResponse>> children =
            parentNodeIdToChildNodeIds.getOrDefault(currentNodeId, List.of());

        for (Map.Entry<String, ZipkinSpanResponse> child : children) {
          String childNodeId = child.getKey();
          ZipkinSpanResponse refSpan = child.getValue();
          if (matchingSpanIds.contains(refSpan.getId())) {
            // Skip the case where the ancestor is a direct parent of the same logical node ID
            if (parentNodeId.equals(childNodeId)) continue;

            // Found a child that matches the filter, create edge
            // from starting parent to this child.
            // This creates the transitive edge, skipping any intermediate nodes.
            // Don't traverse past this child - it will be processed in its own iteration.
            nodes.add(nodeIdToNode.get(parentNodeId));
            nodes.add(nodeIdToNode.get(childNodeId));

            SortedMap<String, String> edgeMetadata =
                config.createMetadataFromSpan(refSpan, GraphConfig.EntityType.EDGE);
            String edgeKey = Edge.generateKey(parentNodeId, childNodeId, edgeMetadata);

            edges
                .computeIfAbsent(edgeKey, k -> new Edge(parentNodeId, childNodeId, edgeMetadata))
                .incrementObservedCount();
          } else {
            // Non-filtered intermediate node - continue traversing through it
            work.push(childNodeId);
          }
        }
      }
    }

    return new Graph(new ArrayList<>(nodes), new ArrayList<>(edges.values()));
  }
}
