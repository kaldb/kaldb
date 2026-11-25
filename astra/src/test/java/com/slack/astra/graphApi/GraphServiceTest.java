package com.slack.astra.graphApi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.zipkinApi.TraceFetcher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class GraphServiceTest {
  @Mock private TraceFetcher traceFetcher;
  private GraphService graphService;
  private ObjectMapper objectMapper;
  private MeterRegistry meterRegistry;

  @BeforeEach
  public void setup() throws IOException {
    MockitoAnnotations.openMocks(this);

    // Load custom config from YAML file
    Path configPath =
        new File(
                Objects.requireNonNull(
                        getClass()
                            .getClassLoader()
                            .getResource("test-dependency-graph-config.yaml"))
                    .getFile())
            .toPath();
    meterRegistry = new SimpleMeterRegistry();
    graphService = spy(new GraphService(traceFetcher, GraphConfig.load(configPath), meterRegistry));
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testGetSubgraphByTraceId_emptyResult() throws Exception {
    String traceId = "test_trace_empty";

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(Collections.emptyList());

    // Verify timers start at 0
    assertEquals(0.0, MetricsUtil.getTimerCount("astra_graph_service_trace_fetch", meterRegistry));
    assertEquals(0.0, MetricsUtil.getTimerCount("astra_graph_service_graph_build", meterRegistry));

    HttpResponse response =
        graphService.getSubgraph(
            traceId,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());

    // Verify it's valid JSON with nodes and edges arrays
    String content = aggregatedResponse.contentUtf8();
    JsonNode jsonNode = objectMapper.readTree(content);

    assertTrue(jsonNode.has("subgraph"));
    assertTrue(jsonNode.has("traceFetchTimeMs"));
    assertTrue(jsonNode.has("subgraphBuildTimeMs"));

    assertTrue(jsonNode.get("subgraph").isEmpty());

    // Verify both timers have been recorded once
    assertEquals(1.0, MetricsUtil.getTimerCount("astra_graph_service_trace_fetch", meterRegistry));
    assertEquals(1.0, MetricsUtil.getTimerCount("astra_graph_service_graph_build", meterRegistry));
  }

  @Test
  public void testGetSubgraphByTraceId_withComplexHierarchy() throws Exception {
    String traceId = "test_trace_complex";

    // Create a complex span hierarchy:
    // root (API Gateway)
    //   ├── child1 (Auth Service)
    //   ├── child2 (User Service)
    //   │   ├── grandchild1 (Database)
    //   │   └── grandchild2 (Cache Service)
    //   └── child3 (Notification Service)
    //       └── grandchild3 (Email Service)
    List<com.slack.astra.zipkinApi.ZipkinSpanResponse> testSpans =
        List.of(
            // Root span - API Gateway
            TestUtils.createSpanWithTags(
                "root",
                traceId,
                null,
                Map.of(
                    "kube.app",
                    "api-gateway",
                    "kube.namespace",
                    "prod",
                    "operation_name",
                    "http.request",
                    "resource",
                    "some_resource",
                    "tag.http.target.canonical_path",
                    "/api/users/profile",
                    "tag.http.target.host",
                    "app1.ns1")),

            // First level children
            TestUtils.createSpanWithTags(
                "child1",
                traceId,
                "root",
                Map.of(
                    "kube.app",
                    "auth-service",
                    "kube.namespace",
                    "prod",
                    "operation_name",
                    "http.request",
                    "resource",
                    "some_resource2",
                    "tag.http.target.canonical_path",
                    "/api/auth/validate",
                    "tag.http.target.host",
                    "app2.ns2")),
            TestUtils.createSpanWithTags(
                "child2",
                traceId,
                "root",
                Map.of(
                    "kube.app", "user-service",
                    "kube.namespace", "prod",
                    "operation_name", "user.fetch",
                    "resource", "getUserProfile")),
            TestUtils.createSpanWithTags(
                "child3",
                traceId,
                "root",
                Map.of(
                    "kube.app", "notification-service",
                    "kube.namespace", "prod",
                    "operation_name", "notify.send",
                    "resource", "sendNotification")),

            // Second level children (grandchildren)
            TestUtils.createSpanWithTags(
                "grandchild1",
                traceId,
                "child2",
                Map.of(
                    "kube.app", "postgres-db",
                    "kube.namespace", "prod",
                    "operation_name", "db.query",
                    "resource", "SELECT * FROM users WHERE id = ?")),
            TestUtils.createSpanWithTags(
                "grandchild2",
                traceId,
                "child2",
                Map.of(
                    "kube.app", "redis-cache",
                    "kube.namespace", "prod",
                    "operation_name", "cache.get",
                    "resource", "user:profile:12345")),
            TestUtils.createSpanWithTags(
                "grandchild3",
                traceId,
                "child3",
                Map.of(
                    "kube.app", "email-service",
                    "kube.namespace", "prod",
                    "operation_name", "email.send",
                    "resource", "sendEmail")));

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(testSpans);

    HttpResponse response =
        graphService.getSubgraph(
            traceId,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());

    // Verify it's valid JSON with proper structure
    String content = aggregatedResponse.contentUtf8();
    JsonNode jsonNode = objectMapper.readTree(content);

    // Verify structure
    assertTrue(jsonNode.has("subgraph"));
    assertTrue(jsonNode.has("traceFetchTimeMs"));
    assertTrue(jsonNode.has("subgraphBuildTimeMs"));

    JsonNode subgraph = jsonNode.get("subgraph");
    assertTrue(subgraph.has("nodes"));
    assertTrue(subgraph.has("edges"));

    // Verify data content
    JsonNode nodes = subgraph.get("nodes");
    JsonNode edges = subgraph.get("edges");

    assertEquals(7, nodes.size(), "Should have 7 nodes (1 root + 3 children + 3 grandchildren)");
    assertEquals(
        6,
        edges.size(),
        "Should have 6 edges (3 root->child + 2 child2->grandchild + 1 child3->grandchild)");

    // Verify all nodes have required fields
    for (JsonNode node : nodes) {
      assertTrue(node.has("id"));
      assertTrue(node.has("metadata"));

      JsonNode metadata = node.get("metadata");
      assertTrue(metadata.has("service"));
      assertTrue(metadata.has("resource"));
    }

    // Verify all edges have required fields
    for (JsonNode edge : edges) {
      assertTrue(edge.has("sourceNodeId"));
      assertTrue(edge.has("targetNodeId"));

      JsonNode metadata = edge.get("metadata");
      assertTrue(metadata.has("operation"));
    }
  }

  @Test
  public void testGetSubgraphByTraceId_withEmptyFilter() throws Exception {
    String traceId = "test_trace_empty_filter";

    List<com.slack.astra.zipkinApi.ZipkinSpanResponse> testSpans =
        List.of(
            TestUtils.createSpanWithTags(
                "parent1",
                traceId,
                null,
                Map.of(
                    "kube.app", "app1",
                    "kube.namespace", "prod",
                    "operation_name", "http.request",
                    "resource", "resource1")),
            TestUtils.createSpanWithTags(
                "child1",
                traceId,
                "parent1",
                Map.of(
                    "kube.app", "app2",
                    "kube.namespace", "prod",
                    "operation_name", "db.query",
                    "resource", "resource2")));

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(testSpans);

    // Empty filter JSON - should return all nodes (no filtering applied)
    String emptyFilterJson = "{}";

    HttpResponse response =
        graphService.getSubgraph(
            traceId,
            Optional.empty(),
            Optional.empty(),
            Optional.of(emptyFilterJson),
            Optional.empty(),
            Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());

    String content = aggregatedResponse.contentUtf8();
    JsonNode jsonNode = objectMapper.readTree(content);

    JsonNode subgraph = jsonNode.get("subgraph");
    JsonNode nodes = subgraph.get("nodes");
    JsonNode edges = subgraph.get("edges");

    // Empty filter should return all nodes and edges
    assertEquals(2, nodes.size(), "Empty filter should return all 2 nodes");
    assertEquals(1, edges.size(), "Empty filter should return all 1 edge");
  }

  @Test
  public void testGetSubgraphByTraceId_withInvalidFilterJson() throws Exception {
    String traceId = "test_trace_invalid_filter";

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(List.of());

    // Invalid JSON - missing closing brace
    String invalidFilterJson = "{\"operation_name\":[\"http.request\"";

    HttpResponse response =
        graphService.getSubgraph(
            traceId,
            Optional.empty(),
            Optional.empty(),
            Optional.of(invalidFilterJson),
            Optional.empty(),
            Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.BAD_REQUEST, aggregatedResponse.status());
    String content = aggregatedResponse.contentUtf8();
    assertTrue(content.contains("Invalid buildFilter JSON"));
  }

  @Test
  public void testGetSubgraphByTraceId_withMalformedFilterStructure() throws Exception {
    String traceId = "test_trace_malformed_filter";

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(List.of());

    // Valid JSON but wrong structure - should be Map<String, List<String>> not Map<String, String>
    String malformedFilterJson = "{\"operation_name\":\"http.request\"}";

    HttpResponse response =
        graphService.getSubgraph(
            traceId,
            Optional.empty(),
            Optional.empty(),
            Optional.of(malformedFilterJson),
            Optional.empty(),
            Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.BAD_REQUEST, aggregatedResponse.status());
    String content = aggregatedResponse.contentUtf8();
    assertTrue(content.contains("Invalid buildFilter JSON"));
  }

  @Test
  public void testGetSubgraphByTraceId_withValidFilter() throws Exception {
    String traceId = "test_trace_with_filter";

    // Create spans with different operations
    List<com.slack.astra.zipkinApi.ZipkinSpanResponse> testSpans =
        List.of(
            // Root span with http.request
            TestUtils.createSpanWithTags(
                "root",
                traceId,
                null,
                Map.of(
                    "kube.app",
                    "api-gateway",
                    "kube.namespace",
                    "prod",
                    "operation_name",
                    "http.request",
                    "resource",
                    "some_resource",
                    "tag.http.target.canonical_path",
                    "/api/endpoint",
                    "tag.http.target.host",
                    "gateway.prod")),
            // Child with http.request
            TestUtils.createSpanWithTags(
                "child1",
                traceId,
                "root",
                Map.of(
                    "kube.app",
                    "service1",
                    "kube.namespace",
                    "prod",
                    "operation_name",
                    "http.request",
                    "resource",
                    "some_resource2",
                    "tag.http.target.canonical_path",
                    "/api/service1",
                    "tag.http.target.host",
                    "service1.prod")),
            // Child with grpc.request
            TestUtils.createSpanWithTags(
                "child2",
                traceId,
                "root",
                Map.of(
                    "kube.app", "service2",
                    "kube.namespace", "prod",
                    "operation_name", "grpc.request",
                    "resource", "grpcMethod")),
            // Child with db.query (should be filtered out)
            TestUtils.createSpanWithTags(
                "child3",
                traceId,
                "root",
                Map.of(
                    "kube.app", "database",
                    "kube.namespace", "prod",
                    "operation_name", "db.query",
                    "resource", "SELECT * FROM users")));

    when(traceFetcher.getSpansByTraceId(
            anyString(),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class),
            any(Optional.class)))
        .thenReturn(testSpans);

    // Filter to only include http.request and grpc.request operations
    String filterJson = "{\"operation_name\":[\"http.request\",\"grpc.request\"]}";

    HttpResponse response =
        graphService.getSubgraph(
            traceId,
            Optional.empty(),
            Optional.empty(),
            Optional.of(filterJson),
            Optional.empty(),
            Optional.empty());
    AggregatedHttpResponse aggregatedResponse = response.aggregate().join();

    assertEquals(HttpStatus.OK, aggregatedResponse.status());

    String content = aggregatedResponse.contentUtf8();
    JsonNode jsonNode = objectMapper.readTree(content);

    JsonNode subgraph = jsonNode.get("subgraph");
    JsonNode nodes = subgraph.get("nodes");
    JsonNode edges = subgraph.get("edges");

    // Should have 3 nodes (root, child1, child2) - child3 with db.query is filtered out
    assertEquals(3, nodes.size(), "Should have 3 nodes after filtering");
    // Should have 2 edges (root->child1, root->child2)
    assertEquals(2, edges.size(), "Should have 2 edges after filtering");
  }
}
