package com.slack.astra.contrib.dashboardsgateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DashboardsGatewayServiceTest {
  @Test
  void durationSortIsAFieldReferenceRatherThanAnEqualityValue() throws Exception {
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"sort":[{"durationInNanos":"desc"},{"durationInNanos":{"order":"asc"}}]}
        """,
                    null));
    assertEquals("desc", query.at("/sort/0/duration").asText());
    assertEquals("asc", query.at("/sort/1/duration/order").asText());
  }

  @Test
  void todayRangeIncludesTheEntireUtcDay() throws Exception {
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"query":{"range":{"startTime":{"gte":"now/d","lte":"now/d"}}}}
        """,
                    null));
    assertEquals(
        86399999,
        query.at("/query/range/_timesinceepoch/lte").asLong()
            - query.at("/query/range/_timesinceepoch/gte").asLong());
  }

  @Test
  void latencyEqualityFiltersUseStoredMicroseconds() throws Exception {
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"query":{"bool":{"filter":[
        {"term":{"traceGroupFields.durationInNanos":{"value":1744624000}}},
        {"terms":{"durationInNanos":[1000,2000]}}]}}}
        """,
                    null));
    assertEquals(1744624, query.at("/query/bool/filter/0/term/duration/value").asDouble());
    assertEquals(1, query.at("/query/bool/filter/1/terms/duration/0").asDouble());
    assertEquals(2, query.at("/query/bool/filter/1/terms/duration/1").asDouble());
  }

  @Test
  void relativeTimeFromNativeTraceListUsesEpochMillis() throws Exception {
    long before = System.currentTimeMillis();
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"query":{"range":{"startTime":{"gte":"now-15m","lte":"now"}}}}
        """,
                    null));
    long from = query.at("/query/range/_timesinceepoch/gte").asLong();
    long to = query.at("/query/range/_timesinceepoch/lte").asLong();
    assertTrue(from >= before - 900000);
    assertTrue(to <= System.currentTimeMillis());
    assertEquals(900000, to - from);
  }

  @Test
  void translatesNativeWaterfallQueryWithoutChangingUserValues() throws Exception {
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"size":3000,"query":{"bool":{"must":[
          {"term":{"traceId":"serviceName"}},
          {"exists":{"field":"serviceName"}},
          {"range":{"startTime":{"gte":"2026-11-15T20:00:00Z","lte":"2026-11-15T20:01:00Z"}}}
        ]}},"sort":[{"startTime":{"order":"desc"}}],"_source":{"includes":["spanId","serviceName"]}}
        """,
                    null));
    assertEquals("serviceName", query.at("/query/bool/must/0/term/trace_id").asText());
    assertEquals("resource.service.name", query.at("/query/bool/must/1/exists/field").asText());
    assertEquals(1794772800000L, query.at("/query/bool/must/2/range/_timesinceepoch/gte").asLong());
    assertEquals("desc", query.at("/sort/0/_timesinceepoch/order").asText());
    assertFalse(query.has("_source"));
  }

  @Test
  void exposesRealSpanIdentityParentServiceAndNanosecondTiming() throws Exception {
    JsonNode result =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.response(
                    """
        {"_shards":{"failed":1},"hits":{"hits":[{"_id":"abc","sort":[1794772800123],
        "_source":{"trace_id":"def","parent_id":"parent","resource.service.name":"checkout",
        "service_name":"otel-demo-traces","name":"PlaceOrder","duration":1250,
        "_timesinceepoch":"2026-11-15T20:00:00.123Z","status.code":"STATUS_CODE_ERROR"}}]}}
        """));
    JsonNode hit = result.at("/hits/hits/0");
    assertEquals("abc", hit.at("/_source/spanId").asText());
    assertEquals("def", hit.at("/_source/traceId").asText());
    assertEquals("parent", hit.at("/_source/parentSpanId").asText());
    assertEquals("checkout", hit.at("/_source/serviceName").asText());
    assertEquals(1250000, hit.at("/_source/durationInNanos").asLong());
    assertEquals("2026-11-15T20:00:00.124250Z", hit.at("/_source/endTime").asText());
    assertEquals(1794772800123000000L, hit.at("/sort/0").asLong());
    assertEquals(2, hit.at("/_source/status.code").asInt());
    assertEquals(1, result.at("/_shards/failed").asInt());
  }

  @Test
  void rootSpansAndMissingDurationRemainUsable() throws Exception {
    JsonNode result =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.response(
                    """
        {"hits":{"hits":[{"_id":"root","_source":{"trace_id":"trace",
        "resource.service.name":"frontend","_timesinceepoch":"2026-11-15T20:00:00Z"}}]}}
        """));
    assertEquals("", result.at("/hits/hits/0/_source/parentSpanId").asText());
    assertFalse(result.at("/hits/hits/0/_source").has("durationInNanos"));
  }

  @Test
  void rootTraceQueriesUseRootMetadataAndKeepPipelineAggregations() throws Exception {
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"size":0,"query":{"bool":{"must":[]}},"aggs":{"traces":{
        "terms":{"field":"traceId"},"aggs":{"trace_group":{"terms":{"field":"traceGroup"}},
        "latency":{"max":{"script":{"source":"doc['traceGroupFields.durationInNanos'].value / 1000000.0"}}},
        "last_updated":{"max":{"field":"traceGroupFields.endTime"}}}}}}
        """,
                    null));
    assertTrue(query.toString().contains("parent_id"));
    assertEquals("name", query.at("/aggs/traces/aggs/trace_group/terms/field").asText());
    assertTrue(query.at("/aggs/traces/aggs/latency/max/script/source").asText().contains("* 1000"));
    assertTrue(
        query
            .at("/aggs/traces/aggs/last_updated/max/script/source")
            .asText()
            .contains("doc['_timesinceepoch'].value +"));
  }

  @Test
  void countsTraceErrorsInKaldbAndRestoresFilterAggregationShape() throws Exception {
    JsonNode query =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.request(
                    """
        {"size":0,"aggs":{"error_count":{"filter":{"term":{"traceGroupFields.statusCode":"2"}}}}}
        """,
                    null));
    assertTrue(
        query.at("/aggs/error_count/sum/script/source").asText().contains("STATUS_CODE_ERROR"));
    JsonNode response =
        new ObjectMapper()
            .readTree(
                TraceAnalyticsAdapter.response(
                    """
        {"aggregations":{"error_count":{"value":3.0}}}
        """));
    assertEquals(3, response.at("/aggregations/error_count/doc_count").asInt());
  }

  @Test
  void forwardsRequestsToExpectedUpstreams() {
    try (GatewayHarness gatewayHarness = new GatewayHarness("")) {
      assertForwardedTo(gatewayHarness.get("/_nodes/astra-query"), "astra", "/_nodes/astra-query");
      assertForwardedTo(
          gatewayHarness.get("/_nodes/astra-query/jvm"), "astra", "/_nodes/astra-query/jvm");
      assertForwardedTo(gatewayHarness.get("/_nodes/stats"), "opensearch", "/_nodes/stats");
      assertForwardedTo(
          gatewayHarness.get("/_mapping?startTimeEpochMs=1&endTimeEpochMs=2"),
          "astra",
          "/_mapping?startTimeEpochMs=1&endTimeEpochMs=2");
      assertForwardedTo(gatewayHarness.get("/_alias/test-alias"), "astra", "/_alias/test-alias");
      assertForwardedTo(gatewayHarness.get("/_alias"), "astra", "/_alias");
    }
  }

  @Test
  void nativeTraceRoutesReachKaldbOnlyWhenEnabled() throws Exception {
    try (GatewayHarness harness = new GatewayHarness("otel-demo-traces")) {
      AggregatedHttpResponse response =
          harness
              .gatewayClient
              .post(
                  "/otel-v1-apm-span-%2A/_search?size=3000",
                  "{\"query\":{\"term\":{\"traceId\":\"def\"}}}")
              .aggregate()
              .join();
      assertEquals(HttpStatus.OK, response.status());
      JsonNode body = new ObjectMapper().readTree(response.contentUtf8());
      assertEquals("astra", body.path("upstream").asText());
      assertEquals("/otel-demo-traces/_search?size=3000", body.path("path").asText());
      assertEquals("def", body.at("/request/query/term/trace_id").asText());
      assertEquals(3000, body.at("/request/size").asInt());
      assertEquals("checkout", body.at("/hits/hits/0/_source/serviceName").asText());
      assertEquals(
          HttpStatus.OK,
          harness
              .gatewayClient
              .head("/otel-v1-apm-span-*,otel-v1-apm-service-map*?allow_no_indices=false")
              .aggregate()
              .join()
              .status());
      assertEquals(
          HttpStatus.BAD_REQUEST,
          harness
              .gatewayClient
              .post("/otel-v1-apm-span-*/_search", "invalid JSON")
              .aggregate()
              .join()
              .status());
    }
    try (GatewayHarness harness = new GatewayHarness("")) {
      assertForwardedTo(
          harness.get("/otel-v1-apm-span-*/_search"), "astra", "/otel-v1-apm-span-*/_search");
      assertForwardedTo(harness.get("/.kibana/_search"), "opensearch", "/.kibana/_search");
    }
  }

  @Test
  void gatewayErrorResponseDoesNotExposeUpstreamDetails() {
    AggregatedHttpResponse response =
        DashboardsGatewayService.gatewayErrorResponse(
                URI.create("http://astra-query.internal:8081"),
                new RuntimeException("connection refused"))
            .aggregate()
            .join();

    String body = response.content(StandardCharsets.UTF_8);
    assertEquals(502, response.status().code());
    assertEquals("gateway upstream request failed\n", body);
    assertFalse(body.contains("astra-query.internal"));
    assertFalse(body.contains("connection refused"));
  }

  @Test
  void requestProcessingErrorResponseDoesNotExposeExceptionDetails() {
    AggregatedHttpResponse response =
        DashboardsGatewayService.requestProcessingErrorResponse(
                "/_search", new IllegalStateException("decoder blew up"))
            .aggregate()
            .join();

    String body = response.content(StandardCharsets.UTF_8);
    assertEquals(502, response.status().code());
    assertEquals("gateway request processing failed\n", body);
    assertFalse(body.contains("/_search"));
    assertFalse(body.contains("decoder blew up"));
  }

  private static void assertForwardedTo(
      AggregatedHttpResponse response, String upstreamName, String expectedPath) {
    assertEquals(HttpStatus.OK, response.status());
    assertEquals(
        upstreamName + " handled " + expectedPath, response.content(StandardCharsets.UTF_8));
  }

  private static final class GatewayHarness implements AutoCloseable {
    private final Server astraServer;
    private final Server openSearchServer;
    private final Server gatewayServer;
    private final WebClient gatewayClient;

    private GatewayHarness(String dataset) {
      astraServer = newUpstreamServer("astra");
      openSearchServer = newUpstreamServer("opensearch");
      astraServer.start().join();
      openSearchServer.start().join();

      GatewayConfig config =
          new GatewayConfig(
              URI.create("http://127.0.0.1:" + astraServer.activeLocalPort()),
              URI.create("http://127.0.0.1:" + openSearchServer.activeLocalPort()),
              "127.0.0.1",
              0,
              Duration.ofSeconds(5),
              1024 * 1024,
              dataset);
      gatewayServer =
          Server.builder().http(0).serviceUnder("/", new DashboardsGatewayService(config)).build();
      gatewayServer.start().join();
      gatewayClient = WebClient.of("http://127.0.0.1:" + gatewayServer.activeLocalPort());
    }

    private AggregatedHttpResponse get(String path) {
      return gatewayClient.get(path).aggregate().join();
    }

    @Override
    public void close() {
      gatewayServer.stop().join();
      openSearchServer.stop().join();
      astraServer.stop().join();
    }

    private static Server newUpstreamServer(String upstreamName) {
      return Server.builder()
          .http(0)
          .service(
              "/otel-demo-traces/_search",
              (ctx, req) ->
                  HttpResponse.from(
                      req.aggregate()
                          .thenApply(
                              request -> {
                                try {
                                  ObjectMapper json = new ObjectMapper();
                                  JsonNode body =
                                      json.readTree(
                                          """
                      {"hits":{"hits":[{"_id":"abc","_source":{"trace_id":"def",
                      "resource.service.name":"checkout","duration":1250,
                      "_timesinceepoch":"2026-11-15T20:00:00Z"}}]}}
                      """);
                                  ((com.fasterxml.jackson.databind.node.ObjectNode) body)
                                      .put("upstream", upstreamName);
                                  ((com.fasterxml.jackson.databind.node.ObjectNode) body)
                                      .put("path", req.path());
                                  ((com.fasterxml.jackson.databind.node.ObjectNode) body)
                                      .set("request", json.readTree(request.contentUtf8()));
                                  return HttpResponse.of(
                                      HttpStatus.OK,
                                      MediaType.JSON_UTF_8,
                                      json.writeValueAsString(body));
                                } catch (Exception e) {
                                  throw new RuntimeException(e);
                                }
                              })))
          .serviceUnder(
              "/",
              (ctx, req) ->
                  HttpResponse.of(
                      HttpStatus.OK,
                      MediaType.PLAIN_TEXT_UTF_8,
                      upstreamName + " handled " + req.path()))
          .build();
    }
  }
}
