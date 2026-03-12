package com.slack.astra.contrib.dashboardsgateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
  void forwardsRequestsToExpectedUpstreams() {
    try (GatewayHarness gatewayHarness = new GatewayHarness()) {
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

    private GatewayHarness() {
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
              1024 * 1024);
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
