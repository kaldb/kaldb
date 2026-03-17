package com.slack.astra.contrib.dashboardsgateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class DashboardsGatewayServiceTest {
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
}
