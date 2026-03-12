package com.slack.astra.contrib.dashboardsgateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DashboardsGatewayRoutingTest {
  private static final DashboardsGatewayRouting ROUTING = new DashboardsGatewayRouting();

  @Test
  void routesClusterMetadataToAstra() {
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/_nodes"));
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/_cluster/state/nodes"));
    // /_nodes sub-paths are operational endpoints that should stay on OpenSearch
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/_nodes/stats"));
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/_nodes/http"));
  }

  @Test
  void routesUserSearchesToAstra() {
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/test/_search"));
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/test/_msearch"));
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/*%3A*/_search"));
    // Root-level variants should also go to Astra
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/_search"));
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/_mapping"));
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/_alias"));
    assertEquals(Upstream.ASTRA, ROUTING.selectUpstream("/_alias/my-alias"));
    // Scroll contexts live in OpenSearch
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/_search/scroll"));
  }

  @Test
  void routesHiddenIndicesToOpenSearch() {
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/.kibana/_search"));
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/.opensearch_dashboards/_mapping"));
  }

  @Test
  void routesAdminApisToOpenSearch() {
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/_cat/indices"));
    assertEquals(Upstream.OPENSEARCH, ROUTING.selectUpstream("/_plugins/_security/api/account"));
  }

  @Test
  void returnsSyntheticSecurityAccountResponse() {
    assertTrue(ROUTING.syntheticResponse("/_plugins/_security/api/account").isPresent());
    assertTrue(ROUTING.syntheticResponse("/test/_search").isEmpty());
  }

  @Test
  void returnsGatewayHealthResponse() {
    assertTrue(ROUTING.syntheticResponse("/_astra/gateway/health").isPresent());
  }
}
