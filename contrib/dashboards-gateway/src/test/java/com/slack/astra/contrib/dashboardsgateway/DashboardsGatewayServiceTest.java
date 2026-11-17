package com.slack.astra.contrib.dashboardsgateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.Server;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DashboardsGatewayServiceTest {
  @TempDir Path temporaryDirectory;

  private static GatewayAuthorization.Dataset largeDataset() {
    var mapping = AuthorizedSearch.object("{\"mappings\":{\"properties\":{}}}");
    var caps = AuthorizedSearch.object("{\"fields\":{}}");
    Set<String> names = new java.util.LinkedHashSet<>();
    for (int i = 0; i < 400; i++) {
      String name = "field_" + i;
      names.add(name);
      ((com.fasterxml.jackson.databind.node.ObjectNode) mapping.path("mappings").path("properties"))
          .putObject(name)
          .put("type", "text");
      ((com.fasterxml.jackson.databind.node.ObjectNode) caps.get("fields"))
          .putObject(name)
          .putObject("text")
          .put("type", "text");
    }
    return new GatewayAuthorization.Dataset(names, mapping, caps);
  }

  @Test
  void boundsAdversarialWildcardAndSelectorWork() {
    org.junit.jupiter.api.Assertions.assertThrows(
        GatewayAuthorization.Rejected.class,
        () -> AuthorizedSearch.resolve("a".repeat(2049), Set.of("public")));
    org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
        Duration.ofMillis(500),
        () ->
            assertEquals(
                Set.of(),
                AuthorizedSearch.resolve("************z", Set.of("permissions-demo-public"))));
    var dataset = largeDataset();
    var selectors = GatewayAuthorization.JSON.createArrayNode();
    for (int i = 0; i < 2049; i++) selectors.add("field_0");
    org.junit.jupiter.api.Assertions.assertThrows(
        GatewayAuthorization.Rejected.class, () -> AuthorizedSearch.select(selectors, dataset));
    org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
        Duration.ofMillis(500),
        () ->
            org.junit.jupiter.api.Assertions.assertThrows(
                GatewayAuthorization.Rejected.class,
                () ->
                    AuthorizedSearch.select(
                        GatewayAuthorization.JSON.getNodeFactory().textNode("************z"),
                        dataset)));
  }

  @Test
  void boundsAggregateUnfieldedQueryExpansion() {
    var dataset = largeDataset();
    var input = AuthorizedSearch.object("{\"query\":{\"bool\":{\"should\":[]}}}");
    var clauses =
        (com.fasterxml.jackson.databind.node.ArrayNode)
            input.path("query").path("bool").get("should");
    clauses.add(AuthorizedSearch.object("{\"query_string\":{\"query\":\"alpha\"}}"));
    assertEquals(
        400,
        AuthorizedSearch.search(input, dataset)
            .path("query")
            .path("bool")
            .path("should")
            .get(0)
            .path("bool")
            .path("should")
            .size());
    for (int i = 1; i < 12; i++) clauses.add(clauses.get(0).deepCopy());
    org.junit.jupiter.api.Assertions.assertThrows(
        GatewayAuthorization.Rejected.class, () -> AuthorizedSearch.search(input, dataset));
  }

  @Test
  void rejectsIncompleteNativePermissionEntries() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      var baseline = AuthorizedSearch.object(h.roles);
      for (String required :
          new String[] {"index_patterns", "allowed_actions", "fls", "masked_fields"}) {
        var broken = baseline.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode)
                broken.path("reader").path("index_permissions").get(0))
            .remove(required);
        h.roles = broken.toString();
        assertEquals(503, h.search(SecureHarness.USER).status().code(), required);
      }
      for (String required : new String[] {"cluster_permissions", "tenant_permissions"}) {
        var broken = baseline.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) broken.get("reader")).remove(required);
        h.roles = broken.toString();
        assertEquals(503, h.search(SecureHarness.USER).status().code(), required);
      }
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void rejectsUnsupportedResultSemanticsBeforeKaldb() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      java.util.List<Integer> statuses = new java.util.ArrayList<>();
      for (String body :
          new String[] {
            "{\"post_filter\":{\"match_none\":{}}}",
            "{\"query\":{\"query_string\":{\"query\":\"alpha beta\",\"default_operator\":\"AND\"}}}"
          }) statuses.add(h.request("/public/_search", body).status().code());
      assertEquals(java.util.List.of(400, 400), statuses);
      assertEquals(
          400,
          h.request(
                  "/_msearch",
                  "{\"index\":\"public\"}\n{}\n{\"index\":\"public\"}\n{\"post_filter\":{\"match_none\":{}}}\n")
              .status()
              .code());
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void authorizesUrlFieldSelectorsAndFiltersCapsSubset() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      var caps = h.request("/public/_field_caps?fields=body", "{}");
      var search = h.request("/public/_search?fields=secret", "{}");
      h.caps = "{\"indices\":[\"public\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}}}}";
      var hidden = h.request("/public/_field_caps?fields=secret", "{}");
      org.junit.jupiter.api.Assertions.assertAll(
          () -> assertFalse(caps.contentUtf8().contains("secret")),
          () -> assertEquals(400, search.status().code()),
          () -> assertEquals(403, hidden.status().code()),
          () -> assertEquals(0, h.calls.get()));
    }
  }

  @Test
  void deniesEveryExplicitHiddenFieldReferenceBeforeKaldb() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.caps = "{\"indices\":[\"public\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}}}}";
      for (String body :
          new String[] {
            "{\"query\":{\"term\":{\"secret\":\"PRIVATE\"}}}",
            "{\"query\":{\"exists\":{\"field\":\"secret\"}}}",
            "{\"query\":{\"range\":{\"secret\":{\"gte\":1}}}}",
            "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"secret\":\"PRIVATE\"}}]}}}",
            "{\"sort\":[{\"secret\":\"asc\"}]}",
            "{\"aggs\":{\"values\":{\"terms\":{\"field\":\"secret\"}}}}",
            "{\"_source\":[\"secret\"]}",
            "{\"_source\":[\"sec*\"]}",
            "{\"_source\":[\"customer.*\"]}",
            "{\"fields\":[\"secret\"]}",
            "{\"docvalue_fields\":[{\"field\":\"secret\"}]}",
            "{\"query\":{\"query_string\":{\"query\":\"secret:PRIVATE\"}}}"
          }) {
        assertEquals(403, h.request("/public/_search", body).status().code(), body);
      }
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void rejectsMixedMsearchAtomicallyAndMalformedResourceShapes() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.caps = "{\"indices\":[\"public\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}}}}";
      String mixed =
          "{\"index\":\"public\"}\n{\"query\":{\"match_all\":{}}}\n{\"index\":\"public\"}\n{\"query\":{\"term\":{\"secret\":\"PRIVATE\"}}}\n";
      assertEquals(403, h.request("/_msearch", mixed).status().code());
      assertEquals(400, h.request("/_msearch", "{}\n{}\n").status().code());
      assertEquals(400, h.request("/_msearch", "{\"index\":\"public\"}\n{}").status().code());
      assertEquals(
          403, h.request("/public/_msearch", "{\"index\":\"private\"}\n{}\n").status().code());
      h.probeStatus = 403;
      assertEquals(403, h.request("/priva%74e/_search", "{}").status().code());
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void onlyNativeDashboardsServiceCanUseSavedObjectScroll() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      assertEquals(403, h.request("/_search/scroll/context", "{}").status().code());
      h.serviceIdentity = true;
      var response = h.request("/_search/scroll/context", "{}");
      assertEquals(200, response.status().code());
      org.junit.jupiter.api.Assertions.assertTrue(response.contentUtf8().contains("native_scroll"));
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void routesOnlySavedObjectMultiGetToNative() {
    var request =
        AggregatedHttpRequest.of(
            HttpMethod.POST,
            "/_mget",
            MediaType.JSON_UTF_8,
            "{\"docs\":[{\"_index\":\".kibana\",\"_id\":\"index-pattern:demo\"}]}");
    assertEquals(
        AuthorizedSearch.Kind.NATIVE, AuthorizedSearch.inspect(request, Set.of("public")).kind());
    var mixed =
        AggregatedHttpRequest.of(
            HttpMethod.POST,
            "/_mget",
            MediaType.JSON_UTF_8,
            "{\"docs\":[{\"_index\":\".kibana\",\"_id\":\"demo\"},{\"_index\":\"public\",\"_id\":\"1\"}]}");
    org.junit.jupiter.api.Assertions.assertThrows(
        GatewayAuthorization.Rejected.class,
        () -> AuthorizedSearch.inspect(mixed, Set.of("public")));
  }

  @Test
  void acceptsDashboardsEmptyScriptFieldsWithoutEnablingScripts() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      assertEquals(
          200,
          h.request(
                  "/public/_search",
                  "{\"script_fields\":{},\"stored_fields\":[\"*\"],\"_source\":{\"excludes\":[]},\"aggs\":{}}")
              .status()
              .code());
      assertFalse(h.lastAstra.contentUtf8().contains("script_fields"));
      int before = h.calls.get();
      assertEquals(
          400,
          h.request("/public/_search", "{\"script_fields\":{\"x\":{\"script\":\"1\"}}}")
              .status()
              .code());
      assertEquals(before, h.calls.get());
    }
  }

  @Test
  void acceptsDashboardsSearchTransportParameters() {
    assertEquals(
        AuthorizedSearch.Kind.SEARCH,
        AuthorizedSearch.inspect(
                AggregatedHttpRequest.of(
                    HttpMethod.POST,
                    "/public/_search?ignore_unavailable=true&track_total_hits=true&timeout=30000ms&preference=123&pretty=true",
                    MediaType.JSON_UTF_8,
                    "{}"),
                Set.of("public"))
            .kind());
  }

  @Test
  void supportsNativeDashboardsBootstrapWithoutRoutingToKaldb() {
    for (String path :
        new String[] {
          "/_alias/.kibana",
          "/_template/.kibana",
          "/_cluster/health/.kibana",
          "/_cat/templates/opensearch_dashboards_index_template*",
          "/_index_template/opensearch_dashboards_index_template",
          "/_aliases"
        }) {
      var plan =
          AuthorizedSearch.inspect(
              AggregatedHttpRequest.of(HttpMethod.GET, path), Set.of("public"));
      assertEquals(AuthorizedSearch.Kind.NATIVE, plan.kind(), path);
    }
  }

  @Test
  void rejectsSchemaChangesDuringAuthorization() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.afterMapping =
          "{\"public\":{\"mappings\":{\"properties\":{\"body\":{\"type\":\"text\"}}}}}";
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void rejectsContainerValuesInScalarResponseFields() {
    var mapping =
        GatewayAuthorization.parse(
            "{\"mappings\":{\"properties\":{\"body\":{\"type\":\"text\"}}}}"
                .getBytes(StandardCharsets.UTF_8));
    var dataset = new GatewayAuthorization.Dataset(Set.of("body"), mapping, mapping);
    var response =
        GatewayAuthorization.parse(
            "{\"hits\":{\"hits\":[{\"_index\":\"public\",\"fields\":{\"body\":[{\"secret\":\"PRIVATE\"}]}}]}}"
                .getBytes(StandardCharsets.UTF_8));
    org.junit.jupiter.api.Assertions.assertThrows(
        GatewayAuthorization.Rejected.class,
        () -> AuthorizedSearch.filteredSearch(response, "public", dataset));
  }

  @Test
  void filtersMetadataAndRejectsUnsupportedQueriesWithoutKaldbCalls() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.caps = "{\"indices\":[\"public\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}}}}";
      for (String path : new String[] {"/public/_mapping", "/public/_field_caps?fields=*"}) {
        var response = h.request(path, "{}");
        assertEquals(200, response.status().code());
        assertFalse(response.contentUtf8().contains("secret"));
      }
      for (String body :
          new String[] {
            "{\"script_fields\":{\"x\":{\"script\":\"1\"}}}",
            "{\"runtime_mappings\":{}}",
            "{\"query\":{\"script\":{}}}",
            "{\"query\":{\"query_string\":{\"query\":\"body:foo OR secret:bar\"}}}",
            "{\"query\":{\"bool\":{\"unknown\":[]}}}",
            "{\"query\":{\"match_all\":{}},\"query\":{}}"
          }) {
        assertEquals(400, h.request("/public/_search", body).status().code());
      }
      assertEquals(400, h.request("/public/_search?q=secret:PRIVATE", "{}").status().code());
      assertEquals(403, h.request("/_search/scroll", "{}").status().code());
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void projectsVisibleFieldsAndDoesNotSearchHiddenValues() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.caps = "{\"indices\":[\"public\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}}}}";
      h.astraResponse =
          "{\"hits\":{\"total\":{\"value\":1},\"hits\":[{\"_index\":\"public\",\"_source\":{\"body\":\"PUBLIC\",\"secret\":\"PRIVATE\",\"unknown\":\"PRIVATE\"},\"fields\":{\"body\":[\"PUBLIC\"],\"secret\":[\"PRIVATE\"]}}]}}";
      var response =
          h.request("/public/_search", "{\"query\":{\"query_string\":{\"query\":\"PRIVATE\"}}}");
      assertEquals(200, response.status().code());
      assertFalse(response.contentUtf8().contains("PRIVATE"));
      assertFalse(h.lastAstra.contentUtf8().contains("secret"));
      assertFalse(h.lastAstra.contentUtf8().contains("query_string"));
      org.junit.jupiter.api.Assertions.assertTrue(h.lastAstra.contentUtf8().contains("body"));
      h.astraResponse =
          "{\"hits\":{\"hits\":[{\"_index\":\"private\",\"_source\":{\"secret\":\"PRIVATE\"}}]}}";
      assertEquals(503, h.request("/public/_search", "{}").status().code());
    }
  }

  @Test
  void rejectsUnsupportedFlsPolicyFormsAndParentExclusions() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      for (String rule : new String[] {"secret", "~sec*", "~/secret/", "~customer"}) {
        var role = AuthorizedSearch.object(SecureHarness.ROLES);
        ((com.fasterxml.jackson.databind.node.ObjectNode)
                role.path("reader").path("index_permissions").get(0))
            .putArray("fls")
            .add(rule);
        h.roles = role.toString();
        h.mapping =
            "{\"public\":{\"mappings\":{\"properties\":{\"body\":{\"type\":\"text\"},\"secret\":{\"type\":\"keyword\"},\"customer\":{\"properties\":{\"region\":{\"type\":\"keyword\"}}}}}}}";
        assertEquals(403, h.search(SecureHarness.USER).status().code(), rule);
      }
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void securedRequestsAuthenticateBeforeContactingKaldb() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      assertEquals(401, h.search(null).status().code());
      assertEquals(401, h.search("Basic invalid").status().code());
      assertEquals(0, h.calls.get());
      assertEquals(200, h.search(SecureHarness.USER).status().code());
      assertEquals(1, h.calls.get());
    }
  }

  @Test
  void securedRequestsFailClosedOnIndexAndPolicyFailures() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.probeStatus = 403;
      assertEquals(403, h.search(SecureHarness.USER).status().code());
      h.probeStatus = 404;
      assertEquals(403, h.search(SecureHarness.USER).status().code());
      h.probeStatus = 200;
      h.policyStatus = 503;
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      h.policyStatus = 200;
      h.roles = "{}";
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      h.roles = "not-json";
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void rejectsDlsAndMaskingEvenOnUnrelatedEffectiveRoleIndexes() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      for (String policy :
          new String[] {"\"dls\":\"{\\\"match_all\\\":{}}\"", "\"masked_fields\":[\"secret\"]"}) {
        var role = AuthorizedSearch.object(SecureHarness.ROLES);
        var permission =
            (com.fasterxml.jackson.databind.node.ObjectNode)
                role.path("reader").path("index_permissions").get(0);
        permission.putArray("index_patterns").add("unrelated");
        permission.setAll(AuthorizedSearch.object("{" + policy + "}"));
        h.roles = role.toString();
        assertEquals(403, h.search(SecureHarness.USER).status().code());
      }
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void rejectsMalformedAndPartialNativeProbesWithoutKaldbCalls() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      h.probeBody = "{\"_shards\":{\"successful\":1,\"failed\":1}}";
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      h.probeBody = SecureHarness.PROBE;
      h.caps = "{}";
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      h.caps = "{\"indices\":[\"other\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}}}}";
      assertEquals(503, h.search(SecureHarness.USER).status().code());
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void rejectsMultifieldAndAliasSchemasAsAWhole() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      for (String mapping :
          new String[] {
            "{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}}",
            "{\"type\":\"alias\",\"path\":\"secret\"}"
          }) {
        h.mapping = "{\"public\":{\"mappings\":{\"properties\":{\"body\":" + mapping + "}}}}";
        assertEquals(403, h.search(SecureHarness.USER).status().code());
      }
      assertEquals(0, h.calls.get());
    }
  }

  @Test
  void revocationAppliesToNextRequestAndSpoofedHeadersAreNotAuthoritative() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      assertEquals(200, h.search(SecureHarness.USER).status().code());
      assertEquals(1, h.calls.get());
      assertFalse(h.lastAstra.headers().contains("authorization"));
      assertFalse(h.lastAstra.headers().contains("x-proxy-user"));
      assertFalse(h.lastAstra.headers().contains("_opendistro_security_user"));
      h.probeStatus = 403;
      assertEquals(403, h.search(SecureHarness.USER).status().code());
      assertEquals(1, h.calls.get());
      assertFalse(h.spoofSeen);
    }
  }

  @Test
  void securedAccountEndpointUsesRealNativePrincipal() throws Exception {
    try (SecureHarness h = new SecureHarness(temporaryDirectory)) {
      AggregatedHttpResponse account =
          h.client
              .execute(
                  HttpRequest.of(
                      RequestHeaders.builder(HttpMethod.GET, "/_plugins/_security/api/account")
                          .set("authorization", SecureHarness.USER)
                          .build()))
              .aggregate()
              .join();
      assertEquals(200, account.status().code());
      assertEquals("{\"user_name\":\"actual-reader\"}", account.contentUtf8());
      assertEquals(0, h.calls.get());
    }
  }

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
              1024 * 1024,
              null);
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

  private static final class SecureHarness implements AutoCloseable {
    static final String USER = "Basic dXNlcjpwYXNz";
    static final String POLICY = "Basic cG9saWN5OnRlc3Q=";
    static final String PROBE =
        "{\"_shards\":{\"total\":1,\"successful\":1,\"failed\":0},\"timed_out\":false,\"hits\":{\"total\":{\"value\":0}}}";
    final AtomicInteger calls = new AtomicInteger();
    final Server astra;
    final Server nativeServer;
    final Server gateway;
    final WebClient client;
    volatile int probeStatus = 200;
    volatile int policyStatus = 200;
    static final String ROLES =
        "{\"reader\":{\"cluster_permissions\":[],\"tenant_permissions\":[],\"index_permissions\":[{\"index_patterns\":[\"public\"],\"allowed_actions\":[\"read\"],\"fls\":[],\"masked_fields\":[]}]}}";
    volatile String roles = ROLES;
    volatile String probeBody = PROBE;
    volatile String caps =
        "{\"indices\":[\"public\"],\"fields\":{\"body\":{\"text\":{\"type\":\"text\"}},\"secret\":{\"keyword\":{\"type\":\"keyword\"}}}}";
    volatile String mapping =
        "{\"public\":{\"mappings\":{\"properties\":{\"body\":{\"type\":\"text\"},\"secret\":{\"type\":\"keyword\"}}}}}";
    volatile AggregatedHttpRequest lastAstra;
    final AtomicInteger mappingCalls = new AtomicInteger();
    volatile String afterMapping;
    volatile String astraResponse = "{\"hits\":{\"total\":{\"value\":0},\"hits\":[]}}";
    volatile boolean spoofSeen;
    volatile boolean serviceIdentity;

    SecureHarness(Path directory) throws Exception {
      astra =
          Server.builder()
              .http(0)
              .serviceUnder(
                  "/",
                  (ctx, req) ->
                      HttpResponse.from(
                          req.aggregate()
                              .thenApply(
                                  r -> {
                                    calls.incrementAndGet();
                                    lastAstra = r;
                                    return HttpResponse.of(
                                        HttpStatus.OK, MediaType.JSON_UTF_8, astraResponse);
                                  })))
              .build();
      nativeServer =
          Server.builder()
              .http(0)
              .serviceUnder(
                  "/",
                  (ctx, req) -> {
                    spoofSeen |=
                        req.headers().contains("x-proxy-user")
                            || req.headers().contains("_opendistro_security_user");
                    String auth = req.headers().get("authorization");
                    if (ctx.path().startsWith("/_plugins/_security/api/roles")) {
                      return HttpResponse.of(
                          HttpStatus.valueOf(POLICY.equals(auth) ? policyStatus : 403),
                          MediaType.JSON_UTF_8,
                          roles);
                    }
                    if (ctx.path().endsWith("/_mapping") && POLICY.equals(auth)) {
                      int read = mappingCalls.incrementAndGet();
                      return HttpResponse.of(
                          MediaType.JSON_UTF_8,
                          read > 1 && afterMapping != null ? afterMapping : mapping);
                    }
                    if (!USER.equals(auth)) return HttpResponse.of(HttpStatus.UNAUTHORIZED);
                    if (ctx.path().equals("/_plugins/_security/authinfo"))
                      return HttpResponse.of(
                          MediaType.JSON_UTF_8,
                          "{\"user_name\":\"actual-reader\",\"roles\":[\""
                              + (serviceIdentity ? "kibana_server" : "reader")
                              + "\"],\"backend_roles\":[]}");
                    if (ctx.path().startsWith("/_search/scroll"))
                      return HttpResponse.of(MediaType.JSON_UTF_8, "{\"native_scroll\":true}");
                    if (ctx.path().equals("/_plugins/_security/api/account"))
                      return HttpResponse.of(
                          MediaType.JSON_UTF_8, "{\"user_name\":\"actual-reader\"}");
                    if (ctx.path().endsWith("/_search"))
                      return HttpResponse.of(
                          HttpStatus.valueOf(probeStatus), MediaType.JSON_UTF_8, probeBody);
                    if (ctx.path().endsWith("/_field_caps"))
                      return HttpResponse.of(MediaType.JSON_UTF_8, caps);
                    if (ctx.path().endsWith("/_mapping"))
                      return HttpResponse.of(MediaType.JSON_UTF_8, mapping);
                    return HttpResponse.of(HttpStatus.NOT_FOUND);
                  })
              .build();
      astra.start().join();
      nativeServer.start().join();
      Path credentials = directory.resolve("policy.json");
      Files.writeString(credentials, "{\"username\":\"policy\",\"password\":\"test\"}");
      GatewayConfig config =
          new GatewayConfig(
              URI.create("http://127.0.0.1:" + astra.activeLocalPort()),
              URI.create("http://127.0.0.1:" + nativeServer.activeLocalPort()),
              "127.0.0.1",
              0,
              Duration.ofSeconds(3),
              1024 * 1024,
              new GatewayConfig.Security(credentials, null, Set.of("public", "private")));
      gateway =
          Server.builder().http(0).serviceUnder("/", new DashboardsGatewayService(config)).build();
      gateway.start().join();
      client = WebClient.of("http://127.0.0.1:" + gateway.activeLocalPort());
    }

    AggregatedHttpResponse search(String authorization) {
      var headers =
          RequestHeaders.builder(HttpMethod.POST, "/public/_search")
              .contentType(MediaType.JSON_UTF_8)
              .set("x-proxy-user", "admin")
              .set("_opendistro_security_user", "forged");
      if (authorization != null) headers.set("authorization", authorization);
      return client
          .execute(
              HttpRequest.of(
                  headers.build(),
                  com.linecorp.armeria.common.HttpData.ofUtf8("{\"query\":{\"match_all\":{}}}")))
          .aggregate()
          .join();
    }

    AggregatedHttpResponse request(String path, String body) {
      return client
          .execute(
              HttpRequest.of(
                  RequestHeaders.builder(HttpMethod.POST, path)
                      .set("authorization", USER)
                      .contentType(MediaType.JSON_UTF_8)
                      .build(),
                  com.linecorp.armeria.common.HttpData.ofUtf8(body)))
          .aggregate()
          .join();
    }

    @Override
    public void close() {
      gateway.stop().join();
      nativeServer.stop().join();
      astra.stop().join();
    }
  }
}
