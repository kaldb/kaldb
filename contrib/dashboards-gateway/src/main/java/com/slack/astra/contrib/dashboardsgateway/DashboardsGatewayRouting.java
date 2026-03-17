package com.slack.astra.contrib.dashboardsgateway;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

final class DashboardsGatewayRouting {
  static final String HEALTH_PATH = "/_astra/gateway/health";
  private static final String SECURITY_ACCOUNT_PATH = "/_plugins/_security/api/account";
  private static final Set<String> ASTRA_INDEX_APIS =
      Set.of("_alias", "_field_caps", "_mapping", "_msearch", "_search");
  private static final byte[] SECURITY_ACCOUNT_RESPONSE =
      """
      {"user_name":"admin","backend_roles":[],"roles":[],"tenants":{},"custom_attribute_names":[]}
      """
          .strip()
          .getBytes(StandardCharsets.UTF_8);
  private static final byte[] HEALTH_RESPONSE =
      """
      {"status":"ok","component":"dashboards-gateway"}
      """
          .strip()
          .getBytes(StandardCharsets.UTF_8);

  // Returns a synthetic response for paths handled locally, without forwarding to any upstream.
  // Armeria automatically strips the body for HEAD requests.
  Optional<HttpResponse> syntheticResponse(String path) {
    if (path.equals(SECURITY_ACCOUNT_PATH)) {
      return Optional.of(
          HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, SECURITY_ACCOUNT_RESPONSE));
    }
    if (path.equals(HEALTH_PATH)) {
      return Optional.of(HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, HEALTH_RESPONSE));
    }
    return Optional.empty();
  }

  Upstream selectUpstream(String path) {
    List<String> segments = pathSegments(path);

    if (path.equals("/") || path.equals("/_nodes") || path.equals("/_cluster/state/nodes")) {
      return Upstream.ASTRA;
    }

    // Scroll contexts live in OpenSearch; check before the /_search catch-all below.
    if (path.startsWith("/_search/scroll")) {
      return Upstream.OPENSEARCH;
    }

    // Root-level index APIs that fan out across all Astra indices.
    if (path.startsWith("/_msearch")
        || path.startsWith("/_field_caps")
        || path.startsWith("/_search")
        || path.startsWith("/_mapping")
        || path.equals("/_alias")
        || path.startsWith("/_alias/")) {
      return Upstream.ASTRA;
    }

    if (path.startsWith("/.")) {
      return Upstream.OPENSEARCH;
    }

    if (segments.size() >= 2 && ASTRA_INDEX_APIS.contains(segments.get(1))) {
      return Upstream.ASTRA;
    }

    if (path.startsWith("/_aliases") || path.startsWith("/_cat") || path.startsWith("/_plugins")) {
      return Upstream.OPENSEARCH;
    }

    if (path.startsWith("/_reindex") || path.startsWith("/_tasks")) {
      return Upstream.OPENSEARCH;
    }

    return Upstream.OPENSEARCH;
  }

  private static List<String> pathSegments(String path) {
    return Arrays.stream(path.split("/")).filter(Predicate.not(String::isBlank)).toList();
  }
}

enum Upstream {
  ASTRA,
  OPENSEARCH
}
