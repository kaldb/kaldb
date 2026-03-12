package com.slack.astra.contrib.dashboardsgateway;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

record GatewayConfig(
    URI astraUri,
    URI openSearchUri,
    String listenHost,
    int listenPort,
    Duration requestTimeout,
    int maxRequestBytes) {

  static GatewayConfig fromEnvironment() {
    Map<String, String> environment = System.getenv();
    return new GatewayConfig(
        parseUri(environment, "ASTRA_URL", "http://astra-query:8081"),
        parseUri(environment, "OPENSEARCH_URL", "http://opensearch:9200"),
        environment.getOrDefault("LISTEN_HOST", "0.0.0.0"),
        Integer.parseInt(environment.getOrDefault("LISTEN_PORT", "9200")),
        Duration.ofSeconds(Long.parseLong(environment.getOrDefault("REQUEST_TIMEOUT_SEC", "30"))),
        Integer.parseInt(environment.getOrDefault("MAX_REQUEST_BYTES", "10485760")));
  }

  URI uriFor(Upstream upstream) {
    return switch (upstream) {
      case ASTRA -> astraUri;
      case OPENSEARCH -> openSearchUri;
    };
  }

  private static URI parseUri(Map<String, String> environment, String envVar, String defaultValue) {
    return URI.create(environment.getOrDefault(envVar, defaultValue));
  }
}
