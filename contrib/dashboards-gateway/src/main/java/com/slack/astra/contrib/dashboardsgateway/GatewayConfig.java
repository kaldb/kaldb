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
        parseIntEnv(environment, "LISTEN_PORT", 9200),
        Duration.ofSeconds(parseLongEnv(environment, "REQUEST_TIMEOUT_SEC", 30L)),
        parseIntEnv(environment, "MAX_REQUEST_BYTES", 10485760));
  }

  URI uriFor(Upstream upstream) {
    return switch (upstream) {
      case ASTRA -> astraUri;
      case OPENSEARCH -> openSearchUri;
    };
  }

  private static URI parseUri(Map<String, String> environment, String envVar, String defaultValue) {
    String rawValue = environment.getOrDefault(envVar, defaultValue);
    try {
      return URI.create(rawValue);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid URI for " + envVar + ": " + rawValue, e);
    }
  }

  private static int parseIntEnv(Map<String, String> environment, String envVar, int defaultValue) {
    String rawValue = environment.getOrDefault(envVar, Integer.toString(defaultValue));
    try {
      return Integer.parseInt(rawValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer for " + envVar + ": " + rawValue, e);
    }
  }

  private static long parseLongEnv(
      Map<String, String> environment, String envVar, long defaultValue) {
    String rawValue = environment.getOrDefault(envVar, Long.toString(defaultValue));
    try {
      return Long.parseLong(rawValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid long for " + envVar + ": " + rawValue, e);
    }
  }
}
