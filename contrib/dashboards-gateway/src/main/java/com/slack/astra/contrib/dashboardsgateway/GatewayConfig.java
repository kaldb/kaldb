package com.slack.astra.contrib.dashboardsgateway;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

record GatewayConfig(
    URI astraUri,
    URI openSearchUri,
    String listenHost,
    int listenPort,
    Duration requestTimeout,
    int maxRequestBytes,
    Security security) {

  record Security(Path policyCredentialsFile, Path caFile, Set<String> datasets) {
    Security {
      datasets = Set.copyOf(datasets);
      if (policyCredentialsFile == null
          || datasets.isEmpty()
          || datasets.size() > 32
          || datasets.stream().anyMatch(name -> !name.matches("[a-z0-9][a-z0-9_-]{0,127}"))) {
        throw new IllegalArgumentException(
            "Secured mode requires valid concrete registered datasets and a policy credential file");
      }
    }
  }

  static GatewayConfig fromEnvironment() {
    Map<String, String> environment = System.getenv();
    Security security = null;
    if (Boolean.parseBoolean(environment.getOrDefault("SECURED_MODE", "false"))) {
      if (!parseUri(environment, "OPENSEARCH_URL", "").getScheme().equals("https")) {
        throw new IllegalArgumentException("Secured mode requires an HTTPS OpenSearch URL");
      }
      security =
          new Security(
              Path.of(environment.getOrDefault("POLICY_READER_CREDENTIALS_FILE", "")),
              Path.of(environment.getOrDefault("OPENSEARCH_CA_FILE", "")),
              Arrays.stream(environment.getOrDefault("REGISTERED_DATASETS", "").split(","))
                  .map(String::trim)
                  .collect(Collectors.toSet()));
    }
    return new GatewayConfig(
        parseUri(environment, "ASTRA_URL", "http://astra-query:8081"),
        parseUri(environment, "OPENSEARCH_URL", "http://opensearch:9200"),
        environment.getOrDefault("LISTEN_HOST", "0.0.0.0"),
        parseIntEnv(environment, "LISTEN_PORT", 9200),
        Duration.ofSeconds(parseLongEnv(environment, "REQUEST_TIMEOUT_SEC", 30L)),
        parseIntEnv(environment, "MAX_REQUEST_BYTES", 10485760),
        security);
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
