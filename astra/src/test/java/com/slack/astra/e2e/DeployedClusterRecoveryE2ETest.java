package com.slack.astra.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.bulkIngestApi.BulkIngestResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("external")
public class DeployedClusterRecoveryE2ETest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final String AGGREGATION_NAME = "per_minute";

  @Test
  public void runRecoveryPhaseAgainstDeployedCluster() throws Exception {
    RecoveryE2EConfig config = RecoveryE2EConfig.fromEnvironment();
    Assumptions.assumeTrue(
        config.enabled(), "Set kaldb.e2e.recovery.enabled=true to run recovery E2E phases.");

    switch (config.phase()) {
      case BASELINE -> runBaselinePhase(config);
      case BACKLOG -> runBacklogPhase(config);
      case VERIFY -> runVerifyPhase(config);
    }
  }

  private void runBaselinePhase(RecoveryE2EConfig config) throws Exception {
    ingestDocuments(config, config.baselineMarker(), config.baselineDocs());
    awaitExpectedAggregations(config, config.baselineMarker(), config.baselineDocs());

    if (!config.baselineSettleDelay().isZero()) {
      TimeUnit.MILLISECONDS.sleep(config.baselineSettleDelay().toMillis());
    }
  }

  private void runBacklogPhase(RecoveryE2EConfig config) throws Exception {
    ingestDocuments(config, config.backlogMarker(), config.backlogDocs());
  }

  private void runVerifyPhase(RecoveryE2EConfig config) {
    awaitExpectedAggregations(config, config.backlogMarker(), config.backlogDocs());
  }

  private void ingestDocuments(RecoveryE2EConfig config, String marker, int totalDocs)
      throws Exception {
    String filler = "x".repeat(config.messageBytes());
    Instant baseBucketTime =
        Instant.now()
            .truncatedTo(ChronoUnit.MINUTES)
            .minus(config.bucketCount() + 2L, ChronoUnit.MINUTES);
    StringBuilder batchBody = new StringBuilder();
    int batchDocs = 0;

    for (int docIndex = 0; docIndex < totalDocs; docIndex++) {
      int bucketIndex = docIndex % config.bucketCount();
      Instant bucketTime = baseBucketTime.plus(bucketIndex, ChronoUnit.MINUTES);

      batchBody
          .append(
              OBJECT_MAPPER.writeValueAsString(
                  Map.of(
                      "index",
                      Map.of("_index", config.dataset(), "_id", marker + "-d" + docIndex))))
          .append('\n');

      Map<String, Object> document = new LinkedHashMap<>();
      document.put("@timestamp", bucketTime.toString());
      document.put("message", marker + " bucket" + bucketIndex + " doc" + docIndex + " " + filler);
      document.put("level", "INFO");
      document.put("test_run_id", marker);
      document.put("bucket_index", bucketIndex);
      document.put("doc_index", docIndex);

      batchBody.append(OBJECT_MAPPER.writeValueAsString(document)).append('\n');
      batchDocs++;

      if (batchDocs == config.batchSize()) {
        postBulkBatch(config, batchBody.toString(), batchDocs);
        batchBody.setLength(0);
        batchDocs = 0;
      }
    }

    if (batchDocs > 0) {
      postBulkBatch(config, batchBody.toString(), batchDocs);
    }
  }

  private void postBulkBatch(RecoveryE2EConfig config, String requestBody, int expectedDocs)
      throws Exception {
    HttpResponse<String> response =
        postText(config.ingestUri("/_bulk"), "application/x-ndjson", requestBody);

    assertThat(response.statusCode()).isEqualTo(200);

    BulkIngestResponse bulkResponse =
        OBJECT_MAPPER.readValue(response.body(), BulkIngestResponse.class);
    assertThat(bulkResponse.totalDocs()).isEqualTo(expectedDocs);
    assertThat(bulkResponse.failedDocs()).isZero();
    assertThat(bulkResponse.errorMsg()).isBlank();
  }

  private void awaitExpectedAggregations(
      RecoveryE2EConfig config, String marker, int expectedDocs) {
    await()
        .pollInterval(config.pollInterval())
        .atMost(config.pollTimeout())
        .untilAsserted(() -> assertExpectedAggregations(config, marker, expectedDocs));
  }

  private void assertExpectedAggregations(RecoveryE2EConfig config, String marker, int expectedDocs)
      throws Exception {
    String requestBody = buildMultiSearchRequest(config.dataset(), marker);
    HttpResponse<String> response =
        postText(config.queryUri("/_msearch"), "application/x-ndjson", requestBody);

    assertThat(response.statusCode()).isEqualTo(200);

    JsonNode root = OBJECT_MAPPER.readTree(response.body());
    JsonNode searchResponse = root.path("responses").get(0);
    assertThat(searchResponse).isNotNull();
    assertThat(searchResponse.path("status").asInt()).isEqualTo(200);
    assertThat(searchResponse.path("_shards").path("failed").asInt()).isEqualTo(0);

    JsonNode buckets = searchResponse.path("aggregations").path(AGGREGATION_NAME).path("buckets");
    assertThat(buckets.isArray()).isTrue();
    assertThat(buckets.size()).isGreaterThan(0);

    long aggregatedDocs = 0;
    for (JsonNode bucket : buckets) {
      aggregatedDocs += bucket.path("doc_count").asLong();
    }

    assertThat(aggregatedDocs).isEqualTo(expectedDocs);
  }

  private String buildMultiSearchRequest(String dataset, String marker) throws IOException {
    Map<String, Object> queryBody = new LinkedHashMap<>();
    queryBody.put("size", 0);
    queryBody.put("query", Map.of("query_string", Map.of("query", "message:" + marker)));
    queryBody.put(
        "aggs",
        Map.of(
            AGGREGATION_NAME,
            Map.of(
                "date_histogram",
                Map.of(
                    "interval",
                    "1m",
                    "field",
                    "_timesinceepoch",
                    "min_doc_count",
                    1,
                    "format",
                    "epoch_millis"))));

    return OBJECT_MAPPER.writeValueAsString(Map.of("index", dataset))
        + "\n"
        + OBJECT_MAPPER.writeValueAsString(queryBody)
        + "\n";
  }

  private HttpResponse<String> postText(URI uri, String contentType, String body)
      throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .header("Content-Type", contentType)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private enum RecoveryPhase {
    BASELINE,
    BACKLOG,
    VERIFY
  }

  private record RecoveryE2EConfig(
      boolean enabled,
      RecoveryPhase phase,
      String runId,
      String ingestBaseUrl,
      String queryBaseUrl,
      String dataset,
      int baselineDocs,
      int backlogDocs,
      int messageBytes,
      int batchSize,
      int bucketCount,
      Duration baselineSettleDelay,
      Duration pollTimeout,
      Duration pollInterval) {

    private static RecoveryE2EConfig fromEnvironment() {
      boolean enabled = Boolean.parseBoolean(readSetting("kaldb.e2e.recovery.enabled", "false"));
      if (!enabled) {
        return new RecoveryE2EConfig(
            false,
            RecoveryPhase.BASELINE,
            "",
            "",
            "",
            "",
            0,
            0,
            0,
            0,
            0,
            Duration.ZERO,
            Duration.ZERO,
            Duration.ZERO);
      }

      return new RecoveryE2EConfig(
          true,
          readPhaseSetting("kaldb.e2e.recovery.phase"),
          readRunIdSetting("kaldb.e2e.recovery.runId"),
          requireSetting("kaldb.e2e.ingestBaseUrl"),
          requireSetting("kaldb.e2e.queryBaseUrl"),
          requireSetting("kaldb.e2e.dataset"),
          readIntSetting("kaldb.e2e.recovery.baselineDocs", 20_000),
          readIntSetting("kaldb.e2e.recovery.backlogDocs", 2_000),
          readIntSetting("kaldb.e2e.recovery.messageBytes", 2048),
          readIntSetting("kaldb.e2e.recovery.batchSize", 500),
          readIntSetting("kaldb.e2e.recovery.bucketCount", 20),
          Duration.ofSeconds(readIntSetting("kaldb.e2e.recovery.baselineSettleSeconds", 30)),
          Duration.ofSeconds(readIntSetting("kaldb.e2e.recovery.pollTimeoutSeconds", 600)),
          Duration.ofSeconds(readIntSetting("kaldb.e2e.recovery.pollIntervalSeconds", 5)));
    }

    private String baselineMarker() {
      return "e2erecovery" + runId + "baseline";
    }

    private String backlogMarker() {
      return "e2erecovery" + runId + "backlog";
    }

    private static RecoveryPhase readPhaseSetting(String key) {
      return RecoveryPhase.valueOf(requireSetting(key).toUpperCase(Locale.ROOT));
    }

    private static String readRunIdSetting(String key) {
      String runId = requireSetting(key).replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
      if (runId.isBlank()) {
        throw new IllegalStateException(
            "Setting must contain at least one alphanumeric char: " + key);
      }
      return runId;
    }

    private static String requireSetting(String key) {
      String value = readSetting(key, "");
      if (value.isBlank()) {
        throw new IllegalStateException("Missing required setting: " + key);
      }
      return value;
    }

    private static int readIntSetting(String key, int defaultValue) {
      return Integer.parseInt(readSetting(key, String.valueOf(defaultValue)));
    }

    private static String readSetting(String key, String defaultValue) {
      String value = System.getProperty(key);
      if (value == null || value.isBlank()) {
        String envKey = key.toUpperCase(Locale.ROOT).replace('.', '_');
        value = System.getenv(envKey);
      }
      if (value == null || value.isBlank()) {
        return defaultValue;
      }
      return value;
    }

    private URI ingestUri(String path) {
      return URI.create(trimTrailingSlash(ingestBaseUrl) + path);
    }

    private URI queryUri(String path) {
      return URI.create(trimTrailingSlash(queryBaseUrl) + path);
    }

    private static String trimTrailingSlash(String baseUrl) {
      if (baseUrl.endsWith("/")) {
        return baseUrl.substring(0, baseUrl.length() - 1);
      }
      return baseUrl;
    }
  }
}
