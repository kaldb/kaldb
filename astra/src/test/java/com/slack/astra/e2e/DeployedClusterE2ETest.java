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
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("external")
public class DeployedClusterE2ETest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final String AGGREGATION_NAME = "per_minute";

  @Test
  public void testBulkIngestAndAggregationQueryAgainstDeployedCluster() throws Exception {
    ExternalClusterConfig config = ExternalClusterConfig.fromEnvironment();
    Assumptions.assumeTrue(
        config.enabled(), "Set kaldb.e2e.enabled=true to run the deployed-cluster black-box test.");

    Instant baseBucketTime =
        Instant.now()
            .truncatedTo(ChronoUnit.MINUTES)
            .minus(config.bucketCount() + 2L, ChronoUnit.MINUTES);
    String marker = "e2ecache" + UUID.randomUUID().toString().replace("-", "");

    ingestDocuments(config, marker, baseBucketTime);

    await()
        .pollInterval(config.pollInterval())
        .atMost(config.pollTimeout())
        .untilAsserted(() -> assertExpectedAggregations(config, marker));
  }

  private void ingestDocuments(ExternalClusterConfig config, String marker, Instant baseBucketTime)
      throws Exception {
    String filler = "x".repeat(config.messageBytes());
    StringBuilder batchBody = new StringBuilder();
    int batchDocs = 0;
    int totalDocs = 0;

    for (int bucketIndex = 0; bucketIndex < config.bucketCount(); bucketIndex++) {
      Instant bucketTime = baseBucketTime.plus(bucketIndex, ChronoUnit.MINUTES);
      for (int docIndex = 0; docIndex < config.docsPerBucket(); docIndex++) {
        batchBody
            .append(
                OBJECT_MAPPER.writeValueAsString(
                    Map.of(
                        "index",
                        Map.of(
                            "_index",
                            config.dataset(),
                            "_id",
                            marker + "-b" + bucketIndex + "-d" + docIndex))))
            .append('\n');

        Map<String, Object> document = new LinkedHashMap<>();
        document.put("@timestamp", bucketTime.toString());
        document.put(
            "message", marker + " bucket" + bucketIndex + " doc" + docIndex + " " + filler);
        document.put("level", "INFO");
        document.put("test_run_id", marker);
        document.put("bucket_index", bucketIndex);
        document.put("doc_index", docIndex);

        batchBody.append(OBJECT_MAPPER.writeValueAsString(document)).append('\n');
        batchDocs++;
        totalDocs++;

        if (batchDocs == config.batchSize()) {
          postBulkBatch(config, batchBody.toString(), batchDocs);
          batchBody.setLength(0);
          batchDocs = 0;
        }
      }
    }

    if (batchDocs > 0) {
      postBulkBatch(config, batchBody.toString(), batchDocs);
    }

    assertThat(totalDocs).isEqualTo(config.totalDocs());
  }

  private void postBulkBatch(ExternalClusterConfig config, String requestBody, int expectedDocs)
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

  private void assertExpectedAggregations(ExternalClusterConfig config, String marker)
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

    assertThat(aggregatedDocs).isEqualTo(config.totalDocs());
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

  private record ExternalClusterConfig(
      boolean enabled,
      String ingestBaseUrl,
      String queryBaseUrl,
      String dataset,
      int bucketCount,
      int docsPerBucket,
      int messageBytes,
      int batchSize,
      Duration pollTimeout,
      Duration pollInterval) {

    private static ExternalClusterConfig fromEnvironment() {
      boolean enabled = Boolean.parseBoolean(readSetting("kaldb.e2e.enabled", "false"));
      if (!enabled) {
        return new ExternalClusterConfig(
            false, "", "", "", 0, 0, 0, 0, Duration.ZERO, Duration.ZERO);
      }

      String ingestBaseUrl = requireSetting("kaldb.e2e.ingestBaseUrl");
      String queryBaseUrl = requireSetting("kaldb.e2e.queryBaseUrl");
      String dataset = requireSetting("kaldb.e2e.dataset");

      return new ExternalClusterConfig(
          true,
          ingestBaseUrl,
          queryBaseUrl,
          dataset,
          readIntSetting("kaldb.e2e.bucketCount", 20),
          readIntSetting("kaldb.e2e.docsPerBucket", 1000),
          readIntSetting("kaldb.e2e.messageBytes", 2048),
          readIntSetting("kaldb.e2e.batchSize", 500),
          Duration.ofSeconds(readIntSetting("kaldb.e2e.pollTimeoutSeconds", 300)),
          Duration.ofSeconds(readIntSetting("kaldb.e2e.pollIntervalSeconds", 5)));
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

    private int totalDocs() {
      return bucketCount * docsPerBucket;
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
