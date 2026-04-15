package com.slack.astra.tools.syntheticdataprobe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class SyntheticDataProbeTest {
  private static final Instant BASE_TIME = Instant.parse("2026-04-15T00:00:00Z");

  @Test
  void successfulIngestAndQueryProducesHealthyMetrics() throws Exception {
    try (FakeKaldbServer fakeServer = new FakeKaldbServer()) {
      MutableClock clock = new MutableClock(BASE_TIME);
      SyntheticDataProbe probe = newProbe(fakeServer, clock);
      try {
        probe.ingestCycle();
        fakeServer.setQueryResponse(200, queryResponse(BASE_TIME.toEpochMilli(), 2));
        clock.advance(Duration.ofMinutes(1));
        probe.queryCycle();

        assertEquals(1, fakeServer.bulkRequests().size());
        assertEquals(1, fakeServer.queryRequests().size());
        String queryRequest = fakeServer.queryRequests().get(0);
        assertContains(queryRequest, "test_run_id:\\\"run-1\\\"");
        assertContains(queryRequest, "hostname:\\\"target.example\\\"");
        assertContains(queryRequest, "\"_timesinceepoch\"");
        assertContains(queryRequest, "\"gte\":");
        assertContains(queryRequest, "\"lt\":");

        String metrics = probe.buildMetricsPayload();
        assertContains(metrics, "kaldb_synthetic_data_probe_window_ready 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_window_min_ratio 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_window_max_abs_delta_docs 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_succeeded_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_failed_total 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_query_succeeded_total 1.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_expected_docs{bucket_age_minutes=\"1\"} 2.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_observed_docs{bucket_age_minutes=\"1\"} 2.0\n");
        assertContains(
            metrics, "kaldb_synthetic_data_probe_bucket_doc_delta{bucket_age_minutes=\"1\"} 0.0\n");
        assertContains(
            metrics, "kaldb_synthetic_data_probe_bucket_doc_ratio{bucket_age_minutes=\"1\"} 1.0\n");
      } finally {
        probe.stop();
      }
    }
  }

  @Test
  void explicitIngestHttpFailureIncrementsFailureMetrics() throws Exception {
    try (FakeKaldbServer fakeServer = new FakeKaldbServer()) {
      fakeServer.setBulkResponse(500, "{\"error\":\"boom\"}");
      MutableClock clock = new MutableClock(BASE_TIME);
      SyntheticDataProbe probe = newProbe(fakeServer, clock);
      try {
        probe.ingestCycle();

        String metrics = probe.buildMetricsPayload();
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_requests_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_succeeded_total 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_failed_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_docs_accepted_total 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_docs_failed_total 4.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_last_ingest_status_code 500.0\n");
      } finally {
        probe.stop();
      }
    }
  }

  @Test
  void explicitIngestPartialFailureIncrementsFailureMetrics() throws Exception {
    try (FakeKaldbServer fakeServer = new FakeKaldbServer()) {
      fakeServer.setBulkResponse(200, bulkResponse(4, 1));
      MutableClock clock = new MutableClock(BASE_TIME);
      SyntheticDataProbe probe = newProbe(fakeServer, clock);
      try {
        probe.ingestCycle();

        String metrics = probe.buildMetricsPayload();
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_requests_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_succeeded_total 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_failed_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_docs_accepted_total 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_docs_failed_total 4.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_last_ingest_status_code 200.0\n");
      } finally {
        probe.stop();
      }
    }
  }

  @Test
  void silentAcceptedButMissingDocsProducesLowRatioMetrics() throws Exception {
    try (FakeKaldbServer fakeServer = new FakeKaldbServer()) {
      MutableClock clock = new MutableClock(BASE_TIME);
      SyntheticDataProbe probe = newProbe(fakeServer, clock);
      try {
        probe.ingestCycle();
        fakeServer.setQueryResponse(200, queryResponse(BASE_TIME.toEpochMilli(), 0));
        clock.advance(Duration.ofMinutes(1));
        probe.queryCycle();

        String metrics = probe.buildMetricsPayload();
        assertContains(metrics, "kaldb_synthetic_data_probe_window_ready 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_window_min_ratio 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_window_max_abs_delta_docs 2.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_ingest_batches_succeeded_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_query_succeeded_total 1.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_expected_docs{bucket_age_minutes=\"1\"} 2.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_observed_docs{bucket_age_minutes=\"1\"} 0.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_doc_delta{bucket_age_minutes=\"1\"} -2.0\n");
        assertContains(
            metrics, "kaldb_synthetic_data_probe_bucket_doc_ratio{bucket_age_minutes=\"1\"} 0.0\n");
      } finally {
        probe.stop();
      }
    }
  }

  @Test
  void partialSilentLossProducesPartialRatioMetrics() throws Exception {
    try (FakeKaldbServer fakeServer = new FakeKaldbServer()) {
      MutableClock clock = new MutableClock(BASE_TIME);
      SyntheticDataProbe probe = newProbe(fakeServer, clock);
      try {
        probe.ingestCycle();
        fakeServer.setQueryResponse(200, queryResponse(BASE_TIME.toEpochMilli(), 1));
        clock.advance(Duration.ofMinutes(1));
        probe.queryCycle();

        String metrics = probe.buildMetricsPayload();
        assertContains(metrics, "kaldb_synthetic_data_probe_window_ready 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_window_min_ratio 0.5\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_window_max_abs_delta_docs 1.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_expected_docs{bucket_age_minutes=\"1\"} 2.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_observed_docs{bucket_age_minutes=\"1\"} 1.0\n");
        assertContains(
            metrics,
            "kaldb_synthetic_data_probe_bucket_doc_delta{bucket_age_minutes=\"1\"} -1.0\n");
        assertContains(
            metrics, "kaldb_synthetic_data_probe_bucket_doc_ratio{bucket_age_minutes=\"1\"} 0.5\n");
      } finally {
        probe.stop();
      }
    }
  }

  @Test
  void queryHttpFailureIncrementsFailureMetrics() throws Exception {
    try (FakeKaldbServer fakeServer = new FakeKaldbServer()) {
      fakeServer.setQueryResponse(500, "{\"error\":\"boom\"}");
      MutableClock clock = new MutableClock(BASE_TIME);
      SyntheticDataProbe probe = newProbe(fakeServer, clock);
      try {
        probe.queryCycle();

        String metrics = probe.buildMetricsPayload();
        assertContains(metrics, "kaldb_synthetic_data_probe_query_requests_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_query_succeeded_total 0.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_query_failed_total 1.0\n");
        assertContains(metrics, "kaldb_synthetic_data_probe_last_query_status_code 500.0\n");
      } finally {
        probe.stop();
      }
    }
  }

  private static SyntheticDataProbe newProbe(FakeKaldbServer fakeServer, MutableClock clock)
      throws IOException {
    return new SyntheticDataProbe(
        new SyntheticDataProbe.Config(
            fakeServer.bulkUri(),
            fakeServer.queryUri(),
            "logs",
            "run-1",
            "target.example",
            "other.example",
            "hostname",
            "127.0.0.1",
            0,
            4,
            1,
            0,
            1,
            1L,
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5)),
        clock);
  }

  private static String bulkResponse(int totalDocs, int failedDocs) {
    return "{\"totalDocs\":" + totalDocs + ",\"failedDocs\":" + failedDocs + "}";
  }

  private static String queryResponse(long bucketStartMs, long docCount) {
    return "{\"responses\":[{\"status\":200,\"_shards\":{\"failed\":0},\"aggregations\":{\"per_minute\":{\"buckets\":[{\"key\":"
        + bucketStartMs
        + ",\"doc_count\":"
        + docCount
        + "}]}}}]}";
  }

  private static void assertContains(String text, String expected) {
    assertTrue(
        text.contains(expected),
        () -> "Expected text to contain:\n" + expected + "\nActual text:\n" + text);
  }

  private static final class MutableClock extends Clock {
    private long epochMs;
    private final ZoneId zone;

    private MutableClock(Instant instant) {
      this(instant, ZoneOffset.UTC);
    }

    private MutableClock(Instant instant, ZoneId zone) {
      this.epochMs = instant.toEpochMilli();
      this.zone = zone;
    }

    private void advance(Duration duration) {
      epochMs += duration.toMillis();
    }

    @Override
    public ZoneId getZone() {
      return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return new MutableClock(Instant.ofEpochMilli(epochMs), zone);
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(epochMs);
    }
  }

  private static final class FakeKaldbServer implements AutoCloseable {
    private final HttpServer server;
    private final List<String> bulkRequests = new CopyOnWriteArrayList<>();
    private final List<String> queryRequests = new CopyOnWriteArrayList<>();
    private volatile Response bulkResponse = new Response(200, bulkResponse(4, 0));
    private volatile Response queryResponse = new Response(200, queryResponse(0L, 0L));

    private FakeKaldbServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext("/_bulk", this::handleBulk);
      server.createContext("/_msearch", this::handleQuery);
      server.start();
    }

    private URI bulkUri() {
      return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/_bulk");
    }

    private URI queryUri() {
      return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/_msearch");
    }

    private List<String> bulkRequests() {
      return bulkRequests;
    }

    private List<String> queryRequests() {
      return queryRequests;
    }

    private void setBulkResponse(int status, String body) {
      bulkResponse = new Response(status, body);
    }

    private void setQueryResponse(int status, String body) {
      queryResponse = new Response(status, body);
    }

    private void handleBulk(HttpExchange exchange) throws IOException {
      bulkRequests.add(readRequestBody(exchange));
      writeResponse(exchange, bulkResponse);
    }

    private void handleQuery(HttpExchange exchange) throws IOException {
      queryRequests.add(readRequestBody(exchange));
      writeResponse(exchange, queryResponse);
    }

    @Override
    public void close() {
      server.stop(0);
    }

    private static String readRequestBody(HttpExchange exchange) throws IOException {
      return new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void writeResponse(HttpExchange exchange, Response response) throws IOException {
      byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
      exchange.sendResponseHeaders(response.status(), body.length);
      try (OutputStream responseBody = exchange.getResponseBody()) {
        responseBody.write(body);
      }
    }
  }

  private record Response(int status, String body) {}
}
