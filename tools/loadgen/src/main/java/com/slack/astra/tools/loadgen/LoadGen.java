package com.slack.astra.tools.loadgen;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class LoadGen {
  private static final DateTimeFormatter ISO_MILLIS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  public static void main(String[] args) {
    final String bulkUrl = env("KALDB_BULK_URL", "http://localhost:8086/_bulk");
    final String index = env("INDEX", "test");
    final String serviceName = env("SERVICE_NAME", "test");
    final String testRunId = env("TEST_RUN_ID", "");
    final String messagePrefix = env("MESSAGE_PREFIX", "Synthetic log");
    final int batchSize = Integer.parseInt(env("BATCH_SIZE", "5"));
    final double intervalSec = Double.parseDouble(env("INTERVAL_SEC", "1.0"));
    long id = Long.parseLong(env("START_ID", "100"));
    final double maxBackoff = Double.parseDouble(env("MAX_BACKOFF", "5.0"));

    final HttpClient http =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    final HttpRequest.Builder reqTemplate =
        HttpRequest.newBuilder()
            .uri(URI.create(bulkUrl))
            .timeout(Duration.ofSeconds(10))
            .header("Content-type", "application/x-ndjson");

    final String[] levels = {"INFO", "WARN", "ERROR", "DEBUG"};
    final Random rnd = new Random();

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("\n🛑 Stopping load generator.");
                }));

    System.out.printf(
        "🌊 Streaming logs to %s (index=%s, service=%s, run_id=%s, prefix=%s, batch=%d, every %.3fs). Ctrl+C to stop.%n",
        bulkUrl, index, serviceName, testRunId, messagePrefix, batchSize, intervalSec);

    double backoff = intervalSec;
    long sent = 0;

    while (true) {
      StringBuilder ndjson = new StringBuilder(256 * batchSize);
      for (int i = 0; i < batchSize; i++) {
        String ts = ISO_MILLIS.format(Instant.now());
        String level = levels[rnd.nextInt(levels.length)];
        ndjson
            .append("{ \"index\": { \"_index\": \"")
            .append(index)
            .append("\", \"_id\": \"")
            .append(id)
            .append("\" } }\n");
        ndjson
            .append("{ \"@timestamp\": \"")
            .append(ts)
            .append("\", \"level\": \"")
            .append(level)
            .append("\", \"message\": \"")
            .append(messagePrefix)
            .append(" ")
            .append(id)
            .append("\", \"service-name\": \"")
            .append(serviceName)
            .append("\"");
        if (!testRunId.isBlank()) {
          ndjson
              .append(", \"test_run_id\": \"")
              .append(testRunId.replace("\"", "\\\""))
              .append("\"");
        }
        ndjson.append(" }\n");
        id++;
      }

      HttpRequest req =
          reqTemplate.POST(HttpRequest.BodyPublishers.ofString(ndjson.toString())).build();

      try {
        HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
        int code = resp.statusCode();
        if (code >= 200 && code < 300) {
          sent += batchSize;
          System.out.printf("✔ sent %,d docs\r", sent);
          sleep(intervalSec);
          backoff = intervalSec;
        } else {
          System.err.printf("%n❗ bulk POST failed: HTTP %d. Backing off %.2fs…%n", code, backoff);
          sleep(backoff);
          backoff = Math.min(maxBackoff, Math.max(intervalSec, backoff * 1.5));
        }
      } catch (IOException | InterruptedException e) {
        System.err.printf("%n❗ bulk POST error: %s. Backing off %.2fs…%n", e.getMessage(), backoff);
        sleep(backoff);
        backoff = Math.min(maxBackoff, Math.max(intervalSec, backoff * 1.5));
      }
    }
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return (v == null || v.isBlank()) ? def : v;
  }

  private static void sleep(double seconds) {
    try {
      TimeUnit.MILLISECONDS.sleep((long) (seconds * 1000.0));
    } catch (InterruptedException ignored) {
    }
  }
}
