package com.slack.astra.tools.syntheticdataprobe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

final class SyntheticDataProbe {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DateTimeFormatter ISO_MILLIS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
  private static final long MINUTE_MS = 60_000L;
  private static final long NO_COVERED_BUCKET = Long.MIN_VALUE;
  private static final String AGGREGATION_NAME = "per_minute";
  private static final String METRIC_PREFIX = "kaldb_synthetic_data_probe_";
  private static final MediaType PROMETHEUS_TEXT_MEDIA_TYPE =
      MediaType.parse("text/plain; version=0.0.4; charset=utf-8");

  private final Config config;
  private final Clock clock;
  private final HttpClient httpClient;
  private final ConcurrentMap<Long, BucketStats> bucketStatsByStartMs;
  private final LongAdder ingestRequestsTotal;
  private final LongAdder ingestBatchesSucceededTotal;
  private final LongAdder ingestBatchesFailedTotal;
  private final LongAdder ingestCatchupBatchesTotal;
  private final LongAdder ingestDocsAcceptedTotal;
  private final LongAdder ingestDocsFailedTotal;
  private final LongAdder queryRequestsTotal;
  private final LongAdder querySucceededTotal;
  private final LongAdder queryFailedTotal;
  private final AtomicLong lastIngestSuccessEpochMs;
  private final AtomicLong lastQuerySuccessEpochMs;
  private final AtomicLong lastIngestStatusCode;
  private final AtomicLong lastQueryStatusCode;
  private final AtomicLong nextDocId;
  private final AtomicLong lastCoveredBucketStartMs;
  private final ScheduledExecutorService ingestExecutor;
  private final ScheduledExecutorService queryExecutor;
  private final Server metricsServer;
  private final CountDownLatch done;
  private final String filler;
  private volatile boolean stopped;

  private SyntheticDataProbe(Config config) throws IOException {
    this(config, Clock.systemUTC());
  }

  SyntheticDataProbe(Config config, Clock clock) throws IOException {
    this.config = config;
    this.clock = clock;
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(config.connectTimeout())
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    this.bucketStatsByStartMs = new ConcurrentHashMap<>();
    this.ingestRequestsTotal = new LongAdder();
    this.ingestBatchesSucceededTotal = new LongAdder();
    this.ingestBatchesFailedTotal = new LongAdder();
    this.ingestCatchupBatchesTotal = new LongAdder();
    this.ingestDocsAcceptedTotal = new LongAdder();
    this.ingestDocsFailedTotal = new LongAdder();
    this.queryRequestsTotal = new LongAdder();
    this.querySucceededTotal = new LongAdder();
    this.queryFailedTotal = new LongAdder();
    this.lastIngestSuccessEpochMs = new AtomicLong();
    this.lastQuerySuccessEpochMs = new AtomicLong();
    this.lastIngestStatusCode = new AtomicLong();
    this.lastQueryStatusCode = new AtomicLong();
    this.nextDocId = new AtomicLong(config.startId());
    this.lastCoveredBucketStartMs = new AtomicLong(NO_COVERED_BUCKET);
    this.ingestExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "kaldb-synthetic-data-probe-ingest"));
    this.queryExecutor =
        Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "kaldb-synthetic-data-probe-query"));
    this.metricsServer =
        Server.builder()
            .http(new InetSocketAddress(config.metricsHost(), config.metricsPort()))
            .service("/metrics", (ctx, req) -> metricsResponse())
            .service("/readyz", (ctx, req) -> readyResponse())
            .build();
    this.done = new CountDownLatch(1);
    this.filler = "x".repeat(config.messageBytes());
  }

  static void runFromEnvironment() throws Exception {
    new SyntheticDataProbe(Config.fromEnvironment()).run();
  }

  private void run() throws InterruptedException {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(this::stop, "kaldb-synthetic-data-probe-shutdown"));

    System.out.printf(
        "Starting synthetic data probe.%n  bulk=%s%n  query=%s%n  index=%s%n  metrics=http://%s:%d/metrics%n  run_id=%s%n  target_hostname=%s%n  distractor_hostname=%s%n  window_buckets=%d%n  checked_bucket_age_minutes=%d..%d%n  max_catchup_buckets_per_cycle=%d%n",
        config.bulkUri(),
        config.queryUri(),
        config.index(),
        config.metricsHost(),
        config.metricsPort(),
        config.runId(),
        config.targetHostname(),
        config.distractorHostname(),
        config.windowBuckets(),
        config.newestBucketAgeMinutes(),
        config.oldestCheckedBucketAgeMinutes(),
        config.maxCatchupBucketsPerCycle());

    metricsServer.start().join();
    var unusedIngestTask =
        ingestExecutor.scheduleWithFixedDelay(
            this::safeIngestCycle, 0L, config.ingestInterval().toMillis(), TimeUnit.MILLISECONDS);
    var unusedQueryTask =
        queryExecutor.scheduleWithFixedDelay(
            this::safeQueryCycle, 0L, config.queryInterval().toMillis(), TimeUnit.MILLISECONDS);

    done.await();
  }

  void stop() {
    if (stopped) {
      return;
    }
    stopped = true;
    ingestExecutor.shutdownNow();
    queryExecutor.shutdownNow();
    metricsServer.close();
    httpClient.close();
    done.countDown();
  }

  private void safeIngestCycle() {
    try {
      ingestCycle();
    } catch (Exception e) {
      ingestBatchesFailedTotal.increment();
      ingestDocsFailedTotal.add(config.batchSize());
      System.err.printf("synthetic data probe ingest cycle failed: %s%n", e.getMessage());
    }
  }

  private void safeQueryCycle() {
    try {
      queryCycle();
    } catch (Exception e) {
      queryFailedTotal.increment();
      System.err.printf("synthetic data probe query cycle failed: %s%n", e.getMessage());
    }
  }

  void ingestCycle() throws IOException, InterruptedException {
    long nowMs = clock.millis();
    long activeBucketStartMs = activeBucketStartMs(nowMs);
    long lastCoveredBucketStartMs = this.lastCoveredBucketStartMs.get();
    boolean catchingUp =
        lastCoveredBucketStartMs != NO_COVERED_BUCKET
            && lastCoveredBucketStartMs < activeBucketStartMs;
    long firstBucketStartMs =
        catchingUp ? lastCoveredBucketStartMs + MINUTE_MS : activeBucketStartMs;
    int maxBucketsThisCycle = catchingUp ? config.maxCatchupBucketsPerCycle() : 1;

    int ingestedBuckets = 0;
    for (long bucketStartMs = firstBucketStartMs;
        bucketStartMs <= activeBucketStartMs && ingestedBuckets < maxBucketsThisCycle;
        bucketStartMs += MINUTE_MS) {
      if (!ingestBucket(bucketStartMs, nowMs, catchingUp)) {
        return;
      }
      markBucketCovered(bucketStartMs);
      ingestedBuckets++;
    }

    pruneOldBuckets(nowMs);
  }

  private boolean ingestBucket(long bucketStartMs, long nowMs, boolean catchup)
      throws IOException, InterruptedException {
    IngestBatch batch = buildIngestBatch(bucketStartMs);

    ingestRequestsTotal.increment();
    HttpResponse<String> response =
        postText(config.bulkUri(), "application/x-ndjson", batch.requestBody());
    lastIngestStatusCode.set(response.statusCode());

    if (response.statusCode() != 200) {
      ingestBatchesFailedTotal.increment();
      ingestDocsFailedTotal.add(batch.totalDocs());
      System.err.printf(
          "synthetic data probe ingest HTTP %d for bucket %d%n",
          response.statusCode(), bucketStartMs);
      return false;
    }

    JsonNode root = OBJECT_MAPPER.readTree(response.body());
    int totalDocs = root.path("totalDocs").asInt(-1);
    int failedDocs = root.path("failedDocs").asInt(-1);
    if (failedDocs != 0 || totalDocs != batch.totalDocs()) {
      ingestBatchesFailedTotal.increment();
      ingestDocsFailedTotal.add(batch.totalDocs());
      System.err.printf(
          "synthetic data probe ingest partial failure: totalDocs=%d failedDocs=%d expected=%d%n",
          totalDocs, failedDocs, batch.totalDocs());
      return false;
    }

    BucketStats bucketStats =
        bucketStatsByStartMs.computeIfAbsent(bucketStartMs, ignored -> new BucketStats());
    bucketStats.expectedTargetDocs.add(batch.targetDocs());
    ingestBatchesSucceededTotal.increment();
    if (catchup) {
      ingestCatchupBatchesTotal.increment();
    }
    ingestDocsAcceptedTotal.add(batch.totalDocs());
    lastIngestSuccessEpochMs.set(nowMs);
    return true;
  }

  private void markBucketCovered(long bucketStartMs) {
    lastCoveredBucketStartMs.accumulateAndGet(bucketStartMs, Math::max);
  }

  void queryCycle() throws IOException, InterruptedException {
    long nowMs = clock.millis();
    long activeBucketStartMs = activeBucketStartMs(nowMs);
    long windowStartMs =
        activeBucketStartMs - (long) config.oldestCheckedBucketAgeMinutes() * MINUTE_MS;
    long windowEndMs =
        activeBucketStartMs - (long) (config.newestBucketAgeMinutes() - 1) * MINUTE_MS;
    String requestBody = buildQueryRequest(windowStartMs, windowEndMs);

    queryRequestsTotal.increment();
    HttpResponse<String> response =
        postText(config.queryUri(), "application/x-ndjson", requestBody);
    lastQueryStatusCode.set(response.statusCode());

    if (response.statusCode() != 200) {
      queryFailedTotal.increment();
      System.err.printf("synthetic data probe query HTTP %d%n", response.statusCode());
      return;
    }

    JsonNode root = OBJECT_MAPPER.readTree(response.body());
    JsonNode searchResponse = root.path("responses").get(0);
    if (searchResponse == null
        || searchResponse.path("status").asInt(-1) != 200
        || searchResponse.path("_shards").path("failed").asInt(0) != 0) {
      queryFailedTotal.increment();
      System.err.printf(
          "synthetic data probe query returned unexpected response: %s%n", response.body());
      return;
    }

    for (int age = config.newestBucketAgeMinutes();
        age <= config.oldestCheckedBucketAgeMinutes();
        age++) {
      long bucketStartMs = activeBucketStartMs - (long) age * MINUTE_MS;
      bucketStatsByStartMs
          .computeIfAbsent(bucketStartMs, ignored -> new BucketStats())
          .observedTargetDocs
          .set(0L);
    }

    JsonNode buckets = searchResponse.path("aggregations").path(AGGREGATION_NAME).path("buckets");
    if (!buckets.isArray()) {
      queryFailedTotal.increment();
      System.err.println("synthetic data probe query did not return aggregation buckets");
      return;
    }

    for (JsonNode bucket : buckets) {
      long bucketStartMs = bucket.path("key").asLong(Long.MIN_VALUE);
      if (bucketStartMs == Long.MIN_VALUE) {
        continue;
      }
      long ageMinutes = (activeBucketStartMs - bucketStartMs) / MINUTE_MS;
      if (ageMinutes < config.newestBucketAgeMinutes()
          || ageMinutes > config.oldestCheckedBucketAgeMinutes()) {
        continue;
      }
      BucketStats bucketStats =
          bucketStatsByStartMs.computeIfAbsent(bucketStartMs, ignored -> new BucketStats());
      bucketStats.observedTargetDocs.set(bucket.path("doc_count").asLong(0L));
    }

    querySucceededTotal.increment();
    lastQuerySuccessEpochMs.set(nowMs);
    pruneOldBuckets(nowMs);
  }

  private IngestBatch buildIngestBatch(long bucketStartMs) throws IOException {
    String timestamp = ISO_MILLIS.format(Instant.ofEpochMilli(bucketStartMs));
    StringBuilder requestBody = new StringBuilder(512 * config.batchSize());
    int targetDocs = 0;

    for (int index = 0; index < config.batchSize(); index++) {
      long docId = nextDocId.getAndIncrement();
      boolean targetHost = (docId & 1L) == 0L;
      String hostname = targetHost ? config.targetHostname() : config.distractorHostname();
      if (targetHost) {
        targetDocs++;
      }

      requestBody
          .append(
              OBJECT_MAPPER.writeValueAsString(
                  Map.of(
                      "index",
                      Map.of("_index", config.index(), "_id", config.runId() + "-" + docId))))
          .append('\n');

      Map<String, Object> document = new LinkedHashMap<>();
      document.put("@timestamp", timestamp);
      document.put(
          "message", "synthetic-data-probe " + config.runId() + " doc " + docId + " " + filler);
      document.put(config.hostnameField(), hostname);
      document.put("level", "INFO");
      document.put("test_run_id", config.runId());
      document.put("bucket_epoch_ms", bucketStartMs);
      document.put("doc_id", docId);
      requestBody.append(OBJECT_MAPPER.writeValueAsString(document)).append('\n');
    }

    return new IngestBatch(bucketStartMs, config.batchSize(), targetDocs, requestBody.toString());
  }

  private String buildQueryRequest(long windowStartMs, long activeBucketStartMs)
      throws IOException {
    List<Map<String, Object>> filters = new ArrayList<>();
    filters.add(
        Map.of(
            "query_string",
            Map.of("query", "test_run_id:" + quotedQueryStringValue(config.runId()))));
    filters.add(
        Map.of(
            "query_string",
            Map.of(
                "query",
                config.hostnameField() + ":" + quotedQueryStringValue(config.targetHostname()))));
    filters.add(
        Map.of(
            "range",
            Map.of(
                "_timesinceepoch",
                Map.of(
                    "gte", windowStartMs, "lt", activeBucketStartMs, "format", "epoch_millis"))));

    Map<String, Object> queryBody = new LinkedHashMap<>();
    queryBody.put("size", 0);
    queryBody.put("query", Map.of("bool", Map.of("filter", filters)));
    queryBody.put(
        "aggs",
        Map.of(
            AGGREGATION_NAME,
            Map.of(
                "date_histogram",
                Map.of(
                    "fixed_interval",
                    "1m",
                    "field",
                    "_timesinceepoch",
                    "min_doc_count",
                    0,
                    "format",
                    "epoch_millis",
                    "extended_bounds",
                    Map.of("min", windowStartMs, "max", activeBucketStartMs - 1)))));

    return OBJECT_MAPPER.writeValueAsString(Map.of("index", config.index()))
        + "\n"
        + OBJECT_MAPPER.writeValueAsString(queryBody)
        + "\n";
  }

  private HttpResponse<String> postText(URI uri, String contentType, String body)
      throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .timeout(config.requestTimeout())
            .header("Content-Type", contentType)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private com.linecorp.armeria.common.HttpResponse readyResponse() {
    boolean ready = windowReady();
    return com.linecorp.armeria.common.HttpResponse.of(
        ready ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE,
        MediaType.PLAIN_TEXT_UTF_8,
        ready ? "ready\n" : "warming\n");
  }

  private com.linecorp.armeria.common.HttpResponse metricsResponse() {
    return com.linecorp.armeria.common.HttpResponse.of(
        HttpStatus.OK, PROMETHEUS_TEXT_MEDIA_TYPE, buildMetricsPayload());
  }

  String buildMetricsPayload() {
    long nowMs = clock.millis();
    long activeBucketStartMs = activeBucketStartMs(nowMs);
    WindowSnapshot snapshot = buildWindowSnapshot(activeBucketStartMs);
    StringBuilder metrics = new StringBuilder(4096);

    appendMetricType(metrics, metricName("up"), "gauge");
    appendMetricType(metrics, metricName("window_ready"), "gauge");
    appendMetricType(metrics, metricName("window_min_ratio"), "gauge");
    appendMetricType(metrics, metricName("window_max_abs_delta_docs"), "gauge");
    appendMetricType(metrics, metricName("window_buckets_with_expected_docs"), "gauge");
    appendMetricType(metrics, metricName("ingest_coverage_backlog_buckets"), "gauge");
    appendMetricType(metrics, metricName("active_ingest_bucket_epoch_seconds"), "gauge");
    appendMetricType(metrics, metricName("last_ingest_success_epoch_seconds"), "gauge");
    appendMetricType(metrics, metricName("last_query_success_epoch_seconds"), "gauge");
    appendMetricType(metrics, metricName("last_ingest_status_code"), "gauge");
    appendMetricType(metrics, metricName("last_query_status_code"), "gauge");
    appendMetricType(metrics, metricName("ingest_requests_total"), "counter");
    appendMetricType(metrics, metricName("ingest_batches_succeeded_total"), "counter");
    appendMetricType(metrics, metricName("ingest_batches_failed_total"), "counter");
    appendMetricType(metrics, metricName("ingest_catchup_batches_total"), "counter");
    appendMetricType(metrics, metricName("ingest_docs_accepted_total"), "counter");
    appendMetricType(metrics, metricName("ingest_docs_failed_total"), "counter");
    appendMetricType(metrics, metricName("query_requests_total"), "counter");
    appendMetricType(metrics, metricName("query_succeeded_total"), "counter");
    appendMetricType(metrics, metricName("query_failed_total"), "counter");
    appendMetricType(metrics, metricName("bucket_expected_docs"), "gauge");
    appendMetricType(metrics, metricName("bucket_observed_docs"), "gauge");
    appendMetricType(metrics, metricName("bucket_doc_delta"), "gauge");
    appendMetricType(metrics, metricName("bucket_doc_ratio"), "gauge");
    appendMetricType(metrics, metricName("bucket_epoch_seconds"), "gauge");

    appendGauge(metrics, metricName("up"), stopped ? 0 : 1);
    appendGauge(metrics, metricName("window_ready"), snapshot.ready() ? 1 : 0);
    appendGauge(metrics, metricName("window_min_ratio"), snapshot.minRatio());
    appendGauge(metrics, metricName("window_max_abs_delta_docs"), snapshot.maxAbsDeltaDocs());
    appendGauge(
        metrics,
        metricName("window_buckets_with_expected_docs"),
        snapshot.bucketsWithExpectedDocs());
    appendGauge(
        metrics,
        metricName("ingest_coverage_backlog_buckets"),
        ingestCoverageBacklogBuckets(activeBucketStartMs));
    appendGauge(
        metrics, metricName("active_ingest_bucket_epoch_seconds"), activeBucketStartMs / 1000.0);
    appendGauge(
        metrics,
        metricName("last_ingest_success_epoch_seconds"),
        lastIngestSuccessEpochMs.get() / 1000.0);
    appendGauge(
        metrics,
        metricName("last_query_success_epoch_seconds"),
        lastQuerySuccessEpochMs.get() / 1000.0);
    appendGauge(metrics, metricName("last_ingest_status_code"), lastIngestStatusCode.get());
    appendGauge(metrics, metricName("last_query_status_code"), lastQueryStatusCode.get());

    appendCounter(metrics, metricName("ingest_requests_total"), ingestRequestsTotal.sum());
    appendCounter(
        metrics, metricName("ingest_batches_succeeded_total"), ingestBatchesSucceededTotal.sum());
    appendCounter(
        metrics, metricName("ingest_batches_failed_total"), ingestBatchesFailedTotal.sum());
    appendCounter(
        metrics, metricName("ingest_catchup_batches_total"), ingestCatchupBatchesTotal.sum());
    appendCounter(metrics, metricName("ingest_docs_accepted_total"), ingestDocsAcceptedTotal.sum());
    appendCounter(metrics, metricName("ingest_docs_failed_total"), ingestDocsFailedTotal.sum());
    appendCounter(metrics, metricName("query_requests_total"), queryRequestsTotal.sum());
    appendCounter(metrics, metricName("query_succeeded_total"), querySucceededTotal.sum());
    appendCounter(metrics, metricName("query_failed_total"), queryFailedTotal.sum());

    for (BucketSnapshot bucket : snapshot.buckets()) {
      Map<String, String> labels =
          Map.of("bucket_age_minutes", String.valueOf(bucket.bucketAgeMinutes()));
      appendGauge(metrics, metricName("bucket_expected_docs"), labels, bucket.expectedDocs());
      appendGauge(metrics, metricName("bucket_observed_docs"), labels, bucket.observedDocs());
      appendGauge(metrics, metricName("bucket_doc_delta"), labels, bucket.deltaDocs());
      appendGauge(metrics, metricName("bucket_doc_ratio"), labels, bucket.ratio());
      appendGauge(
          metrics, metricName("bucket_epoch_seconds"), labels, bucket.bucketStartMs() / 1000.0);
    }

    return metrics.toString();
  }

  private WindowSnapshot buildWindowSnapshot(long activeBucketStartMs) {
    List<BucketSnapshot> buckets = new ArrayList<>();
    double minRatio = Double.POSITIVE_INFINITY;
    long maxAbsDeltaDocs = 0L;
    int bucketsWithExpectedDocs = 0;

    for (int age = config.newestBucketAgeMinutes();
        age <= config.oldestCheckedBucketAgeMinutes();
        age++) {
      long bucketStartMs = activeBucketStartMs - (long) age * MINUTE_MS;
      BucketStats bucketStats = bucketStatsByStartMs.get(bucketStartMs);
      long expectedDocs = bucketStats == null ? 0L : bucketStats.expectedTargetDocs.sum();
      long observedDocs = bucketStats == null ? 0L : bucketStats.observedTargetDocs.get();
      long deltaDocs = observedDocs - expectedDocs;
      double ratio = expectedDocs == 0L ? 0.0 : (double) observedDocs / (double) expectedDocs;

      if (expectedDocs > 0L) {
        minRatio = Math.min(minRatio, ratio);
        bucketsWithExpectedDocs++;
      }
      maxAbsDeltaDocs = Math.max(maxAbsDeltaDocs, Math.abs(deltaDocs));
      buckets.add(
          new BucketSnapshot(age, bucketStartMs, expectedDocs, observedDocs, deltaDocs, ratio));
    }

    if (!Double.isFinite(minRatio)) {
      minRatio = 0.0;
    }
    return new WindowSnapshot(
        buckets,
        bucketsWithExpectedDocs == config.windowBuckets(),
        bucketsWithExpectedDocs,
        minRatio,
        maxAbsDeltaDocs);
  }

  private boolean windowReady() {
    long activeBucketStartMs = activeBucketStartMs(clock.millis());
    for (int age = config.newestBucketAgeMinutes();
        age <= config.oldestCheckedBucketAgeMinutes();
        age++) {
      long bucketStartMs = activeBucketStartMs - (long) age * MINUTE_MS;
      BucketStats bucketStats = bucketStatsByStartMs.get(bucketStartMs);
      if (bucketStats == null || bucketStats.expectedTargetDocs.sum() <= 0L) {
        return false;
      }
    }
    return true;
  }

  private void pruneOldBuckets(long nowMs) {
    long activeBucketStartMs = activeBucketStartMs(nowMs);
    long keepAfterMs =
        activeBucketStartMs - (long) (config.oldestCheckedBucketAgeMinutes() + 3) * MINUTE_MS;
    bucketStatsByStartMs.keySet().removeIf(bucketStartMs -> bucketStartMs < keepAfterMs);
  }

  private long ingestCoverageBacklogBuckets(long activeBucketStartMs) {
    long lastCoveredBucketStartMs = this.lastCoveredBucketStartMs.get();
    if (lastCoveredBucketStartMs == NO_COVERED_BUCKET
        || lastCoveredBucketStartMs >= activeBucketStartMs) {
      return 0L;
    }
    return (activeBucketStartMs - lastCoveredBucketStartMs) / MINUTE_MS;
  }

  private long activeBucketStartMs(long nowMs) {
    return truncateToMinute(nowMs);
  }

  private static long truncateToMinute(long epochMs) {
    return epochMs - Math.floorMod(epochMs, MINUTE_MS);
  }

  private static String quotedQueryStringValue(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  private static String metricName(String suffix) {
    return METRIC_PREFIX + suffix;
  }

  private static void appendCounter(StringBuilder metrics, String name, long value) {
    appendMetricSample(metrics, name, Map.of(), (double) value);
  }

  private static void appendGauge(StringBuilder metrics, String name, long value) {
    appendMetricSample(metrics, name, Map.of(), (double) value);
  }

  private static void appendGauge(StringBuilder metrics, String name, double value) {
    appendMetricSample(metrics, name, Map.of(), value);
  }

  private static void appendGauge(
      StringBuilder metrics, String name, Map<String, String> labels, long value) {
    appendMetricSample(metrics, name, labels, (double) value);
  }

  private static void appendGauge(
      StringBuilder metrics, String name, Map<String, String> labels, double value) {
    appendMetricSample(metrics, name, labels, value);
  }

  private static void appendMetricType(StringBuilder metrics, String name, String type) {
    metrics.append("# TYPE ").append(name).append(' ').append(type).append('\n');
  }

  private static void appendMetricSample(
      StringBuilder metrics, String name, Map<String, String> labels, double value) {
    metrics.append(name);
    if (!labels.isEmpty()) {
      metrics.append('{');
      boolean first = true;
      for (Map.Entry<String, String> entry : labels.entrySet()) {
        if (!first) {
          metrics.append(',');
        }
        metrics
            .append(entry.getKey())
            .append("=\"")
            .append(entry.getValue().replace("\\", "\\\\").replace("\"", "\\\""))
            .append('"');
        first = false;
      }
      metrics.append('}');
    }
    metrics.append(' ').append(value).append('\n');
  }

  private record IngestBatch(
      long bucketStartMs, int totalDocs, int targetDocs, String requestBody) {}

  private record BucketSnapshot(
      int bucketAgeMinutes,
      long bucketStartMs,
      long expectedDocs,
      long observedDocs,
      long deltaDocs,
      double ratio) {}

  private record WindowSnapshot(
      List<BucketSnapshot> buckets,
      boolean ready,
      int bucketsWithExpectedDocs,
      double minRatio,
      long maxAbsDeltaDocs) {}

  private static final class BucketStats {
    private final LongAdder expectedTargetDocs = new LongAdder();
    private final AtomicLong observedTargetDocs = new AtomicLong();
  }

  record Config(
      URI bulkUri,
      URI queryUri,
      String index,
      String runId,
      String targetHostname,
      String distractorHostname,
      String hostnameField,
      String metricsHost,
      int metricsPort,
      int batchSize,
      int windowBuckets,
      int newestBucketAgeMinutes,
      int maxCatchupBucketsPerCycle,
      int messageBytes,
      long startId,
      Duration ingestInterval,
      Duration queryInterval,
      Duration connectTimeout,
      Duration requestTimeout) {

    Config {
      if (windowBuckets < 1) {
        throw new IllegalArgumentException("windowBuckets must be at least 1");
      }
      if (newestBucketAgeMinutes < 1) {
        throw new IllegalArgumentException("newestBucketAgeMinutes must be at least 1");
      }
      if (maxCatchupBucketsPerCycle < 1) {
        throw new IllegalArgumentException("maxCatchupBucketsPerCycle must be at least 1");
      }
    }

    int oldestCheckedBucketAgeMinutes() {
      return newestBucketAgeMinutes + windowBuckets - 1;
    }

    private static Config fromEnvironment() {
      String runId =
          Env.get("SYNTHETIC_DATA_PROBE_RUN_ID", "synthetic" + System.currentTimeMillis());
      String distractorHostname =
          Env.get("DISTRACTOR_HOSTNAME", runId + ".other.synthetic-data-probe.test");
      return new Config(
          URI.create(Env.get("KALDB_BULK_URL", "http://localhost:8086/_bulk")),
          URI.create(Env.get("KALDB_QUERY_URL", "http://localhost:8081/_msearch")),
          Env.get("INDEX", "logs"),
          runId,
          Env.get("TARGET_HOSTNAME", runId + ".target.synthetic-data-probe.test"),
          distractorHostname,
          Env.get("HOSTNAME_FIELD", "hostname"),
          Env.get("METRICS_HOST", "0.0.0.0"),
          Env.getInt("METRICS_PORT", 9464),
          Env.getInt("BATCH_SIZE", 50),
          Env.getInt("WINDOW_BUCKETS", 5),
          Env.getInt("NEWEST_BUCKET_AGE_MINUTES", 1),
          Env.getInt("MAX_CATCHUP_BUCKETS_PER_CYCLE", 3),
          Env.getInt("MESSAGE_BYTES", 256),
          Env.getLong("START_ID", 1),
          Duration.ofMillis(Math.round(Env.getDouble("INTERVAL_SEC", 1.0) * 1000.0)),
          Duration.ofMillis(Math.round(Env.getDouble("QUERY_INTERVAL_SEC", 10.0) * 1000.0)),
          Duration.ofSeconds(Env.getInt("CONNECT_TIMEOUT_SEC", 5)),
          Duration.ofSeconds(Env.getInt("REQUEST_TIMEOUT_SEC", 15)));
    }
  }
}
