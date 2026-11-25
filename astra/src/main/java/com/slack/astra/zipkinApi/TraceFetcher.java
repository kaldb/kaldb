package com.slack.astra.zipkinApi;

import static com.slack.astra.metadata.dataset.DatasetPartitionMetadata.MATCH_ALL_DATASET;

import brave.Tracing;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.VisibleForTesting;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(TraceFetcher.class);

  private final AstraQueryServiceBase searcher;
  private final BlobStore blobStore;

  private final int defaultMaxSpans;
  private final int defaultLookbackMins;
  private final long defaultDataFreshnessInSeconds;

  public static final String TRACE_CACHE_PREFIX = "traceCacheData";

  public TraceFetcher(
      AstraQueryServiceBase searcher,
      BlobStore blobStore,
      int defaultMaxSpans,
      int defaultLookbackMins,
      long defaultDataFreshnessInSeconds) {
    this.searcher = searcher;
    this.blobStore = blobStore;
    this.defaultMaxSpans = defaultMaxSpans;
    this.defaultLookbackMins = defaultLookbackMins;
    this.defaultDataFreshnessInSeconds = defaultDataFreshnessInSeconds;
  }

  private static record Result(String rawJson, List<ZipkinSpanResponse> spans) {
    Result(String rawJson) {
      this(rawJson, null);
    }

    Result(List<ZipkinSpanResponse> spans) {
      this(null, spans);
    }

    boolean isCached() {
      return rawJson != null;
    }
  }

  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          // sort alphabetically for easier test asserts
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          // don't serialize null values or empty maps
          .serializationInclusion(JsonInclude.Include.NON_EMPTY)
          .build();

  // DD trace id is a long encoded as a string.
  private static final Pattern DIGITS = Pattern.compile("^\\d+$");

  private static boolean isDDTraceId(String s) {
    return s != null && DIGITS.matcher(s).matches();
  }

  private record TraceIds(String hex, String base64) {}

  private static TraceIds convertTraceId(String traceId) {
    if (traceId == null || traceId.isEmpty()) return null;

    String hex = null;
    String base64Url = null;

    // If input is hex → convert to Base64 URL-safe
    if (traceId.matches("^[0-9a-fA-F]+$") && traceId.length() % 2 == 0) {
      try {
        byte[] bytes = Hex.decodeHex(traceId.toCharArray());
        hex = traceId.toLowerCase();
        base64Url = Base64.getUrlEncoder().encodeToString(bytes);
        return new TraceIds(hex, base64Url);
      } catch (Exception ignored) {
        return null;
      }
    }

    // Otherwise, assume Base64-URL → convert to hex
    try {
      byte[] bytes = Base64.getUrlDecoder().decode(traceId);
      hex = Hex.encodeHexString(bytes);
      base64Url = traceId;
      return new TraceIds(hex, base64Url);
    } catch (Exception ignored) {
      return null;
    }
  }

  private static JSONObject singleTermQuery(String traceFieldName, String traceId) {
    JSONObject traceObject = new JSONObject();
    traceObject.put(traceFieldName, traceId);
    JSONObject queryJson = new JSONObject();
    queryJson.put("term", traceObject);
    return queryJson;
  }

  private static JSONObject buildTraceIdQuery(
      String traceFieldName, String traceId, String convertedId) {
    // When there is no converted ID, return the original simple term query
    if (convertedId == null) {
      return singleTermQuery(traceFieldName, traceId);
    }

    // When we have a convertedId, do traceId OR convertedId
    JSONArray shouldArray = new JSONArray();
    // term for original traceId
    shouldArray.put(singleTermQuery(traceFieldName, traceId));
    // term for convertedId
    shouldArray.put(singleTermQuery(traceFieldName, convertedId));
    JSONObject boolQuery = new JSONObject();
    boolQuery.put("should", shouldArray);
    boolQuery.put("minimum_should_match", 1);

    JSONObject queryJson = new JSONObject();
    queryJson.put("bool", boolQuery);

    return queryJson;
  }

  private Result fetchTraceResult(
      String traceId,
      Optional<Long> startTimeEpochMs,
      Optional<Long> endTimeEpochMs,
      Optional<Integer> maxSpans,
      Optional<Boolean> userRequest,
      Optional<Long> dataFreshnessInSeconds,
      Optional<Boolean> ddTraceIdEnabled,
      Optional<Boolean> searchByHexAndBase64)
      throws IOException {

    String traceFieldName = "trace_id";
    String convertedId = null;
    // if trace id looks like dd_trace_id, then use dd_trace_id field to search. If true, ignores
    // the hex/base64 flag
    if (ddTraceIdEnabled.isPresent() && ddTraceIdEnabled.get() && isDDTraceId(traceId)) {
      traceFieldName = "dd_trace_id";
    } else if (searchByHexAndBase64.isPresent() && searchByHexAndBase64.get()) {
      TraceIds traceIds = convertTraceId(traceId);
      // to ensure we only cache the same trace once, we use the base64 version as the traceId.
      // TraceIds are null
      // in the case of being unable to convert, fallback to original traceId
      if (traceIds != null) {
        traceId = traceIds.base64;
        convertedId = traceIds.hex;
      }
    }

    // Log the custom header userRequest value if present
    if (userRequest.isPresent()) {
      LOG.info("Received custom header X-User-Request: {}", userRequest.get());

      // try to retrieve trace data from blob store cache; check timestamp before using blob store
      // cache for data freshness
      String traceData = retrieveDataFromBlobStoreCache(traceId);
      if (traceData != null) {
        LOG.info("Trace data retrieved from blob store cache for traceId={}", traceId);
        return new Result(traceData);
      }
    }

    JSONObject queryJson = buildTraceIdQuery(traceFieldName, traceId, convertedId);
    String queryString = queryJson.toString();
    LOG.debug("Querying with queryString={}", queryString);

    long startTime =
        startTimeEpochMs.orElseGet(
            () -> Instant.now().minus(this.defaultLookbackMins, ChronoUnit.MINUTES).toEpochMilli());
    // we are adding a buffer to end time also because some machines clock may be ahead of current
    // system clock and those spans would be stored but can't be queried

    long endTime =
        endTimeEpochMs.orElseGet(
            () -> Instant.now().plus(this.defaultLookbackMins, ChronoUnit.MINUTES).toEpochMilli());
    int howMany = maxSpans.orElse(this.defaultMaxSpans);

    brave.Span span = Tracing.currentTracer().currentSpan();
    span.tag("startTimeEpochMs", String.valueOf(startTime));
    span.tag("endTimeEpochMs", String.valueOf(endTime));
    span.tag("howMany", String.valueOf(howMany));
    // Add custom header to span tags if present
    userRequest.ifPresent(headerValue -> span.tag("userRequest", headerValue.toString()));

    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();
    AstraSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset(MATCH_ALL_DATASET)
                .setQuery(queryString)
                .setStartTimeEpochMs(startTime)
                .setEndTimeEpochMs(endTime)
                .setHowMany(howMany)
                .build());

    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    List<ZipkinSpanResponse> spans = convertLogWireMessageToZipkinSpan(messages);

    if (userRequest.isPresent() && userRequest.get() && !messages.isEmpty()) {
      long dataFreshnessInSecondsValue =
          dataFreshnessInSeconds.orElse(
              this.defaultDataFreshnessInSeconds); // default to 15 minutes if not
      // Check if no new spans in trace data, it can be saved
      Instant latestSpanTimestamp = getLatestSpanTimestamp(messages);

      if (shouldSaveToBlobStoreCache(latestSpanTimestamp, dataFreshnessInSecondsValue)) {
        LOG.info(
            "Data freshness check done, can be saved to blob store cache for traceId={}", traceId);
        // Save the trace data to blob store cache
        saveDataToBlobStoreCache(traceId, objectMapper.writeValueAsString(spans));
      }
    }

    return new Result(spans);
  }

  public List<ZipkinSpanResponse> getSpansByTraceId(
      String traceId,
      Optional<Long> startTimeEpochMs,
      Optional<Long> endTimeEpochMs,
      Optional<Integer> maxSpans,
      Optional<Boolean> userRequest,
      Optional<Long> dataFreshnessInSeconds,
      Optional<Boolean> ddTraceIdEnabled,
      Optional<Boolean> searchByHexAndBase64)
      throws IOException {
    Result result =
        fetchTraceResult(
            traceId,
            startTimeEpochMs,
            endTimeEpochMs,
            maxSpans,
            userRequest,
            dataFreshnessInSeconds,
            ddTraceIdEnabled,
            searchByHexAndBase64);

    if (result.isCached()) {
      return objectMapper.readValue(result.rawJson, new TypeReference<>() {});
    }
    return result.spans;
  }

  public String getByTraceId(
      String traceId,
      Optional<Long> startTimeEpochMs,
      Optional<Long> endTimeEpochMs,
      Optional<Integer> maxSpans,
      Optional<Boolean> userRequest,
      Optional<Long> dataFreshnessInSeconds,
      Optional<Boolean> ddTraceIdEnabled,
      Optional<Boolean> searchByHexAndBase64)
      throws IOException {
    Result result =
        fetchTraceResult(
            traceId,
            startTimeEpochMs,
            endTimeEpochMs,
            maxSpans,
            userRequest,
            dataFreshnessInSeconds,
            ddTraceIdEnabled,
            searchByHexAndBase64);

    if (result.isCached()) {
      return result.rawJson;
    }
    return objectMapper.writeValueAsString(result.spans);
  }

  protected static List<ZipkinSpanResponse> convertLogWireMessageToZipkinSpan(
      List<LogWireMessage> messages) throws JsonProcessingException {
    List<ZipkinSpanResponse> traces = new ArrayList<>(messages.size());
    for (LogWireMessage message : messages) {
      if (message.getId() == null) {
        LOG.warn("Document={} cannot have missing id ", message);
        continue;
      }

      String id = message.getId();
      String messageTraceId = null;
      String parentId = null;
      String name = null;
      String serviceName = null;
      String timestamp = String.valueOf(message.getTimestamp().toEpochMilli());
      long duration = 0L;
      Map<String, String> messageTags = new HashMap<>();

      for (String k : message.getSource().keySet()) {
        Object value = message.getSource().get(k);
        if (LogMessage.ReservedField.TRACE_ID.fieldName.equals(k)) {
          messageTraceId = (String) value;
        } else if (LogMessage.ReservedField.PARENT_ID.fieldName.equals(k)) {
          parentId = (String) value;
        } else if (LogMessage.ReservedField.NAME.fieldName.equals(k)) {
          name = (String) value;
        } else if (LogMessage.ReservedField.SERVICE_NAME.fieldName.equals(k)) {
          serviceName = (String) value;
        } else if (LogMessage.ReservedField.DURATION.fieldName.equals(k)) {
          duration = ((Number) value).longValue();
        } else if (LogMessage.ReservedField.ID.fieldName.equals(k)) {
          id = (String) value;
        } else {
          messageTags.put(k, String.valueOf(value));
        }
      }

      // TODO: today at Slack the duration is sent as "duration_ms"
      // We we have this special handling which should be addressed upstream
      // and then removed from here
      if (duration == 0) {
        Object value =
            message.getSource().getOrDefault(LogMessage.ReservedField.DURATION_MS.fieldName, 0);
        duration = TimeUnit.MICROSECONDS.convert(Duration.ofMillis(((Number) value).intValue()));
      }

      // these are some mandatory fields without which the grafana zipkin plugin fails to display
      // the span
      if (messageTraceId == null) {
        messageTraceId = message.getId();
      }
      if (timestamp == null) {
        LOG.warn(
            "Document id={} missing {}",
            message,
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
        continue;
      }

      final ZipkinSpanResponse span = new ZipkinSpanResponse(id, messageTraceId);
      span.setParentId(parentId);
      span.setName(name);
      if (serviceName != null) {
        ZipkinEndpointResponse remoteEndpoint = new ZipkinEndpointResponse();
        remoteEndpoint.setServiceName(serviceName);
        span.setRemoteEndpoint(remoteEndpoint);
      }
      span.setTimestamp(convertToMicroSeconds(message.getTimestamp()));
      span.setDuration(duration);
      span.setTags(messageTags);
      traces.add(span);
    }

    return traces;
  }

  // returning LogWireMessage instead of LogMessage
  // If we return LogMessage the caller then needs to call getSource which is a deep copy of the
  // object. To return LogWireMessage we do a JSON parse
  static List<LogWireMessage> searchResultToLogWireMessage(AstraSearch.SearchResult searchResult)
      throws IOException {
    List<LogWireMessage> messages = new ArrayList<>(searchResult.getHitsCount());
    for (AstraSearch.SearchResult.Hit searchResultHit : searchResult.getHitsList()) {
      LogWireMessage hit = JsonUtil.read(searchResultHit.getMessage(), LogWireMessage.class);
      // LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(hit);
    }
    return messages;
  }

  @VisibleForTesting
  protected static long convertToMicroSeconds(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  @VisibleForTesting
  protected Instant getLatestSpanTimestamp(List<LogWireMessage> spanList) {
    return spanList.stream()
        .map(LogWireMessage::getTimestamp)
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  protected String retrieveDataFromBlobStoreCache(String traceId) {
    assert traceId != null && !traceId.isEmpty();

    try {
      // Retrieve the compressed trace data from blob store cache
      String jsonData =
          blobStore.readFileData(
              String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId), true);

      if (jsonData == null || jsonData.isEmpty()) {
        LOG.warn("No trace data found in blob store cache for traceId={}", traceId);
        return null;
      }
      LOG.info(
          "Retrieved and decompressed trace data from blob store cache for traceId={}", traceId);
      return jsonData;

    } catch (Exception e) {
      // Log other exceptions as errors
      LOG.error("Error retrieving trace data from blob store cache for traceId={}", traceId, e);
      return null;
    }
  }

  private static boolean shouldSaveToBlobStoreCache(
      Instant latestSpanTimestamp, long dataFreshnessInSeconds) {
    Instant currentTime = Instant.now();
    return latestSpanTimestamp.isBefore(
        currentTime.minus(dataFreshnessInSeconds, ChronoUnit.SECONDS));
  }

  protected void saveDataToBlobStoreCache(String traceId, String output) {
    assert traceId != null && !traceId.isEmpty();
    assert output != null && !output.isEmpty();

    try {
      // Upload the compressed trace data to blob store cache
      String srcLocation =
          String.format("%s/tmp-%s/%s.json.gz", TRACE_CACHE_PREFIX, traceId, UUID.randomUUID());
      String dstLocation = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);

      if (blobStore.pathExists("%s/tmp-%s".formatted(TRACE_CACHE_PREFIX, traceId))) {
        LOG.info("Temporary location found in blob store cache for traceId={}", traceId);
        return;
      }

      blobStore.uploadData(srcLocation, output, true);
      blobStore.copyFile(srcLocation, dstLocation);
      blobStore.delete(String.format("%s/tmp-%s", TRACE_CACHE_PREFIX, traceId));

      LOG.info("Compressed trace data saved to blob store cache for traceId={}", traceId);

    } catch (Exception e) {
      LOG.error("Error saving trace data to blob store cache for traceId={}", traceId, e);
      throw new RuntimeException("Failed to save trace data to blob store cache", e);
    }
  }
}
