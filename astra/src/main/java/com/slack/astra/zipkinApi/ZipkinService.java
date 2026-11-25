package com.slack.astra.zipkinApi;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
/**
 * Zipkin compatible API service
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/zipkin/datasource.ts">Grafana
 *     Zipkin API</a> <a
 *     href="https://github.com/openzipkin/zipkin-api/blob/master/zipkin.proto">Trace proto
 *     compatible with Zipkin</a> <a href="https://zipkin.io/zipkin-api/#/">Trace API Swagger
 *     Hub</a>
 */
public class ZipkinService {
  private static final Logger LOG = LoggerFactory.getLogger(ZipkinService.class);
  private final TraceFetcher traceFetcher;

  public ZipkinService(TraceFetcher traceFetcher) {
    this.traceFetcher = traceFetcher;
  }

  @Get
  @Path("/api/v2/services")
  public HttpResponse getServices() throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/spans")
  public HttpResponse getSpans(@Param("serviceName") Optional<String> serviceName)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/traces")
  public HttpResponse getTraces(
      @Param("serviceName") Optional<String> serviceName,
      @Param("spanName") Optional<String> spanName,
      @Param("annotationQuery") Optional<String> annotationQuery,
      @Param("minDuration") Optional<Integer> minDuration,
      @Param("maxDuration") Optional<Integer> maxDuration,
      @Param("endTs") Long endTs,
      @Param("lookback") Long lookback,
      @Param("limit") @Default("10") Integer limit)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Blocking
  @Get("/api/v2/trace/{traceId}")
  public HttpResponse getTraceByTraceId(
      @Param("traceId") String traceId,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("maxSpans") Optional<Integer> maxSpans,
      @Header("X-User-Request") Optional<Boolean> userRequest,
      @Header("X-Data-Freshness-In-Seconds") Optional<Long> dataFreshnessInSeconds,
      @Header("X-DD-TRACE-ID") Optional<Boolean> ddTraceIdEnabled,
      @Header("X-E2E-hex-base64") Optional<Boolean> searchByHexAndBase64)
      throws IOException {
    String output =
        this.traceFetcher.getByTraceId(
            traceId,
            startTimeEpochMs,
            endTimeEpochMs,
            maxSpans,
            userRequest,
            dataFreshnessInSeconds,
            ddTraceIdEnabled,
            searchByHexAndBase64);
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }
}
