package com.slack.astra.graphApi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Param;
import com.slack.astra.zipkinApi.TraceFetcher;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  APIs for exposing traces and their spans as subgraph dependencies.
*/
public class GraphService {
  private static final Logger LOG = LoggerFactory.getLogger(GraphService.class);
  private final TraceFetcher traceFetcher;
  private final GraphBuilder graphBuilder;
  private final MeterRegistry meterRegistry;
  private final Timer traceFetchTimer;
  private final Timer graphBuildTimer;

  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          // sort alphabetically for easier test asserts
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          // don't serialize null values or empty maps
          .serializationInclusion(JsonInclude.Include.NON_EMPTY)
          .build();

  public GraphService(
      TraceFetcher traceFetcher, GraphConfig graphConfig, MeterRegistry meterRegistry) {
    this.traceFetcher = traceFetcher;
    this.graphBuilder = new GraphBuilder(graphConfig);
    this.meterRegistry = meterRegistry;
    this.traceFetchTimer = meterRegistry.timer("astra_graph_service_trace_fetch");
    this.graphBuildTimer = meterRegistry.timer("astra_graph_service_graph_build");

    LOG.info("Started GraphService with GraphBuilder config: {}", graphConfig);
  }

  private record SubgraphResponse(
      Graph subgraph, long traceFetchTimeMs, long subgraphBuildTimeMs) {}

  @Blocking
  @Get("/api/v1/trace/{traceId}/subgraph")
  public HttpResponse getSubgraph(
      @Param("traceId") String traceId,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("buildFilter") Optional<String> buildFilterJson,
      @Param("maxSpans") Optional<Integer> maxSpans,
      @Header("X-User-Request") Optional<Boolean> userRequest)
      throws IOException {
    // Parse the filter from JSON string if provided
    Optional<GraphBuilder.Filter> buildFilter = Optional.empty();
    if (buildFilterJson.isPresent()) {
      try {
        // Parse JSON as Map<String, List<String>> and create GraphConfig.Filter
        TypeReference<Map<String, List<String>>> typeRef = new TypeReference<>() {};
        Map<String, List<String>> filterMap =
            objectMapper.readValue(buildFilterJson.get(), typeRef);
        buildFilter = Optional.of(new GraphBuilder.Filter(filterMap));
      } catch (Exception e) {
        LOG.error("Failed to parse buildFilter JSON: {}", buildFilterJson.get(), e);
        return HttpResponse.of(
            HttpStatus.BAD_REQUEST,
            MediaType.PLAIN_TEXT_UTF_8,
            "Invalid buildFilter JSON: " + e.getMessage());
      }
    }

    Timer.Sample traceFetchSample = Timer.start(meterRegistry);
    List<ZipkinSpanResponse> trace =
        this.traceFetcher.getSpansByTraceId(
            traceId,
            startTimeEpochMs,
            endTimeEpochMs,
            maxSpans,
            userRequest,
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    long traceFetchTimeMilli = traceFetchSample.stop(traceFetchTimer) / 1_000_000;

    Timer.Sample graphBuildSample = Timer.start(meterRegistry);
    Graph subgraph = this.graphBuilder.buildFromSpans(trace, buildFilter);
    long subgraphBuildTimeMilli = graphBuildSample.stop(graphBuildTimer) / 1_000_000;

    SubgraphResponse response =
        new SubgraphResponse(subgraph, traceFetchTimeMilli, subgraphBuildTimeMilli);
    String output = objectMapper.writeValueAsString(response);
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }
}
