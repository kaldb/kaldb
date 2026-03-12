package com.slack.astra.elasticsearchApi;

import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.astra.elasticsearchApi.searchResponse.EsSearchResponse;
import com.slack.astra.elasticsearchApi.searchResponse.HitsMetadata;
import com.slack.astra.elasticsearchApi.searchResponse.SearchResponseHit;
import com.slack.astra.elasticsearchApi.searchResponse.SearchResponseMetadata;
import com.slack.astra.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.StructuredTaskScope;
import org.opensearch.search.aggregations.InternalAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elasticsearch compatible API service, for use in Grafana
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/elasticsearch/datasource.ts">Grafana
 *     ES API</a>
 */
@SuppressWarnings(
    "OptionalUsedAsFieldOrParameterType") // Per https://armeria.dev/docs/server-annotated-service/
public class ElasticsearchApiService {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApiService.class);
  // Keep this in sync with the OpenSearch and OpenSearch Dashboards image versions in
  // docker-compose.yml. Dashboards uses the reported backend version during its startup handshake.
  private static final String OPEN_SEARCH_COMPAT_VERSION = "2.11.1";
  // Keep this in sync with astra/pom.xml's lucene.version property.
  private static final String OPEN_SEARCH_COMPAT_LUCENE_VERSION = "9.7.0";
  private final AstraQueryServiceBase searcher;
  private final CompatibilityMetadata compatibilityMetadata;
  private final OpenSearchSchemaAdapter openSearchSchemaAdapter;

  private final OpenSearchRequest openSearchRequest = new OpenSearchRequest();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ElasticsearchApiService(
      AstraQueryServiceBase searcher, AstraConfigs.AstraConfig astraConfig) {
    this.searcher = searcher;
    this.openSearchSchemaAdapter = new OpenSearchSchemaAdapter(searcher);
    this.compatibilityMetadata = CompatibilityMetadata.fromConfig(astraConfig);
  }

  /** Returns metadata about the cluster */
  @Get
  @Path("/")
  public HttpResponse clusterMetadata() throws IOException {
    // number must validate with npm semver validate for Grafana compatibility. We expose an
    // OpenSearch-compatible version here so OpenSearch Dashboards can complete its node checks.
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(cluster()));
  }

  /** Returns node metadata about the cluster */
  @Get
  @Path("/_nodes")
  public HttpResponse nodesInfo() throws IOException {
    return HttpResponse.of(
        HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(nodesInfoBody()));
  }

  /** Returns node metadata about the cluster */
  @Get
  @Path("/_nodes/:nodeId")
  public HttpResponse nodesInfoById(@Param("nodeId") String nodeId) throws IOException {
    // Astra only exposes one synthetic compatibility node, so the requested path id is ignored.
    return nodesInfo();
  }

  /** Returns node metadata about the cluster */
  @Get
  @Path("/_nodes/:nodeId/:metric")
  public HttpResponse nodesInfoByIdAndMetric(
      @Param("nodeId") String nodeId, @Param("metric") String metric) throws IOException {
    // Astra only exposes one synthetic compatibility node, so the requested path params are
    // ignored and the same metadata is returned for any id/metric combination.
    return nodesInfo();
  }

  /** Returns cluster state for node discovery */
  @Get
  @Path("/_cluster/state/nodes")
  public HttpResponse clusterStateNodes() throws IOException {
    // This is a static compatibility identifier rather than a true ephemeral id. Dashboards only
    // needs a stable OpenSearch-like shape here, not Astra's real cluster membership model.
    var node =
        new ClusterStateResponse.NodeInfo(
            compatibilityMetadata.nodeName(),
            compatibilityMetadata.ephemeralNodeId(),
            compatibilityMetadata.transportAddress(),
            Map.of("cluster_id", compatibilityMetadata.clusterUuid()));
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        JsonUtil.writeAsString(
            new ClusterStateResponse(
                compatibilityMetadata.clusterName(),
                Map.of(compatibilityMetadata.nodeId(), node))));
  }

  /**
   * Multisearch API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">API
   *     doc</a>
   */
  @Post
  @Blocking
  @Path("/_msearch")
  public HttpResponse multiSearch(String postBody) throws Exception {
    LOG.debug("Search request: {}", postBody);

    CurrentTraceContext currentTraceContext = Tracing.current().currentTraceContext();
    try (var scope = new StructuredTaskScope<EsSearchResponse>()) {
      List<StructuredTaskScope.Subtask<EsSearchResponse>> requestSubtasks =
          openSearchRequest.parseMultiSearchRequest(postBody).stream()
              .map((request) -> scope.fork(currentTraceContext.wrap(() -> doSearch(request))))
              .toList();

      scope.join();
      SearchResponseMetadata responseMetadata =
          new SearchResponseMetadata(
              0,
              requestSubtasks.stream().map(StructuredTaskScope.Subtask::get).toList(),
              Map.of("traceId", getTraceId()));
      return HttpResponse.of(
          HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(responseMetadata));
    }
  }

  /**
   * Search API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">API
   *     doc</a>
   */
  @Post
  @Blocking
  @Path("/_search")
  public HttpResponse searchAll(String postBody) throws Exception {
    return singleSearch("_all", postBody);
  }

  /**
   * Search API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html">API
   *     doc</a>
   */
  @Post
  @Blocking
  @Path("/:indexName/_search")
  public HttpResponse search(@Param("indexName") String indexName, String postBody)
      throws Exception {
    return singleSearch(indexName, postBody);
  }

  private HttpResponse singleSearch(String dataset, String postBody) throws Exception {
    LOG.debug("Single search request dataset={} body={}", dataset, postBody);
    EsSearchResponse response =
        doSearch(openSearchRequest.parseSingleSearchRequest(dataset, postBody));
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(response));
  }

  private EsSearchResponse doSearch(AstraSearch.SearchRequest searchRequest) {
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("ElasticsearchApiService.doSearch");
    AstraSearch.SearchResult searchResult = searcher.doSearch(searchRequest);

    span.tag("requestDataset", searchRequest.getDataset());
    span.tag("requestHowMany", String.valueOf(searchRequest.getHowMany()));
    span.tag("resultHitsCount", String.valueOf(searchResult.getHitsCount()));
    span.tag("resultTookMicros", String.valueOf(searchResult.getTookMicros()));
    span.tag("resultFailedNodes", String.valueOf(searchResult.getFailedNodes()));
    span.tag("resultTotalNodes", String.valueOf(searchResult.getTotalNodes()));
    span.tag("resultTotalSnapshots", String.valueOf(searchResult.getTotalNodes()));
    span.tag(
        "resultSnapshotsWithReplicas", String.valueOf(searchResult.getSnapshotsWithReplicas()));

    try {
      HitsMetadata hits = getHits(searchResult);
      return new EsSearchResponse.Builder()
          .hits(hits)
          .aggregations(parseAggregations(searchResult.getInternalAggregations()))
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .debugMetadata(Map.of())
          .status(200)
          .build();
    } catch (Exception e) {
      LOG.error("Error fulfilling OpenSearch request", e);
      span.error(e);
      return new EsSearchResponse.Builder()
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .status(500)
          .build();
    } finally {
      span.finish();
    }
  }

  private JsonNode parseAggregations(ByteString byteInput) throws IOException {
    InternalAggregation internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(byteInput.toByteArray());
    if (internalAggregations != null) {
      return OBJECT_MAPPER.readTree(internalAggregations.toString());
    }
    return null;
  }

  private String getTraceId() {
    TraceContext traceContext = Tracing.current().currentTraceContext().get();
    if (traceContext != null) {
      return traceContext.traceIdString();
    }
    return "";
  }

  private HitsMetadata getHits(AstraSearch.SearchResult searchResult) throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<SearchResponseHit> responseHits = new ArrayList<>(hitsByteList.size());
    for (ByteString bytes : hitsByteList) {
      responseHits.add(SearchResponseHit.fromByteString(bytes));
    }

    return new HitsMetadata.Builder()
        .hitsTotal(ImmutableMap.of("value", responseHits.size(), "relation", "eq"))
        .hits(responseHits)
        .build();
  }

  /**
   * Mapping API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">API
   *     doc</a>
   */
  @Get
  @Path("/:indexName/_mapping")
  public HttpResponse mapping(
      @Param("indexName") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs)
      throws IOException {
    String resolvedIndexName = indexName.orElse("*");
    SchemaTimeRange schemaTimeRange = resolveSchemaTimeRange(startTimeEpochMs, endTimeEpochMs);
    Map<String, Map<String, String>> propertiesMap =
        openSearchSchemaAdapter.buildPropertiesMap(
            resolvedIndexName,
            schemaTimeRange.startTimeEpochMs(),
            schemaTimeRange.endTimeEpochMs());

    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        JsonUtil.writeAsString(
            ImmutableMap.of(
                resolvedIndexName,
                ImmutableMap.of("mappings", ImmutableMap.of("properties", propertiesMap)))));
  }

  /** Returns field aliases for index discovery */
  @Get
  @Path("/:indexName/_alias")
  public HttpResponse getIndexAliases(@Param("indexName") Optional<String> indexName)
      throws IOException {
    var resolvedIndexName = indexName.orElse("*");
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        JsonUtil.writeAsString(Map.of(resolvedIndexName, Map.of("aliases", Map.of()))));
  }

  /** Returns field capabilities for data views */
  @Get
  @Blocking
  @Path("/_field_caps")
  public HttpResponse fieldCapabilitiesForIndexQueryParam(
      @Param("index") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("fields") Optional<String> fields)
      throws IOException {
    return fieldCapabilitiesResponse(indexName, startTimeEpochMs, endTimeEpochMs, fields);
  }

  /** Returns field capabilities for data views */
  @Post
  @Blocking
  @Path("/_field_caps")
  public HttpResponse fieldCapabilitiesPostForIndexQueryParam(
      @Param("index") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("fields") Optional<String> fields)
      throws IOException {
    return fieldCapabilitiesResponse(indexName, startTimeEpochMs, endTimeEpochMs, fields);
  }

  /** Returns field capabilities for data views */
  @Get
  @Blocking
  @Path("/:indexName/_field_caps")
  public HttpResponse fieldCapabilitiesGet(
      @Param("indexName") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("fields") Optional<String> fields)
      throws IOException {
    return fieldCapabilitiesResponse(indexName, startTimeEpochMs, endTimeEpochMs, fields);
  }

  /** Returns field capabilities for data views */
  @Post
  @Blocking
  @Path("/:indexName/_field_caps")
  public HttpResponse fieldCapabilities(
      @Param("indexName") Optional<String> indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("fields") Optional<String> fields)
      throws IOException {
    return fieldCapabilitiesResponse(indexName, startTimeEpochMs, endTimeEpochMs, fields);
  }

  private HttpResponse fieldCapabilitiesResponse(
      Optional<String> indexName,
      Optional<Long> startTimeEpochMs,
      Optional<Long> endTimeEpochMs,
      Optional<String> fields)
      throws IOException {
    String resolvedIndexName = indexName.orElse("*");
    SchemaTimeRange schemaTimeRange = resolveSchemaTimeRange(startTimeEpochMs, endTimeEpochMs);
    RequestedFields requestedFields = RequestedFields.from(fields.orElse(null));
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        JsonUtil.writeAsString(
            Map.of(
                "indices",
                List.of(resolvedIndexName),
                "fields",
                openSearchSchemaAdapter.buildFieldCaps(
                    resolvedIndexName,
                    schemaTimeRange.startTimeEpochMs(),
                    schemaTimeRange.endTimeEpochMs(),
                    requestedFields))));
  }

  private SchemaTimeRange resolveSchemaTimeRange(
      Optional<Long> startTimeEpochMs, Optional<Long> endTimeEpochMs) {
    Instant now = Instant.now();
    return new SchemaTimeRange(
        startTimeEpochMs.orElseGet(() -> now.minus(1, ChronoUnit.HOURS).toEpochMilli()),
        endTimeEpochMs.orElseGet(now::toEpochMilli));
  }

  private ClusterResponse cluster() {
    return new ClusterResponse(
        compatibilityMetadata.nodeName(),
        compatibilityMetadata.clusterName(),
        compatibilityMetadata.clusterUuid(),
        new ClusterResponse.Version(OPEN_SEARCH_COMPAT_VERSION, OPEN_SEARCH_COMPAT_LUCENE_VERSION),
        "The OpenSearch Project: https://opensearch.org/");
  }

  private NodesInfoResponse nodesInfoBody() {
    var node =
        new NodesInfoResponse.NodeInfo(
            compatibilityMetadata.nodeName(),
            compatibilityMetadata.transportAddress(),
            compatibilityMetadata.host(),
            compatibilityMetadata.ip(),
            OPEN_SEARCH_COMPAT_VERSION,
            List.of("cluster_manager", "data", "ingest"),
            Map.of("cluster_id", compatibilityMetadata.clusterUuid()),
            new NodesInfoResponse.NodeInfo.Http(compatibilityMetadata.httpPublishAddress()));
    return new NodesInfoResponse(
        new NodesInfoResponse.NodeStats(1, 1, 0),
        compatibilityMetadata.clusterName(),
        Map.of(compatibilityMetadata.nodeId(), node));
  }

  private record ClusterResponse(
      @JsonProperty("name") String name,
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("cluster_uuid") String clusterUuid,
      @JsonProperty("version") Version version,
      @JsonProperty("tagline") String tagline) {
    private record Version(
        @JsonProperty("distribution") String distribution,
        @JsonProperty("number") String number,
        @JsonProperty("build_type") String buildType,
        @JsonProperty("build_hash") String buildHash,
        @JsonProperty("build_date") String buildDate,
        @JsonProperty("build_snapshot") boolean buildSnapshot,
        @JsonProperty("lucene_version") String luceneVersion,
        @JsonProperty("minimum_wire_compatibility_version") String minimumWireCompatibilityVersion,
        @JsonProperty("minimum_index_compatibility_version")
            String minimumIndexCompatibilityVersion) {
      private Version(String number, String luceneVersion) {
        this(
            "opensearch",
            number,
            "astra",
            "astra",
            "1970-01-01T00:00:00Z",
            false,
            luceneVersion,
            "7.10.0",
            "7.0.0");
      }
    }
  }

  private record SchemaTimeRange(long startTimeEpochMs, long endTimeEpochMs) {}

  private record NodesInfoResponse(
      @JsonProperty("_nodes") NodeStats nodeStats,
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("nodes") Map<String, NodeInfo> nodes) {
    private record NodeStats(
        @JsonProperty("total") int total,
        @JsonProperty("successful") int successful,
        @JsonProperty("failed") int failed) {}

    private record NodeInfo(
        @JsonProperty("name") String name,
        @JsonProperty("transport_address") String transportAddress,
        @JsonProperty("host") String host,
        @JsonProperty("ip") String ip,
        @JsonProperty("version") String version,
        @JsonProperty("roles") List<String> roles,
        @JsonProperty("attributes") Map<String, String> attributes,
        @JsonProperty("http") Http http) {
      private record Http(@JsonProperty("publish_address") String publishAddress) {}
    }
  }

  private record CompatibilityMetadata(
      String clusterName,
      String clusterUuid,
      String nodeId,
      String ephemeralNodeId,
      String nodeName,
      String host,
      String ip,
      String transportAddress,
      String httpPublishAddress) {
    private static final String DEFAULT_CLUSTER_NAME = "astra";
    private static final String DEFAULT_NODE_ID = "astra-query";
    private static final String DEFAULT_NODE_NAME = "astra-query";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8081;

    private static CompatibilityMetadata fromConfig(AstraConfigs.AstraConfig astraConfig) {
      var configuredClusterName = astraConfig.getClusterConfig().getClusterName();
      var clusterName =
          configuredClusterName.isBlank() ? DEFAULT_CLUSTER_NAME : configuredClusterName;
      var queryServerConfig = astraConfig.getQueryConfig().getServerConfig();
      var hasExplicitServerAddress = !queryServerConfig.getServerAddress().isBlank();
      var hasExplicitServerPort = queryServerConfig.getServerPort() != 0;
      var host = hasExplicitServerAddress ? queryServerConfig.getServerAddress() : DEFAULT_HOST;
      var port = hasExplicitServerPort ? queryServerConfig.getServerPort() : DEFAULT_PORT;
      var nodeId =
          hasExplicitServerAddress || hasExplicitServerPort ? host + ":" + port : DEFAULT_NODE_ID;

      return new CompatibilityMetadata(
          clusterName,
          clusterName + "-compat",
          nodeId,
          nodeId + "-ephemeral",
          DEFAULT_NODE_NAME,
          host,
          host,
          host + ":" + port,
          host + ":" + port);
    }
  }

  private record ClusterStateResponse(
      @JsonProperty("cluster_name") String clusterName,
      @JsonProperty("nodes") Map<String, NodeInfo> nodes) {
    private record NodeInfo(
        @JsonProperty("name") String name,
        @JsonProperty("ephemeral_id") String ephemeralId,
        @JsonProperty("transport_address") String transportAddress,
        @JsonProperty("attributes") Map<String, String> attributes) {}
  }
}
