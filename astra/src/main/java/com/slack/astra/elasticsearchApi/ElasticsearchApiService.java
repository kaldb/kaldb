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
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
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
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.StructuredTaskScope;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.search.aggregations.InternalAggregations;
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
  private final DatasetMetadataStore datasetMetadataStore;

  private final OpenSearchRequest openSearchRequest = new OpenSearchRequest();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public ElasticsearchApiService(
      AstraQueryServiceBase searcher,
      String clusterName,
      String host,
      int port,
      DatasetMetadataStore datasetMetadataStore) {
    this.searcher = searcher;
    this.openSearchSchemaAdapter = new OpenSearchSchemaAdapter(searcher);
    this.compatibilityMetadata = CompatibilityMetadata.from(clusterName, host, port);
    this.datasetMetadataStore = Objects.requireNonNull(datasetMetadataStore);
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
      HitsMetadata hits = getHits(searchResult, searchRequest);
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
    InternalAggregations internalAggregations =
        OpenSearchInternalAggregation.fromByteArray(byteInput.toByteArray());
    if (internalAggregations != null) {
      // OpenSearch renders InternalAggregations as {"aggregations": {...}}, but this response
      // builder wants only the inner aggregations object for the top-level response field.
      JsonNode aggregationsJson =
          OBJECT_MAPPER.readTree(Strings.toString(MediaTypeRegistry.JSON, internalAggregations));
      return aggregationsJson.get("aggregations");
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

  private HitsMetadata getHits(
      AstraSearch.SearchResult searchResult, AstraSearch.SearchRequest searchRequest)
      throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<SearchResponseHit> responseHits = new ArrayList<>(hitsByteList.size());
    List<com.slack.astra.logstore.search.SearchQuery.SortFieldSpec> sortFieldSpecs =
        com.slack.astra.logstore.search.SearchResultUtils.parseSortFieldSpecs(
            searchRequest.getSortJson());
    for (ByteString bytes : hitsByteList) {
      responseHits.add(SearchResponseHit.fromByteString(bytes, sortFieldSpecs));
    }

    return new HitsMetadata.Builder().hitsTotal(hitsTotal(searchResult)).hits(responseHits).build();
  }

  private Map<String, Object> hitsTotal(AstraSearch.SearchResult searchResult) {
    return switch (searchResult.getTotalHitsRelation()) {
      case TOTAL_HITS_EQUAL_TO ->
          ImmutableMap.of("value", searchResult.getTotalHits(), "relation", "eq");
      case TOTAL_HITS_GREATER_THAN_OR_EQUAL_TO ->
          ImmutableMap.of("value", searchResult.getTotalHits(), "relation", "gte");
      case TOTAL_HITS_RELATION_UNSPECIFIED, UNRECOGNIZED -> null;
    };
  }

  /**
   * Mapping API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">API
   *     doc</a>
   */
  @Get
  @Path("/_mapping")
  public HttpResponse mappingAll(
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs)
      throws IOException {
    return mappingResponse(listDatasetNames(), startTimeEpochMs, endTimeEpochMs);
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
      @Param("indexName") String indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs)
      throws IOException {
    return mappingResponse(List.of(indexName), startTimeEpochMs, endTimeEpochMs);
  }

  private HttpResponse mappingResponse(
      List<String> indexNames, Optional<Long> startTimeEpochMs, Optional<Long> endTimeEpochMs)
      throws IOException {
    SchemaTimeRange schemaTimeRange = resolveSchemaTimeRange(startTimeEpochMs, endTimeEpochMs);
    Map<String, Object> responseBody = new TreeMap<>();
    for (String indexName : indexNames) {
      Map<String, Map<String, String>> propertiesMap =
          openSearchSchemaAdapter.buildPropertiesMap(
              indexName, schemaTimeRange.startTimeEpochMs(), schemaTimeRange.endTimeEpochMs());
      responseBody.put(
          indexName, ImmutableMap.of("mappings", ImmutableMap.of("properties", propertiesMap)));
    }

    return HttpResponse.of(
        HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(responseBody));
  }

  /** Returns field aliases for index discovery */
  @Get
  @Path("/_alias")
  public HttpResponse getAliases() throws IOException {
    return aliasResponse(listDatasetNames());
  }

  /** Returns field aliases for index discovery */
  @Get
  @Path("/_alias/:aliasName")
  public HttpResponse getAliasesByName(@Param("aliasName") String aliasName) throws IOException {
    return aliasResponse(List.of());
  }

  /** Returns field aliases for index discovery */
  @Get
  @Path("/:indexName/_alias")
  public HttpResponse getIndexAliases(@Param("indexName") String indexName) throws IOException {
    return aliasResponse(List.of(indexName));
  }

  private HttpResponse aliasResponse(List<String> indexNames) throws IOException {
    Map<String, Object> responseBody = new TreeMap<>();
    for (String indexName : indexNames) {
      responseBody.put(indexName, Map.of("aliases", Map.of()));
    }
    return HttpResponse.of(
        HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(responseBody));
  }

  /** Returns concrete index names for Dashboards data-view resolution */
  @Get
  @Path("/_resolve/index/:indexName")
  public HttpResponse resolveIndex(@Param("indexName") String indexName) throws IOException {
    List<ResolvedIndex> indices = resolveIndices(indexName);
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        JsonUtil.writeAsString(new ResolveIndexResponse(indices, List.of(), List.of())));
  }

  private List<ResolvedIndex> resolveIndices(String indexPattern) {
    if (indexPattern.isBlank()) {
      return List.of();
    }

    return listDatasetNames().stream()
        .filter(indexPattern::equals)
        .findFirst()
        .map(name -> List.of(new ResolvedIndex(name, List.of("open"))))
        .orElse(List.of());
  }

  private List<String> listDatasetNames() {
    return Optional.ofNullable(datasetMetadataStore.listSync()).orElse(List.of()).stream()
        .map(DatasetMetadata::getName)
        .sorted()
        .toList();
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
    return fieldCapabilitiesResponse(
        indexName.orElse("*"), startTimeEpochMs, endTimeEpochMs, fields);
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
    return fieldCapabilitiesResponse(
        indexName.orElse("*"), startTimeEpochMs, endTimeEpochMs, fields);
  }

  /** Returns field capabilities for data views */
  @Get
  @Blocking
  @Path("/:indexName/_field_caps")
  public HttpResponse fieldCapabilitiesGet(
      @Param("indexName") String indexName,
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
      @Param("indexName") String indexName,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("fields") Optional<String> fields)
      throws IOException {
    return fieldCapabilitiesResponse(indexName, startTimeEpochMs, endTimeEpochMs, fields);
  }

  private HttpResponse fieldCapabilitiesResponse(
      String indexName,
      Optional<Long> startTimeEpochMs,
      Optional<Long> endTimeEpochMs,
      Optional<String> fields)
      throws IOException {
    SchemaTimeRange schemaTimeRange = resolveSchemaTimeRange(startTimeEpochMs, endTimeEpochMs);
    RequestedFields requestedFields = RequestedFields.from(fields.orElse(null));
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON_UTF_8,
        JsonUtil.writeAsString(
            Map.of(
                "indices",
                List.of(indexName),
                "fields",
                openSearchSchemaAdapter.buildFieldCaps(
                    indexName,
                    schemaTimeRange.startTimeEpochMs(),
                    schemaTimeRange.endTimeEpochMs(),
                    requestedFields))));
  }

  // Schema discovery defaults to the last hour when callers do not provide an explicit range.
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

  private record ResolveIndexResponse(
      @JsonProperty("indices") List<ResolvedIndex> indices,
      @JsonProperty("aliases") List<Object> aliases,
      @JsonProperty("data_streams") List<Object> dataStreams) {}

  private record ResolvedIndex(
      @JsonProperty("name") String name, @JsonProperty("attributes") List<String> attributes) {}

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

  /**
   * Synthetic OpenSearch cluster and node identity returned by Astra's compatibility endpoints.
   *
   * <p>These values are not Astra-native topology metadata. They exist so responses from {@code /},
   * {@code /_nodes}, and {@code /_cluster/state/nodes} have the OpenSearch-shaped fields that
   * OpenSearch Dashboards expects during startup health and version checks.
   */
  private record CompatibilityMetadata(
      // Returned as `cluster_name` across compatibility responses.
      String clusterName,
      // Returned as `cluster_uuid` and mirrored into node attributes as `cluster_id`.
      String clusterUuid,
      // Used as the key in `nodes` maps and to back `/_nodes/:nodeId` style lookups.
      String nodeId,
      // Returned as `ephemeral_id` in cluster-state payloads because OpenSearch node metadata
      // includes it, even though this shim does not model real process restarts.
      String ephemeralNodeId,
      // Returned as the node `name` in cluster and node metadata responses.
      String nodeName,
      // Returned as the node `host` in `/_nodes` responses.
      String host,
      // Returned as the node `ip` in `/_nodes`; today we mirror `host` because this shim has no
      // separate published IP address.
      String ip,
      // Returned as `transport_address` in node discovery responses.
      String transportAddress,
      // Returned as `http.publish_address` in `/_nodes`, which Dashboards reads during version
      // checks when building human-readable node descriptions.
      String httpPublishAddress) {
    private static final String DEFAULT_CLUSTER_NAME = "astra";
    private static final String DEFAULT_NODE_ID = "astra-query";
    private static final String DEFAULT_NODE_NAME = "astra-query";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8081;

    private static CompatibilityMetadata from(String clusterName, String host, int port) {
      var resolvedClusterName =
          clusterName == null || clusterName.isBlank() ? DEFAULT_CLUSTER_NAME : clusterName;
      var hasExplicitServerAddress = host != null && !host.isBlank();
      var hasExplicitServerPort = port != 0;
      var resolvedHost = hasExplicitServerAddress ? host : DEFAULT_HOST;
      var resolvedPort = hasExplicitServerPort ? port : DEFAULT_PORT;
      var nodeId =
          hasExplicitServerAddress || hasExplicitServerPort
              ? resolvedHost + ":" + resolvedPort
              : DEFAULT_NODE_ID;

      return new CompatibilityMetadata(
          resolvedClusterName,
          resolvedClusterName + "-compat",
          nodeId,
          nodeId + "-ephemeral",
          DEFAULT_NODE_NAME,
          resolvedHost,
          resolvedHost,
          resolvedHost + ":" + resolvedPort,
          resolvedHost + ":" + resolvedPort);
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
