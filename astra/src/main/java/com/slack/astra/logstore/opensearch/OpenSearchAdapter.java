package com.slack.astra.logstore.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketCollector;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketCollector;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationFactory;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ExtendedStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to allow using OpenSearch aggregations and query parsing from within Astra. This
 * class should ultimately act as an adapter where OpenSearch code is not needed external to this
 * class.
 */
public class OpenSearchAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(OpenSearchAdapter.class);

  private static final IndexSettings indexSettings = AstraIndexSettings.getInstance();
  private static final SimilarityService similarityService = AstraSimilarityService.getInstance();
  private static final ThreadPool FIELD_DATA_THREAD_POOL =
      new ThreadPool(Settings.builder().put("node.name", "astra-opensearch-adapter").build());

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(FIELD_DATA_THREAD_POOL::shutdown));
  }

  private final MapperService mapperService;

  private final Map<String, LuceneFieldDef> chunkSchema;

  public OpenSearchAdapter(Map<String, LuceneFieldDef> chunkSchema) {
    this.mapperService = buildMapperService();
    this.chunkSchema = chunkSchema;
  }

  /** Aggregation execution state for one local Lucene search. */
  public record AggregationExecution(
      List<Aggregator> topLevelAggregators, BucketCollector collector) {
    /** Finalizes collection and builds the aggregation result. */
    public InternalAggregations finish() throws IOException {
      collector.postCollection();
      List<InternalAggregation> internalAggregations = new ArrayList<>(topLevelAggregators.size());
      for (Aggregator aggregator : topLevelAggregators) {
        internalAggregations.add(aggregator.buildTopLevel());
      }
      return InternalAggregations.from(internalAggregations);
    }
  }

  private List<Aggregator> buildAggregatorsFromFactory(
      IndexSearcher indexSearcher,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      Query query) {
    QueryShardContext queryShardContext =
        buildQueryShardContext(AstraBigArrays.getInstance(), indexSearcher, mapperService);

    if (aggregatorFactoriesBuilder != null) {
      try {
        AggregatorFactories aggregatorFactories =
            aggregatorFactoriesBuilder.build(queryShardContext, null);
        SearchContext searchContext =
            new AstraSearchContext(
                AstraBigArrays.getInstance(), queryShardContext, indexSearcher, query);
        return aggregatorFactories.createTopLevelAggregators(searchContext);
      } catch (Exception e) {
        LOG.error(
            "Aggregator parse exception {} for AggregatorFactoriesBuilder {} and Query {}",
            e,
            aggregatorFactoriesBuilder,
            query);
        throw new IllegalArgumentException(e);
      }
    }
    return List.of();
  }

  /**
   * Builds a Lucene query using the provided arguments, and the currently loaded schema. Uses
   * Opensearch QueryBuilder's.
   *
   * @see <a href="https://opensearch.org/docs/latest/query-dsl/full-text/query-string/">Query
   *     parsing OpenSearch docs</a>
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html">Query
   *     parsing ES docs</a>
   */
  public Query buildQuery(IndexSearcher indexSearcher, String dataset, QueryBuilder queryBuilder)
      throws IOException {
    QueryShardContext queryShardContext =
        buildQueryShardContext(AstraBigArrays.getInstance(), indexSearcher, mapperService);

    QueryBuilder scopedQueryBuilder = queryBuilder;
    if (dataset != null && !dataset.isBlank() && !dataset.equals("_all") && !dataset.equals("*")) {
      validateDatasetSelector(dataset);
      BoolQueryBuilder datasetScopedQuery = new BoolQueryBuilder();
      // Astra stores multiple logical indices in the same chunk today. The ingest path derives
      // the logical index name from service_name, so scoped index searches must currently filter
      // on service_name at query time to avoid cross-index leakage within a chunk.
      datasetScopedQuery.filter(
          new TermQueryBuilder(LogMessage.ReservedField.SERVICE_NAME.fieldName, dataset));
      if (queryBuilder != null) {
        datasetScopedQuery.must(queryBuilder);
      }
      scopedQueryBuilder = datasetScopedQuery;
    }

    if (scopedQueryBuilder != null) {
      try {
        return scopedQueryBuilder.rewrite(queryShardContext).toQuery(queryShardContext);
      } catch (Exception e) {
        LOG.error("Query parse exception", e);
        throw new IllegalArgumentException(e);
      }
    }
    // TODO: Should this return null? Raise an error? Just return everything?
    return new BoolQueryBuilder().rewrite(queryShardContext).toQuery(queryShardContext);
  }

  private static void validateDatasetSelector(String dataset) {
    if (dataset.indexOf(',') >= 0) {
      throw new IllegalArgumentException(
          "Multi-index dataset selectors are not supported: " + dataset);
    }
    if (dataset.indexOf('*') >= 0) {
      throw new IllegalArgumentException(
          "Wildcard dataset selectors are not supported: " + dataset);
    }
  }

  /**
   * For each defined field in the chunk schema, this will check if the field is already registered,
   * and if not attempt to register it with the mapper service.
   *
   * @see this.loadSchema()
   */
  public void reloadSchema() {
    // TreeMap here ensures the schema is sorted by natural order - to ensure multifields are
    // registered by their parent first, and then fields added second
    for (Map.Entry<String, LuceneFieldDef> entry : new TreeMap<>(chunkSchema).entrySet()) {
      String fieldMapping = entry.getValue().fieldType.toOpenSearchTypeName();
      try {
        if (fieldMapping != null) {
          tryRegisterField(
              mapperService, entry.getValue().name, b -> b.field("type", fieldMapping));
        }
      } catch (Exception e) {
        LOG.error("Error parsing schema mapping for {}", entry.getValue().toString(), e);
      }
    }
  }

  protected static XContentBuilder mapping(
      CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
    XContentBuilder builder =
        XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
    buildFields.accept(builder);
    return builder.endObject().endObject().endObject();
  }

  protected static XContentBuilder fieldMapping(
      String fieldName, CheckedConsumer<XContentBuilder, IOException> buildField)
      throws IOException {
    return mapping(
        b -> {
          b.startObject(fieldName);
          buildField.accept(b);
          b.endObject();
        });
  }

  protected static XContentBuilder fieldMappingWithFields(
      String parentType,
      String parentName,
      String childName,
      CheckedConsumer<XContentBuilder, IOException> buildField)
      throws IOException {

    return mapping(
        b -> {
          b.startObject(parentName);
          b.field("type", parentType);
          b.startObject("fields");
          b.startObject(childName);
          buildField.accept(b);
          b.endObject();
          b.endObject();
          b.endObject();
        });
  }

  /** Builds the local collector state needed to execute one aggregation for one chunk search. */
  public AggregationExecution createAggregationExecution(
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      IndexSearcher indexSearcher,
      Query query)
      throws IOException {
    List<Aggregator> topLevelAggregators =
        buildAggregatorsFromFactory(indexSearcher, aggregatorFactoriesBuilder, query);
    BucketCollector collector = MultiBucketCollector.wrap(topLevelAggregators);
    collector.preCollection();
    return new AggregationExecution(topLevelAggregators, collector);
  }

  /**
   * Registers the field types that can be aggregated by the different aggregators. Each aggregation
   * builder must be registered with the appropriate fields, or the resulting aggregation will be
   * empty.
   */
  private static ValuesSourceRegistry buildValueSourceRegistry() {
    ValuesSourceRegistry.Builder valuesSourceRegistryBuilder = new ValuesSourceRegistry.Builder();

    AutoDateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    DateHistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    HistogramAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    TermsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MultiTermsAggregationFactory.registerAggregators(valuesSourceRegistryBuilder);
    AvgAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    SumAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MinAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    MaxAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    CardinalityAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ExtendedStatsAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    PercentilesAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);
    ValueCountAggregationBuilder.registerAggregators(valuesSourceRegistryBuilder);

    // Filters are registered in a non-standard way
    valuesSourceRegistryBuilder.registerUsage(FiltersAggregationBuilder.NAME);

    return valuesSourceRegistryBuilder.build();
  }

  /**
   * Builds a MapperService using the minimal amount of settings required for Aggregations. After
   * initializing the mapper service, individual fields will still need to be added using
   * this.registerField()
   */
  private static MapperService buildMapperService() {
    return new MapperService(
        OpenSearchAdapter.indexSettings,
        AstraIndexAnalyzer.getInstance(),
        new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
        OpenSearchAdapter.similarityService,
        AstraMapperRegistry.buildNewInstance(),
        () -> {
          throw new UnsupportedOperationException();
        },
        () -> true,
        null);
  }

  /**
   * Minimal implementation of an OpenSearch QueryShardContext while still allowing an
   * AggregatorFactory to successfully instantiate. See AggregatorFactory.class
   */
  private static QueryShardContext buildQueryShardContext(
      BigArrays bigArrays, IndexSearcher indexSearcher, MapperService mapperService) {
    final ValuesSourceRegistry valuesSourceRegistry = buildValueSourceRegistry();
    return new QueryShardContext(
        0,
        OpenSearchAdapter.indexSettings,
        bigArrays,
        null,
        new IndexFieldDataService(
                OpenSearchAdapter.indexSettings,
                new IndicesFieldDataCache(
                    OpenSearchAdapter.indexSettings.getSettings(),
                    new IndexFieldDataCache.Listener() {},
                    null,
                    FIELD_DATA_THREAD_POOL),
                new NoneCircuitBreakerService(),
                mapperService,
                FIELD_DATA_THREAD_POOL)
            ::getForField,
        mapperService,
        OpenSearchAdapter.similarityService,
        ScriptServiceProvider.getInstance(),
        null,
        null,
        null,
        indexSearcher,
        () -> Instant.now().toEpochMilli(),
        null,
        s -> false,
        () -> true,
        valuesSourceRegistry);
  }

  /**
   * Performs an initial load of the schema into the mapper service. This differs from the
   * reloadSchema in that this first builds an entire mapping and then performs a single merge into
   * the mapper service, instead of individually attempting to merge each field as it is
   * encountered.
   */
  public void loadSchema() {
    ObjectNode rootNode = new ObjectNode(JsonNodeFactory.instance);

    for (Map.Entry<String, LuceneFieldDef> entry : new TreeMap<>(chunkSchema).entrySet()) {
      String fieldName = entry.getValue().name;
      if (mapperService.isMetadataField(fieldName)) {
        LOG.trace("Skipping metadata field '{}'", fieldName);
      } else {
        ObjectNode child = new ObjectNode(JsonNodeFactory.instance);
        child.put("type", entry.getValue().fieldType.toOpenSearchTypeName());
        putAtPath(rootNode, child, entry.getKey());
      }
    }

    try {
      XContentBuilder builder =
          XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
      // Filter out fields that start with a dot (.), e.g. ".ipv4". This is a temporary fix until we
      // sanitize elsewhere in the pipeline. It is currently possible for index nodes to publish a
      // segment/snapshot with this data, which causes cache nodes to fail to load the segment.
      rootNode
          .fields()
          .forEachRemaining(
              (entry) -> {
                // if the root node includes a field that is empty, we need to skip it.
                if (!entry.getKey().equals("")) {
                  buildObject(builder, entry);
                } else {
                  LOG.warn(
                      "Skipping empty field name with value '{}'", entry.getValue().toString());
                }
              });

      builder.endObject().endObject().endObject();

      mapperService.merge(
          MapperService.SINGLE_MAPPING_NAME,
          new CompressedXContent(BytesReference.bytes(builder)),
          MapperService.MergeReason.MAPPING_UPDATE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Given a root object node and a new object node, will insert the new node into the provided
   * dot-separated path. If a deeply nested path is provided, this will initialize the entire tree
   * as needed.
   *
   * @param root ObjectNode root node
   * @param newNode ObjectNode to insert
   * @param path Dot separated path (foo.bar)
   */
  private void putAtPath(ObjectNode root, ObjectNode newNode, String path) {
    String[] fieldParts = path.split("\\.");

    ObjectNode currentRef = root;
    for (int i = 0; i < fieldParts.length; i++) {
      if (!currentRef.has(fieldParts[i])) {
        // it doesn't have the current object
        if (i == fieldParts.length - 1) {
          // last thing, add ours
          currentRef.set(fieldParts[i], newNode);
          return;
        } else {
          // add a parent holder
          ObjectNode fields = new ObjectNode(JsonNodeFactory.instance);
          fields.set("fields", new ObjectNode(JsonNodeFactory.instance));
          currentRef.set(fieldParts[i], fields);
          currentRef = (ObjectNode) currentRef.get(fieldParts[i]).get("fields");
        }

      } else {
        // it has the parent
        if (i == fieldParts.length - 1) {
          currentRef.setAll(newNode);
          return;
        } else {
          // it has the parent, does it have a fields?
          if (!currentRef.get(fieldParts[i]).has("fields")) {
            ((ObjectNode) currentRef.get(fieldParts[i]))
                .set("fields", new ObjectNode(JsonNodeFactory.instance));
          }
          currentRef = (ObjectNode) currentRef.get(fieldParts[i]).get("fields");
        }
      }
    }
  }

  /**
   * Builds the resulting XContent from the provided map entries for use in the mapper service. For
   * entries that do not have explicit field definitions (ie, foo.bar exists as a field while foo
   * does not), will default to a binary type.
   */
  private void buildObject(XContentBuilder builder, Map.Entry<String, JsonNode> entry) {
    try {
      builder.startObject(entry.getKey());

      // does it have a type field set?
      if (entry.getValue().has("type")) {
        builder.field("type", entry.getValue().get("type").asText());
      } else {
        // todo - "default" when no field exists
        builder.field("type", "binary");
      }

      // does it have fields set?
      if (entry.getValue().has("fields")) {
        builder.startObject("fields");
        entry
            .getValue()
            .get("fields")
            .fields()
            .forEachRemaining(
                (fieldEntry) -> {
                  // do this same step all-over
                  buildObject(builder, fieldEntry);
                });
        builder.endObject();
      }

      builder.endObject();
    } catch (Exception e) {
      LOG.error("Error building object", e);
    }
  }

  /**
   * Registers a field type and name to the MapperService for use in aggregations. This informs the
   * aggregators how to access a specific field and what value type it contains. registerField(
   * mapperService, LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, b -> b.field("type",
   * "long"));
   */
  private static boolean tryRegisterField(
      MapperService mapperService,
      String fieldName,
      CheckedConsumer<XContentBuilder, IOException> buildField) {
    MappedFieldType fieldType = mapperService.fieldType(fieldName);
    if (mapperService.isMetadataField(fieldName)) {
      LOG.trace("Skipping metadata field '{}'", fieldName);
      return false;
    } else if (fieldType != null) {
      LOG.trace(
          "Field '{}' already exists as typeName '{}', skipping query mapping update",
          fieldType.name(),
          fieldType.familyTypeName());
      return false;
    } else {
      try {
        XContentBuilder mapping;
        if (fieldName.contains(".")) {
          String[] fieldParts = fieldName.split("\\.");
          String parentFieldName =
              String.join(".", Arrays.copyOfRange(fieldParts, 0, fieldParts.length - 1));
          String localFieldName = fieldParts[fieldParts.length - 1];
          MappedFieldType parent = mapperService.fieldType(parentFieldName);

          if (parent == null) {
            // if we don't have a parent, register this dot separated name as its own thing
            // this will cause future registrations to fail if someone comes along with a parent
            // since you can't merge those items

            // todo - consider making a decision about the parent here
            // todo - if this happens can I declare bankruptcy and start over on the mappings?
            mapping = fieldMapping(fieldName, buildField);
          } else {
            mapping =
                fieldMappingWithFields(
                    parent.typeName(), parentFieldName, localFieldName, buildField);
          }
        } else {
          mapping = fieldMapping(fieldName, buildField);
        }
        mapperService.merge(
            "_doc",
            new CompressedXContent(BytesReference.bytes(mapping)),
            MapperService.MergeReason.MAPPING_UPDATE);
      } catch (Exception e) {
        LOG.warn(
            "Error doing map update which affects aggregation failure for field={} errorMsg={} exception={}",
            fieldName,
            e.getMessage(),
            e.getStackTrace());
      }
      return true;
    }
  }
}
