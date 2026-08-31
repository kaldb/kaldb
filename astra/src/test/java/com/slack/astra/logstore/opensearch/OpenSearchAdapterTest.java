package com.slack.astra.logstore.opensearch;

import static com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension.search;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.google.common.collect.ImmutableMap;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.search.SearchResult;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.testlib.SpanUtil;
import com.slack.astra.testlib.TemporaryLogStoreAndSearcherExtension;
import com.slack.astra.util.QueryBuilderUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;

public class OpenSearchAdapterTest {
  private static Tracing tracing;

  @Test
  public void shouldKeepIndexIdentityOutOfSharedSettings() {
    assertThat(
            AstraIndexSettings.getSharedServiceSettings()
                .hasValue(IndexMetadata.SETTING_INDEX_UUID))
        .isFalse();
  }

  @Test
  public void shouldCreateDistinctIndexIdentities() {
    IndexSettings firstAdapterSettings = AstraIndexSettings.create();
    IndexSettings secondAdapterSettings = AstraIndexSettings.create();
    assertThat(firstAdapterSettings.getIndex().getUUID())
        .isNotEqualTo(secondAdapterSettings.getIndex().getUUID());
  }

  private record TestIndex(Directory directory, DirectoryReader reader, IndexSearcher searcher)
      implements AutoCloseable {
    @Override
    public void close() throws IOException {
      reader.close();
      directory.close();
    }
  }

  private static TestIndex buildTwoSegmentKeywordIndex(String shardName) throws IOException {
    Directory directory = new ByteBuffersDirectory();
    try (IndexWriter writer =
        new IndexWriter(
            directory,
            new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE))) {
      addKeywordDocument(writer, "api");
      writer.commit();
      addKeywordDocument(writer, "worker");
      writer.commit();
    }

    DirectoryReader reader =
        OpenSearchDirectoryReader.wrap(
            DirectoryReader.open(directory),
            new ShardId(shardName, UUID.randomUUID().toString(), 0));
    return new TestIndex(directory, reader, new IndexSearcher(reader));
  }

  private static void addKeywordDocument(IndexWriter writer, String value) throws IOException {
    Document document = new Document();
    document.add(
        new StringField(LogMessage.ReservedField.SERVICE_NAME.fieldName, value, Field.Store.NO));
    document.add(
        new SortedDocValuesField(
            LogMessage.ReservedField.SERVICE_NAME.fieldName, new BytesRef(value)));
    writer.addDocument(document);
  }

  private static void runTermsAggregation(OpenSearchAdapter adapter, IndexSearcher searcher)
      throws IOException {
    AggregatorFactories.Builder factories = new AggregatorFactories.Builder();
    factories.addAggregator(
        new TermsAggregationBuilder("services")
            .field(LogMessage.ReservedField.SERVICE_NAME.fieldName));
    Query query = new MatchAllDocsQuery();
    OpenSearchAdapter.AggregationExecution execution =
        adapter.createAggregationExecution(factories, searcher, query);
    searcher.search(query, execution.collector());
    Object aggregation = execution.finish().get("services");
    assertThat(aggregation).isNotNull();
  }

  @BeforeAll
  public static void beforeAll() {
    tracing = Tracing.newBuilder().build();
  }

  @AfterAll
  public static void afterAll() {
    if (tracing != null) {
      tracing.close();
      tracing = null;
    }
  }

  @RegisterExtension
  public TemporaryLogStoreAndSearcherExtension logStoreAndSearcherRule =
      new TemporaryLogStoreAndSearcherExtension(false);

  private final OpenSearchAdapter openSearchAdapter;

  public OpenSearchAdapterTest() throws IOException {
    ImmutableMap.Builder<String, LuceneFieldDef> fieldDefBuilder = ImmutableMap.builder();
    fieldDefBuilder.put(
        LogMessage.SystemField.ID.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.ID.fieldName, FieldType.ID.name, false, true, true));
    fieldDefBuilder.put(
        LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
        new LuceneFieldDef(
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
            FieldType.LONG.name,
            false,
            true,
            true));
    fieldDefBuilder.put(
        LogMessage.ReservedField.SERVICE_NAME.fieldName,
        new LuceneFieldDef(
            LogMessage.ReservedField.SERVICE_NAME.fieldName,
            FieldType.KEYWORD.name,
            false,
            true,
            true));
    openSearchAdapter = new OpenSearchAdapter(fieldDefBuilder.build());
    // We need to reload the schema so that query optimizations take into account the schema
    openSearchAdapter.reloadSchema();
  }

  @Test
  public void testAggregationExecutionBuildsAggregationResult() throws IOException {
    AvgAggregationBuilder avgAggregationBuilder = new AvgAggregationBuilder("foo");
    avgAggregationBuilder.field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    avgAggregationBuilder.missing("2");

    AvgAggregationBuilder avgAggregationBuilder2 = new AvgAggregationBuilder("bar");
    avgAggregationBuilder2.field(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
    avgAggregationBuilder2.missing("2");

    AggregatorFactories.Builder aggregatorFactoriesBuilder = new AggregatorFactories.Builder();
    aggregatorFactoriesBuilder.addAggregator(avgAggregationBuilder);
    aggregatorFactoriesBuilder.addAggregator(avgAggregationBuilder2);
    OpenSearchAdapter.AggregationExecution aggregationExecution =
        openSearchAdapter.createAggregationExecution(
            aggregatorFactoriesBuilder,
            logStoreAndSearcherRule
                .logStore
                .getAstraSearcherManager()
                .getLuceneSearcherManager()
                .acquire(),
            null);
    InternalAggregations reduced = aggregationExecution.finish();
    InternalAvg reducedAvg = (InternalAvg) reduced.get("foo");
    InternalAvg reducedAvg2 = (InternalAvg) reduced.get("bar");

    assertThat(reducedAvg.getName()).isEqualTo("foo");
    assertThat(reducedAvg.getType()).isEqualTo("avg");
    assertThat(reducedAvg.getValue()).isEqualTo(Double.valueOf("NaN"));
    assertThat(reducedAvg2.getName()).isEqualTo("bar");
    assertThat(reducedAvg2.getType()).isEqualTo("avg");
    assertThat(reducedAvg2.getValue()).isEqualTo(Double.valueOf("NaN"));

    // todo - we don't have access to the package local methods for extra asserts - use reflection?
  }

  @Test
  public void shouldReuseFieldDataAndCleanOnlyTheClosedAdapter() throws Exception {
    OpenSearchAdapter.cleanFieldDataCache();
    long initialEntryCount = OpenSearchAdapter.fieldDataCacheEntryCount();
    Map<String, LuceneFieldDef> keywordSchema =
        Map.of(
            LogMessage.ReservedField.SERVICE_NAME.fieldName,
            new LuceneFieldDef(
                LogMessage.ReservedField.SERVICE_NAME.fieldName,
                FieldType.KEYWORD.name,
                false,
                true,
                true));

    try (OpenSearchAdapter adapterA = new OpenSearchAdapter(keywordSchema);
        OpenSearchAdapter adapterB = new OpenSearchAdapter(keywordSchema);
        TestIndex indexA = buildTwoSegmentKeywordIndex("cache-test-a");
        TestIndex indexB = buildTwoSegmentKeywordIndex("cache-test-b")) {
      assertThat(indexA.reader().leaves()).hasSize(2);
      assertThat(indexB.reader().leaves()).hasSize(2);
      adapterA.reloadSchema();
      adapterB.reloadSchema();

      runTermsAggregation(adapterA, indexA.searcher());
      assertThat(OpenSearchAdapter.fieldDataCacheEntryCount()).isEqualTo(initialEntryCount + 1);

      runTermsAggregation(adapterA, indexA.searcher());
      assertThat(OpenSearchAdapter.fieldDataCacheEntryCount()).isEqualTo(initialEntryCount + 1);

      runTermsAggregation(adapterB, indexB.searcher());
      assertThat(OpenSearchAdapter.fieldDataCacheEntryCount()).isEqualTo(initialEntryCount + 2);

      adapterA.close();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                OpenSearchAdapter.cleanFieldDataCache();
                assertThat(OpenSearchAdapter.fieldDataCacheEntryCount())
                    .isEqualTo(initialEntryCount + 1);
              });

      runTermsAggregation(adapterB, indexB.searcher());
      assertThat(OpenSearchAdapter.fieldDataCacheEntryCount()).isEqualTo(initialEntryCount + 1);
    }

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> {
              OpenSearchAdapter.cleanFieldDataCache();
              assertThat(OpenSearchAdapter.fieldDataCacheEntryCount()).isEqualTo(initialEntryCount);
            });
  }

  @Test
  public void shouldProduceQueryFromQueryBuilder() throws Exception {
    BoolQueryBuilder boolQueryBuilder =
        new BoolQueryBuilder().filter(new RangeQueryBuilder("_timesinceepoch").gte(1).lte(100));
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();

    Query rangeQuery = openSearchAdapter.buildQuery(indexSearcher, "_all", boolQueryBuilder);
    assertThat(rangeQuery).isNotNull();
    assertThat(rangeQuery.toString()).contains("_timesinceepoch:[1 TO 100]");
  }

  @Test
  public void shouldScopeQueriesByDatasetUsingServiceName() throws Exception {
    BoolQueryBuilder boolQueryBuilder =
        new BoolQueryBuilder().filter(new RangeQueryBuilder("_timesinceepoch").gte(1).lte(100));
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();

    Query scopedQuery = openSearchAdapter.buildQuery(indexSearcher, "test", boolQueryBuilder);

    assertThat(scopedQuery.toString()).contains("service_name:test");
    assertThat(scopedQuery.toString()).contains("_timesinceepoch:[1 TO 100]");
  }

  @Test
  public void shouldNotScopeQueriesForWildcardDatasets() throws Exception {
    BoolQueryBuilder boolQueryBuilder =
        new BoolQueryBuilder().filter(new RangeQueryBuilder("_timesinceepoch").gte(1).lte(100));
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();

    Query allDatasetQuery = openSearchAdapter.buildQuery(indexSearcher, "_all", boolQueryBuilder);
    Query wildcardDatasetQuery = openSearchAdapter.buildQuery(indexSearcher, "*", boolQueryBuilder);

    assertThat(allDatasetQuery.toString()).contains("_timesinceepoch:[1 TO 100]");
    assertThat(wildcardDatasetQuery.toString()).contains("_timesinceepoch:[1 TO 100]");
    assertThat(wildcardDatasetQuery.toString()).doesNotContain("service_name:");
  }

  @Test
  public void shouldRejectCommaSeparatedDatasetSelectors() throws Exception {
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();

    assertThatThrownBy(
            () ->
                openSearchAdapter.buildQuery(indexSearcher, "foo,bar", new MatchAllQueryBuilder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Multi-index dataset selectors are not supported");
  }

  @Test
  public void shouldRejectWildcardDatasetSelectors() throws Exception {
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();

    assertThatThrownBy(
            () -> openSearchAdapter.buildQuery(indexSearcher, "foo*", new MatchAllQueryBuilder()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Wildcard dataset selectors are not supported");
  }

  @Test
  public void shouldFilterSearchResultsByDataset() throws Exception {
    long baseMicros = Instant.now().toEpochMilli() * 1000;
    logStoreAndSearcherRule.logStore.addMessage(
        SpanUtil.makeSpan("trace-1", "id-1", "", baseMicros, 100, "foo-span", "foo", "INFO"));
    logStoreAndSearcherRule.logStore.addMessage(
        SpanUtil.makeSpan("trace-2", "id-2", "", baseMicros + 1, 100, "bar-span", "bar", "INFO"));
    logStoreAndSearcherRule.logStore.commit();
    logStoreAndSearcherRule.logStore.refresh();

    SearchResult<LogMessage> searchResult =
        search(logStoreAndSearcherRule.logSearcher, "foo", 10, null, null, null);

    assertThat(searchResult.hits).hasSize(1);
    assertThat(searchResult.hits.get(0).message().getIndex()).isEqualTo("foo");
    assertThat(searchResult.hits.get(0).message().getSource())
        .containsEntry(LogMessage.ReservedField.SERVICE_NAME.fieldName, "foo");
  }

  @Test
  public void shouldParseIdFieldSearch() throws Exception {
    String idField = "_id";
    String idValue = "1";
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();
    Query idQuery =
        openSearchAdapter.buildQuery(
            indexSearcher,
            "_all",
            new QueryStringQueryBuilder(String.format("%s:%s", idField, idValue)));
    BytesRef queryStrBytes = new BytesRef(Uid.encodeId("1").bytes);
    // idQuery.toString="#_id:([fe 1f])"
    // queryStrBytes.toString="[fe 1f]"
    assertThat(idQuery.toString()).contains(queryStrBytes.toString());
  }

  @Test
  public void shouldExcludeDateFilterWhenNullTimestamps() throws Exception {
    IndexSearcher indexSearcher =
        logStoreAndSearcherRule
            .logStore
            .getAstraSearcherManager()
            .getLuceneSearcherManager()
            .acquire();
    Query nullBothTimestamps =
        openSearchAdapter.buildQuery(
            indexSearcher, "_all", QueryBuilderUtil.generateQueryBuilder("", null, null));
    nullBothTimestamps = indexSearcher.rewrite(nullBothTimestamps);
    // null for both timestamps with no query string should be optimized into a matchall
    assertThat(nullBothTimestamps).isInstanceOf(MatchAllDocsQuery.class);

    Query nullStartTimestamp =
        openSearchAdapter.buildQuery(
            indexSearcher, "_all", QueryBuilderUtil.generateQueryBuilder("_id:a", null, 100L));
    nullStartTimestamp = indexSearcher.rewrite(nullStartTimestamp);
    assertThat(nullStartTimestamp).isInstanceOf(BooleanQuery.class);

    Optional<IndexSortSortedNumericDocValuesRangeQuery> filterNullStartQuery =
        ((BooleanQuery) nullStartTimestamp)
            .clauses().stream()
                .filter(
                    booleanClause ->
                        booleanClause.query() instanceof IndexSortSortedNumericDocValuesRangeQuery)
                .map(
                    booleanClause ->
                        (IndexSortSortedNumericDocValuesRangeQuery) booleanClause.query())
                .findFirst();
    assertThat(filterNullStartQuery).isPresent();
    // a null start and provided end should result in an optimized range query of min long to the
    // end value
    assertThat(filterNullStartQuery.get().toString()).contains(String.valueOf(Long.MIN_VALUE));
    assertThat(filterNullStartQuery.get().toString()).contains(String.valueOf(100L));

    Query nullEndTimestamp =
        openSearchAdapter.buildQuery(
            indexSearcher, "_all", QueryBuilderUtil.generateQueryBuilder("_id:a", 100L, null));
    nullEndTimestamp = indexSearcher.rewrite(nullEndTimestamp);
    Optional<IndexSortSortedNumericDocValuesRangeQuery> filterNullEndQuery =
        ((BooleanQuery) nullEndTimestamp)
            .clauses().stream()
                .filter(
                    booleanClause ->
                        booleanClause.query() instanceof IndexSortSortedNumericDocValuesRangeQuery)
                .map(
                    booleanClause ->
                        (IndexSortSortedNumericDocValuesRangeQuery) booleanClause.query())
                .findFirst();
    assertThat(filterNullEndQuery).isPresent();
    // a null end and provided start should result in an optimized range query of start value to max
    // long
    assertThat(filterNullEndQuery.get().toString()).contains(String.valueOf(100L));
    assertThat(filterNullEndQuery.get().toString()).contains(String.valueOf(Long.MAX_VALUE));
  }

  @Test
  public void shouldHandleFieldsStartingWithDot() throws IOException {
    // Define a schema with a field name starting with a dot
    ImmutableMap<String, LuceneFieldDef> chunkSchema =
        ImmutableMap.of(".ipv4", new LuceneFieldDef(".ipv4", FieldType.IP.name, false, true, true));

    // Create a new OpenSearchAdapter instance with this schema
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema);

    // Run loadSchema and assert it passes (does not throw an exception)
    try {
      adapter.loadSchema(); // This should pass
    } catch (RuntimeException e) {
      // The test fails if the exception message contains "name cannot be empty string"
      assertThat(e.getMessage()).doesNotContain("name cannot be empty string");
      org.junit.jupiter.api.Assertions.fail("Unexpected exception thrown: " + e.getMessage());
    }
  }

  @Test
  public void shouldHandleFieldsWithDotInMiddle() throws IOException {
    // Define a schema with a field name starting with a dot
    ImmutableMap<String, LuceneFieldDef> chunkSchema =
        ImmutableMap.of(
            "something.ipv4",
            new LuceneFieldDef("something.ipv4", FieldType.IP.name, false, true, true));

    // Create a new OpenSearchAdapter instance with this schema
    OpenSearchAdapter adapter = new OpenSearchAdapter(chunkSchema);

    // Run loadSchema and assert it passes (does not throw an exception)
    try {
      adapter.loadSchema(); // This should pass
    } catch (RuntimeException e) {
      // The test fails if there is any exception
      org.junit.jupiter.api.Assertions.fail("Unexpected exception thrown: " + e.getMessage());
    }
  }
}
