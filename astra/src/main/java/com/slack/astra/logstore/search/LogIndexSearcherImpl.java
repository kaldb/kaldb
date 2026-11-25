package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogMessage.SystemField;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.logstore.opensearch.OpenSearchAdapter;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.metadata.schema.LuceneFieldDef;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.search.aggregations.InternalAggregations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A wrapper around lucene that helps us search a single index containing logs.
 * TODO: Add template type to this class definition.
 */
public class LogIndexSearcherImpl implements LogIndexSearcher<LogMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(LogIndexSearcherImpl.class);

  private final SearcherManager searcherManager;

  private final ConcurrentHashMap<String, LuceneFieldDef> chunkSchema;
  private final OpenSearchAdapter openSearchAdapter;

  private final ReferenceManager.RefreshListener refreshListener;

  private final AstraSearcherManager astraSearcherManager;

  public LogIndexSearcherImpl(
      AstraSearcherManager astraSearcherManager,
      ConcurrentHashMap<String, LuceneFieldDef> chunkSchema) {
    this.chunkSchema = chunkSchema;
    this.openSearchAdapter = new OpenSearchAdapter(chunkSchema);
    this.refreshListener =
        new ReferenceManager.RefreshListener() {
          @Override
          public void beforeRefresh() {
            // no-op
          }

          @Override
          public void afterRefresh(boolean didRefresh) {
            openSearchAdapter.reloadSchema();
          }
        };
    this.astraSearcherManager = astraSearcherManager;
    this.searcherManager = astraSearcherManager.getLuceneSearcherManager();
    this.searcherManager.addListener(refreshListener);
    // initialize the adapter with whatever the default schema is

    try {
      openSearchAdapter.loadSchema();
    } catch (Exception e) {
      LOG.error("Failed to load schema due to error:", e);
      this.close();

      throw new RuntimeException(e);
    }
  }

  @Override
  public SearchResult<LogMessage> search(SearchQuery searchQuery) {
    int howMany = searchQuery.leafHowMany();

    ensureNonEmptyString(searchQuery.dataset, "dataset should be a non-empty string");
    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    ensureTrue(
        howMany > 0 || searchQuery.aggregatorFactoriesBuilder != null,
        "Hits or aggregation should be requested.");

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("LogIndexSearcherImpl.search");
    span.tag("dataset", searchQuery.dataset);
    span.tag("howMany", String.valueOf(howMany));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    SearchQuery.HitSortPlan hitSortPlan = searchQuery.hitSortPlan;
    try {
      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();

      try {
        List<SearchResultHit<LogMessage>> results;
        InternalAggregations internalAggregations = null;
        Query query =
            openSearchAdapter.buildQuery(searcher, searchQuery.dataset, searchQuery.queryBuilder);
        OpenSearchAdapter.AggregationExecution aggregationExecution =
            searchQuery.aggregatorFactoriesBuilder == null
                ? null
                : openSearchAdapter.createAggregationExecution(
                    searchQuery.aggregatorFactoriesBuilder, searcher, query);
        TopFieldCollector topFieldCollector =
            howMany > 0
                ? buildTopFieldCollector(
                    howMany,
                    aggregationExecution != null ? Integer.MAX_VALUE : howMany,
                    hitSortPlan)
                : null;

        Collector collector =
            topFieldCollector != null && aggregationExecution != null
                ? MultiCollector.wrap(topFieldCollector, aggregationExecution.collector())
                : topFieldCollector != null ? topFieldCollector : aggregationExecution.collector();
        searcher.search(query, collector);

        if (topFieldCollector != null) {
          TopFieldDocs topDocs = topFieldCollector.topDocs();
          ScoreDoc[] hits = topDocs.scoreDocs;
          results = new ArrayList<>(hits.length);
          for (ScoreDoc hit : hits) {
            FieldDoc fieldDoc = (FieldDoc) hit;
            LogWireMessage wireMessage = buildLogWireMessage(searcher, fieldDoc);
            results.add(
                new SearchResultHit<>(
                    buildLogMessage(wireMessage, searchQuery.sourceFieldFilter),
                    sortValues(fieldDoc, hitSortPlan)));
          }
        } else {
          results = Collections.emptyList();
        }
        if (aggregationExecution != null) {
          internalAggregations = aggregationExecution.finish();
        }

        elapsedTime.stop();
        return new SearchResult<>(
            results, elapsedTime.elapsed(TimeUnit.MICROSECONDS), 0, 0, 1, 1, internalAggregations);
      } finally {
        searcherManager.release(searcher);
      }
    } catch (IOException e) {
      span.error(e);
      throw new IllegalArgumentException("Failed to acquire an index searcher.", e);
    } finally {
      span.finish();
    }
  }

  private List<HitSortValue> sortValues(FieldDoc hit, SearchQuery.HitSortPlan hitSortPlan) {
    List<SearchQuery.SortFieldSpec> sortFieldSpecs = hitSortPlan.effectiveSortFields();
    List<HitSortValue> values = new ArrayList<>(sortFieldSpecs.size());
    for (int i = 0; i < sortFieldSpecs.size(); i++) {
      SearchQuery.SortFieldSpec sortFieldSpec = sortFieldSpecs.get(i);
      values.add(sortValue(hit.fields[i], sortFieldSpec));
    }
    return values;
  }

  private HitSortValue sortValue(Object value, SearchQuery.SortFieldSpec sortFieldSpec) {
    if (value instanceof BytesRef bytesRef) {
      LuceneFieldDef fieldDef = chunkSchema.get(sortFieldSpec.field());
      ByteString bytes = ByteString.copyFrom(bytesRef.bytes, bytesRef.offset, bytesRef.length);
      if (SystemField.ID.fieldName.equals(sortFieldSpec.field())) {
        return HitSortValue.bytes(bytes);
      }
      if (fieldDef != null && fieldDef.fieldType == FieldType.IP) {
        return HitSortValue.ipAddress(bytes);
      }
      return HitSortValue.stringValue(bytesRef.utf8ToString());
    }
    return HitSortValue.of(value);
  }

  private LogWireMessage buildLogWireMessage(IndexSearcher searcher, ScoreDoc hit) {
    String s = "";
    try {
      s = searcher.doc(hit.doc).get(SystemField.SOURCE.fieldName);
      return JsonUtil.read(s, LogWireMessage.class);
    } catch (Exception e) {
      throw new IllegalStateException("Error fetching and parsing a result from index: " + s, e);
    }
  }

  private LogMessage buildLogMessage(
      LogWireMessage wireMessage, SourceFieldFilter sourceFieldFilter) {
    Map<String, Object> source = wireMessage.getSource();

    SourceFieldFilter.FilterType filterType =
        sourceFieldFilter == null ? null : sourceFieldFilter.getFilterType();
    if (filterType == SourceFieldFilter.FilterType.INCLUDE
        || filterType == SourceFieldFilter.FilterType.EXCLUDE) {
      boolean keepMatchingFields = filterType == SourceFieldFilter.FilterType.INCLUDE;
      source =
          source.entrySet().stream()
              .filter(
                  entry -> sourceFieldFilter.appliesToField(entry.getKey()) == keepMatchingFields)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    return new LogMessage(
        wireMessage.getIndex(),
        wireMessage.getType(),
        wireMessage.getId(),
        wireMessage.getTimestamp(),
        source);
  }

  /**
   * Builds a top field collector for the requested amount of results, with the option to set the
   * totalHitsThreshold. If the totalHitsThreshold is set to Integer.MAX_VALUE it will force a
   * ScoreMode.COMPLETE, iterating over all documents at the expense of a longer query time. This
   * value can be set to equal howMany to allow early exiting (ScoreMode.TOP_SCORES), but should
   * only be done when all collectors are tolerant of an early exit.
   */
  private TopFieldCollector buildTopFieldCollector(
      int howMany, int totalHitsThreshold, SearchQuery.HitSortPlan hitSortPlan) {
    return TopFieldCollector.create(
        new Sort(buildSortFields(hitSortPlan)), howMany, null, totalHitsThreshold);
  }

  private SortField[] buildSortFields(SearchQuery.HitSortPlan hitSortPlan) {
    return hitSortPlan.effectiveSortFields().stream()
        .map(this::buildSortField)
        .toArray(SortField[]::new);
  }

  private SortField buildSortField(SearchQuery.SortFieldSpec sortFieldSpec) {
    String fieldName = sortFieldSpec.field();
    if (SystemField.TIME_SINCE_EPOCH.fieldName.equals(fieldName)) {
      return setMissingSortValue(new SortField(fieldName, Type.LONG, sortFieldSpec.descending()));
    }

    LuceneFieldDef fieldDef = chunkSchema.get(fieldName);
    if (fieldDef == null) {
      return setMissingSortValue(new SortField(fieldName, Type.STRING, sortFieldSpec.descending()));
    }
    ensureTrue(fieldDef.storeDocValue, "Sort field must have doc values: " + fieldName);
    return setMissingSortValue(
        new SortField(
            fieldName, toLuceneSortFieldType(fieldDef.fieldType), sortFieldSpec.descending()));
  }

  private SortField setMissingSortValue(SortField sortField) {
    boolean useMinimumValue = sortField.getReverse();
    switch (sortField.getType()) {
      case STRING ->
          sortField.setMissingValue(
              useMinimumValue ? SortField.STRING_FIRST : SortField.STRING_LAST);
      case INT ->
          sortField.setMissingValue(useMinimumValue ? Integer.MIN_VALUE : Integer.MAX_VALUE);
      case LONG -> sortField.setMissingValue(useMinimumValue ? Long.MIN_VALUE : Long.MAX_VALUE);
      case FLOAT ->
          sortField.setMissingValue(
              useMinimumValue ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
      case DOUBLE ->
          sortField.setMissingValue(
              useMinimumValue ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
      default -> {
        // No missing-value policy is available for this Lucene sort type.
      }
    }
    return sortField;
  }

  private Type toLuceneSortFieldType(FieldType fieldType) {
    return switch (fieldType) {
      case DATE, LONG, SCALED_LONG -> Type.LONG;
      case BOOLEAN, INTEGER, SHORT, BYTE -> Type.INT;
      case FLOAT -> Type.FLOAT;
      case DOUBLE -> Type.DOUBLE;
      case KEYWORD, STRING, ID, IP -> Type.STRING;
      default -> throw new IllegalArgumentException("Unsupported sort field type: " + fieldType);
    };
  }

  @Override
  public void close() {
    try {
      searcherManager.removeListener(refreshListener);
      astraSearcherManager.close();
    } catch (IOException e) {
      LOG.error("Encountered error closing searcher manager", e);
    }
  }
}
