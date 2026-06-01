package com.slack.astra.logstore.search;

import static com.slack.astra.util.ArgValidationUtils.ensureNonEmptyString;
import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.base.Stopwatch;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
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
  public SearchResult<LogMessage> search(
      String dataset,
      int howMany,
      List<SearchQuery.SortFieldSpec> sortFieldSpecs,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      SearchQuery.TotalHitsPolicy totalHitsPolicy) {

    ensureNonEmptyString(dataset, "dataset should be a non-empty string");
    ensureTrue(howMany >= 0, "hits requested should not be negative.");
    Objects.requireNonNull(totalHitsPolicy, "totalHitsPolicy should not be null.");

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("LogIndexSearcherImpl.search");
    span.tag("dataset", dataset);
    span.tag("howMany", String.valueOf(howMany));

    Stopwatch elapsedTime = Stopwatch.createStarted();
    try {
      // Acquire an index searcher from searcher manager.
      // This is a useful optimization for indexes that are static.
      IndexSearcher searcher = searcherManager.acquire();

      try {
        List<LogMessage> results;
        InternalAggregations internalAggregations = null;
        Query query = openSearchAdapter.buildQuery(searcher, dataset, queryBuilder);
        OpenSearchAdapter.AggregationExecution aggregationExecution =
            aggregatorFactoriesBuilder == null
                ? null
                : openSearchAdapter.createAggregationExecution(
                    aggregatorFactoriesBuilder, searcher, query);
        TopFieldCollector topFieldCollector =
            howMany > 0
                ? buildTopFieldCollector(
                    howMany,
                    totalHitsThreshold(aggregationExecution, totalHitsPolicy),
                    sortFieldSpecs)
                : null;
        TotalHitCountCollector totalHitCountCollector =
            topFieldCollector == null && aggregationExecution != null && totalHitsPolicy.enabled()
                ? new TotalHitCountCollector()
                : null;

        if (topFieldCollector == null && aggregationExecution == null) {
          TotalHitsData totalHitsData =
              totalHitsPolicy.enabled()
                  ? thresholdExactTotalHits(searcher.count(query), totalHitsPolicy)
                  : TotalHitsData.untracked();
          elapsedTime.stop();
          return new SearchResult<>(
              Collections.emptyList(),
              elapsedTime.elapsed(TimeUnit.MICROSECONDS),
              0,
              0,
              1,
              1,
              totalHitsData.value(),
              totalHitsData.relation(),
              null);
        }

        List<Collector> collectors = new ArrayList<>(3);
        if (topFieldCollector != null) {
          collectors.add(topFieldCollector);
        }
        if (aggregationExecution != null) {
          collectors.add(aggregationExecution.collector());
        }
        if (totalHitCountCollector != null) {
          collectors.add(totalHitCountCollector);
        }
        searcher.search(query, MultiCollector.wrap(collectors));

        TotalHitsData totalHitsData;
        if (topFieldCollector != null) {
          TopDocs topDocs = topFieldCollector.topDocs();
          totalHitsData =
              totalHitsFromTopDocs(topDocs, aggregationExecution != null, totalHitsPolicy);
          ScoreDoc[] hits = topDocs.scoreDocs;
          results = new ArrayList<>(hits.length);
          for (ScoreDoc hit : hits) {
            results.add(buildLogMessage(searcher, hit, sourceFieldFilter));
          }
        } else {
          totalHitsData =
              totalHitCountCollector == null
                  ? TotalHitsData.untracked()
                  : thresholdExactTotalHits(totalHitCountCollector.getTotalHits(), totalHitsPolicy);
          results = Collections.emptyList();
        }
        if (aggregationExecution != null) {
          internalAggregations = aggregationExecution.finish();
        }

        elapsedTime.stop();
        return new SearchResult<>(
            results,
            elapsedTime.elapsed(TimeUnit.MICROSECONDS),
            0,
            0,
            1,
            1,
            totalHitsData.value(),
            totalHitsData.relation(),
            internalAggregations);
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

  private LogMessage buildLogMessage(
      IndexSearcher searcher, ScoreDoc hit, SourceFieldFilter sourceFieldFilter) {
    String s = "";
    try {
      s = searcher.doc(hit.doc).get(SystemField.SOURCE.fieldName);
      LogWireMessage wireMessage = JsonUtil.read(s, LogWireMessage.class);
      Map<String, Object> source = wireMessage.getSource();

      if (sourceFieldFilter != null
          && sourceFieldFilter.getFilterType() == SourceFieldFilter.FilterType.INCLUDE) {
        source =
            wireMessage.getSource().keySet().stream()
                .filter(sourceFieldFilter::appliesToField)
                .collect(Collectors.toMap((key) -> key, (key) -> wireMessage.getSource().get(key)));
      } else if (sourceFieldFilter != null
          && sourceFieldFilter.getFilterType() == SourceFieldFilter.FilterType.EXCLUDE) {
        source =
            wireMessage.getSource().keySet().stream()
                .filter((key) -> !sourceFieldFilter.appliesToField(key))
                .collect(Collectors.toMap((key) -> key, (key) -> wireMessage.getSource().get(key)));
      }

      return new LogMessage(
          wireMessage.getIndex(),
          wireMessage.getType(),
          wireMessage.getId(),
          wireMessage.getTimestamp(),
          source);
    } catch (Exception e) {
      throw new IllegalStateException("Error fetching and parsing a result from index: " + s, e);
    }
  }

  /**
   * Builds a top field collector for the requested amount of results, with the option to set the
   * totalHitsThreshold. If the totalHitsThreshold is set to Integer.MAX_VALUE it will force a
   * ScoreMode.COMPLETE, iterating over all documents at the expense of a longer query time. This
   * value can be set to equal howMany to allow early exiting (ScoreMode.TOP_SCORES), but should
   * only be done when all collectors are tolerant of an early exit.
   */
  private TopFieldCollector buildTopFieldCollector(
      int howMany, int totalHitsThreshold, List<SearchQuery.SortFieldSpec> sortFieldSpecs)
      throws IOException {
    return TopFieldCollector.create(
        new Sort(buildSortFields(sortFieldSpecs)), howMany, null, totalHitsThreshold);
  }

  private SortField[] buildSortFields(List<SearchQuery.SortFieldSpec> sortFieldSpecs) {
    List<SearchQuery.SortFieldSpec> effectiveSortFieldSpecs =
        sortFieldSpecs == null || sortFieldSpecs.isEmpty()
            ? List.of(
                new SearchQuery.SortFieldSpec(
                    SystemField.TIME_SINCE_EPOCH.fieldName, SearchQuery.SortDirection.DESC))
            : sortFieldSpecs;

    return effectiveSortFieldSpecs.stream().map(this::buildSortField).toArray(SortField[]::new);
  }

  private SortField buildSortField(SearchQuery.SortFieldSpec sortFieldSpec) {
    String fieldName = sortFieldSpec.field();
    if (SystemField.TIME_SINCE_EPOCH.fieldName.equals(fieldName)) {
      return new SortField(fieldName, Type.LONG, sortFieldSpec.descending());
    }

    LuceneFieldDef fieldDef = chunkSchema.get(fieldName);
    if (fieldDef == null) {
      throw new IllegalArgumentException("Unsupported sort field: " + fieldName);
    }
    ensureTrue(fieldDef.storeDocValue, "Sort field must have doc values: " + fieldName);
    return new SortField(
        fieldName, toLuceneSortFieldType(fieldDef.fieldType), sortFieldSpec.descending());
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

  private record TotalHitsData(long value, SearchResult.TotalHitsRelation relation) {
    private static TotalHitsData untracked() {
      return new TotalHitsData(0, null);
    }
  }

  private static int totalHitsThreshold(
      OpenSearchAdapter.AggregationExecution aggregationExecution,
      SearchQuery.TotalHitsPolicy totalHitsPolicy) {
    if (aggregationExecution != null) {
      return Integer.MAX_VALUE;
    }
    if (!totalHitsPolicy.enabled()) {
      return 0;
    }
    return totalHitsPolicy.isExact() ? Integer.MAX_VALUE : totalHitsPolicy.threshold();
  }

  private static TotalHitsData totalHitsFromTopDocs(
      TopDocs topDocs,
      boolean exactBecauseOfAggregation,
      SearchQuery.TotalHitsPolicy totalHitsPolicy) {
    if (!totalHitsPolicy.enabled()) {
      return TotalHitsData.untracked();
    }
    if (exactBecauseOfAggregation) {
      return thresholdExactTotalHits(topDocs.totalHits.value, totalHitsPolicy);
    }
    SearchResult.TotalHitsRelation relation =
        totalHitsRelationFromLucene(topDocs.totalHits.relation);
    if (!totalHitsPolicy.isExact() && topDocs.totalHits.value > totalHitsPolicy.threshold()) {
      return new TotalHitsData(
          totalHitsPolicy.threshold(), SearchResult.TotalHitsRelation.GREATER_THAN_OR_EQUAL_TO);
    }
    return new TotalHitsData(topDocs.totalHits.value, relation);
  }

  private static TotalHitsData thresholdExactTotalHits(
      long exactTotalHits, SearchQuery.TotalHitsPolicy totalHitsPolicy) {
    if (!totalHitsPolicy.enabled()) {
      return TotalHitsData.untracked();
    }
    if (totalHitsPolicy.isExact() || exactTotalHits <= totalHitsPolicy.threshold()) {
      return new TotalHitsData(exactTotalHits, SearchResult.TotalHitsRelation.EQUAL_TO);
    }
    return new TotalHitsData(
        totalHitsPolicy.threshold(), SearchResult.TotalHitsRelation.GREATER_THAN_OR_EQUAL_TO);
  }

  private static SearchResult.TotalHitsRelation totalHitsRelationFromLucene(
      TotalHits.Relation relation) {
    return switch (relation) {
      case EQUAL_TO -> SearchResult.TotalHitsRelation.EQUAL_TO;
      case GREATER_THAN_OR_EQUAL_TO -> SearchResult.TotalHitsRelation.GREATER_THAN_OR_EQUAL_TO;
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
