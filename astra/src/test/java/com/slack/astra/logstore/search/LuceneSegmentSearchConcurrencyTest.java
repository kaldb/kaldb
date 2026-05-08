package com.slack.astra.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneSegmentSearchConcurrencyTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(LuceneSegmentSearchConcurrencyTest.class);
  private static final int SEGMENT_COUNT = 12;

  @Test
  public void collectorManagerWithoutSearcherExecutorScansSegmentsSerially() throws Exception {
    try (Directory directory = buildMultiSegmentIndex();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader);

      assertThat(searcher.getLeafContexts()).hasSize(SEGMENT_COUNT);

      long callingThreadId = Thread.currentThread().threadId();
      SearchObservation observation =
          searcher.search(new MatchAllDocsQuery(), new ThreadRecordingCollectorManager(null));

      LOG.info("CollectorManager without executor observation: {}", observation);
      assertThat(observation.documentCount()).isEqualTo(SEGMENT_COUNT);
      assertThat(observation.leafOrds()).hasSize(SEGMENT_COUNT);
      assertThat(observation.collectorCount()).isEqualTo(1);
      assertThat(observation.threadIds()).containsOnly(callingThreadId);
    }
  }

  @Test
  public void collectorSearchWithSearcherExecutorStillScansSegmentsSerially() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(SEGMENT_COUNT);
    try (Directory directory = buildMultiSegmentIndex();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader, executor);
      ThreadRecordingCollector collector = new ThreadRecordingCollector(null);

      assertThat(searcher.getLeafContexts()).hasSize(SEGMENT_COUNT);
      assertThat(searcher.getSlices()).hasSizeGreaterThan(1);

      long callingThreadId = Thread.currentThread().threadId();
      searcher.search(new MatchAllDocsQuery(), collector);

      SearchObservation observation = collector.toObservation();
      LOG.info("Direct Collector with executor observation: {}", observation);
      assertThat(observation.documentCount()).isEqualTo(SEGMENT_COUNT);
      assertThat(observation.leafOrds()).hasSize(SEGMENT_COUNT);
      assertThat(observation.collectorCount()).isEqualTo(1);
      assertThat(observation.threadIds()).containsOnly(callingThreadId);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void collectorManagerWithSearcherExecutorScansSegmentSlicesInParallel() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(SEGMENT_COUNT);
    try (Directory directory = buildMultiSegmentIndex();
        DirectoryReader reader = DirectoryReader.open(directory)) {
      IndexSearcher searcher = new IndexSearcher(reader, executor);
      int sliceCount = searcher.getSlices().length;

      assertThat(searcher.getLeafContexts()).hasSize(SEGMENT_COUNT);
      assertThat(sliceCount).isGreaterThan(1);

      SearchObservation observation =
          searcher.search(
              new MatchAllDocsQuery(),
              new ThreadRecordingCollectorManager(new CyclicBarrier(sliceCount)));

      LOG.info("CollectorManager with executor observation: {}", observation);
      assertThat(observation.documentCount()).isEqualTo(SEGMENT_COUNT);
      assertThat(observation.leafOrds()).hasSize(SEGMENT_COUNT);
      assertThat(observation.collectorCount()).isEqualTo(sliceCount);
      assertThat(observation.threadIds()).hasSizeGreaterThan(1);
    } finally {
      executor.shutdownNow();
    }
  }

  private static Directory buildMultiSegmentIndex() throws IOException {
    Directory directory = new ByteBuffersDirectory();
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
    indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    indexWriterConfig.setUseCompoundFile(false);

    try (IndexWriter writer = new IndexWriter(directory, indexWriterConfig)) {
      for (int i = 0; i < SEGMENT_COUNT; i++) {
        Document document = new Document();
        document.add(new StringField("id", Integer.toString(i), Field.Store.NO));
        writer.addDocument(document);
        writer.flush();
        writer.commit();
      }
    }
    return directory;
  }

  private record SearchObservation(
      int collectorCount, int documentCount, Set<Long> threadIds, Set<Integer> leafOrds) {}

  private static class ThreadRecordingCollectorManager
      implements CollectorManager<ThreadRecordingCollector, SearchObservation> {
    private final CyclicBarrier firstLeafBarrier;
    private final AtomicInteger collectorIdGenerator = new AtomicInteger();

    private ThreadRecordingCollectorManager(CyclicBarrier firstLeafBarrier) {
      this.firstLeafBarrier = firstLeafBarrier;
    }

    @Override
    public ThreadRecordingCollector newCollector() {
      int collectorId = collectorIdGenerator.incrementAndGet();
      LOG.info("Creating collector {}", collectorId);
      return new ThreadRecordingCollector(collectorId, firstLeafBarrier);
    }

    @Override
    public SearchObservation reduce(Collection<ThreadRecordingCollector> collectors) {
      int documentCount = 0;
      Set<Long> threadIds = new HashSet<>();
      Set<Integer> leafOrds = new HashSet<>();
      for (ThreadRecordingCollector collector : collectors) {
        SearchObservation observation = collector.toObservation();
        documentCount += observation.documentCount();
        threadIds.addAll(observation.threadIds());
        leafOrds.addAll(observation.leafOrds());
      }
      return new SearchObservation(collectors.size(), documentCount, threadIds, leafOrds);
    }
  }

  private static class ThreadRecordingCollector extends SimpleCollector {
    private final int collectorId;
    private final Set<Long> threadIds = ConcurrentHashMap.newKeySet();
    private final Set<Integer> leafOrds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger documentCount = new AtomicInteger();
    private final AtomicBoolean waitedOnFirstLeaf = new AtomicBoolean();
    private final CyclicBarrier firstLeafBarrier;

    private ThreadRecordingCollector(CyclicBarrier firstLeafBarrier) {
      this(1, firstLeafBarrier);
    }

    private ThreadRecordingCollector(int collectorId, CyclicBarrier firstLeafBarrier) {
      this.collectorId = collectorId;
      this.firstLeafBarrier = firstLeafBarrier;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      recordThread();
      leafOrds.add(context.ord);
      LOG.info(
          "Collector {} entering leaf ord={} maxDoc={} thread={}",
          collectorId,
          context.ord,
          context.reader().maxDoc(),
          Thread.currentThread().getName());
      if (firstLeafBarrier != null && waitedOnFirstLeaf.compareAndSet(false, true)) {
        awaitFirstLeafBarrier();
      }
    }

    @Override
    public void collect(int doc) {
      recordThread();
      documentCount.incrementAndGet();
      LOG.info(
          "Collector {} collected doc={} thread={}",
          collectorId,
          doc,
          Thread.currentThread().getName());
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    private SearchObservation toObservation() {
      return new SearchObservation(
          1, documentCount.get(), Set.copyOf(threadIds), Set.copyOf(leafOrds));
    }

    private void recordThread() {
      threadIds.add(Thread.currentThread().threadId());
    }

    private void awaitFirstLeafBarrier() throws IOException {
      try {
        LOG.info(
            "Collector {} waiting at first-leaf barrier on thread={}",
            collectorId,
            Thread.currentThread().getName());
        firstLeafBarrier.await(5, TimeUnit.SECONDS);
        LOG.info(
            "Collector {} passed first-leaf barrier on thread={}",
            collectorId,
            Thread.currentThread().getName());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (TimeoutException | java.util.concurrent.BrokenBarrierException e) {
        throw new IOException(e);
      }
    }
  }
}
