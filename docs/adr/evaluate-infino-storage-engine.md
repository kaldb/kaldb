# ADR 0009: Replace Lucene Readers and Writers with Infino through Java FFM

## Status

Current state: `Draft`

Date: 2026-11-05

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

Replace KalDB's Lucene index writer and index reader with Infino, embedded in the
JVM process through a Rust C ABI shim and Java's Foreign Function and Memory
(FFM) API. Upgrade the build and runtime from Java 21 to **Java 25 LTS** as
part of the same work. Preserve KalDB's existing search and ingestion behavior.

The engineering question is **how much adapter and compatibility code we need
to build**. Infino already supplies storage, indexes, SQL execution, and caching.
FFM supplies the native call mechanism. Neither supplies KalDB's OpenSearch
semantics or its distributed aggregation result representation.

The proposed boundary is KalDB's `LogStore` and `LogIndexSearcher`, rather than
Lucene's `IndexWriter`/`IndexReader` classes. Keep Kafka ingestion, chunks,
ZooKeeper coordination, query routing, HTTP/gRPC APIs, and coordinator reduction.
There is no separate engine service in this design.

Code was reviewed at KalDB commit `9983fff8a53729d287d2d2a54e1860717bda3c92`
and Infino commit
[`44dceb2fc7e6f5e414a108ee5dbc19746ca0067d`](https://github.com/infino-ai/infino/tree/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d)
(engine `0.11.24`). API findings below describe that pin; proposed shim symbols
and Java classes are ours to build.

## Questions

- **Can this be just two replacement classes?** The two main implementations
  can be `InfinoIndexStore` and `InfinoIndexSearcher`, but their dependencies
  also need adapting: document encoding, query translation, aggregation states,
  snapshot files, native binding, and lifecycle management.
- **Can we keep existing APIs?** Keep `addMessage(Trace.Span)` and
  `search(SearchQuery) -> SearchResult<LogMessage>`, including the current
  request/result proto shapes. Change the internal methods that expose Lucene
  readers, directories, and commits. External conformance does not require
  implementing Lucene's reader internals.
- **Does Infino need modifications?** Basic writes, scalar/text queries, sorting,
  and simple aggregates can use its public API. Exact analyzer/query-language
  parity, mergeable sketch state, and exportable read snapshots are potential
  upstream extensions. They are identified below rather than assumed to exist.
- **Where is most of the build?** OpenSearch query translation and aggregation
  compatibility. The native binding is bounded glue; it does not remove that work.

## Public Interfaces

Keep OpenSearch/Zipkin request and response shapes, field discovery, source
filtering/redaction, sort values, pagination, errors, and logical shard coverage.
Keep `SearchQuery`, `SearchResult`, `astra_search.proto`, and existing
`InternalAggregations` transport unless a specific unsupported aggregation
forces a separately reviewed change.

The implementation adds a native library artifact and loader settings, and
raises the compiler, bytecode, and runtime baseline to Java 25, the latest LTS
as of this ADR's date. Pin a maintained Corretto 25 patch/build for CI and
production. See the [Java support roadmap](https://www.oracle.com/java/technologies/java-se-support-roadmap.html)
and [Corretto 25 container documentation](https://docs.aws.amazon.com/corretto/latest/corretto-25-ug/docker-install.html).

Use Java 25's permanent `java.lang.foreign` API for the shim. FFM itself no
longer needs preview flags, but retain `--enable-preview` while KalDB uses
Java 25's preview structured-concurrency API. For the current classpath
deployment, also pass `--enable-native-access=ALL-UNNAMED` to application,
FFM tests, and benchmark launches. See the
[Java 25 FFM documentation](https://docs.oracle.com/en/java/javase/25/core/foreign-function-and-memory-api.html)
and [native-access requirements](https://docs.oracle.com/en/java/javase/25/core/restricted-methods.html).

New engine/batching/cache settings belong in `astra_configs.proto` with field
comments, config reference updates, and sample config changes. The ADR itself
changes no product code or proto.

## Proposed Changes

### Java 25 LTS prerequisite

Complete and validate the Java upgrade with Lucene still in place before wiring
Infino into the reader/writer path. This keeps runtime migration failures
separate from engine-adapter failures; both changes remain in this ADR's scope.

| Existing area | Required upgrade work |
| --- | --- |
| Root, `astra/`, `benchmarks/`, and `tools/loadgen/` POMs | Set the Java compiler/release target to 25 consistently; verify inherited settings for `contrib/dashboards-gateway/`. Compile all modules with JDK 25. |
| `Dockerfile`, `tools/loadgen/Dockerfile`, `contrib/dashboards-gateway.Dockerfile` | Use a Maven builder running Corretto 25 and a matching Corretto 25 runtime; pin supported image versions/digests when implementing. |
| `.github/workflows/maven.yml`, `.github/workflows/benchmark.yml`, development and launch scripts | Use JDK 25 for builds/tests/JMH; update local setup instructions and native-access/preview launch flags. |
| `logstore/search/AstraDistributedQueryService.java` | Migrate Java 21 scope constructors, single-parameter scope types, `joinUntil`, and `shutdown` usage to Java 25's `StructuredTaskScope.open`, joiner, and timeout configuration APIs. |
| Compiler/test/instrumentation dependencies | Verify Java 25 support in Maven compiler/Surefire, Error Prone, formatting tools, Mockito/Byte Buddy, agents, and the selected Arrow Java version; upgrade only what compatibility checks require. |

The scope migration is real source work: current search and schema fan-out
construct scopes directly, and distributed search collects successful subtasks
while recording failed/timed-out coverage. Java 25's default `open()` policy
fails the scope when a subtask fails; do not adopt it blindly. Choose an explicit
collect-all/join policy and preserve deadlines, cancellation, successful partial
results, trace propagation, and `_shards` accounting. Validate schema fan-out
behavior too. The source path above is relative to
`astra/src/main/java/com/slack/astra/`; the target API is documented in
[Java 25 StructuredTaskScope](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/StructuredTaskScope.html).

Keep preview flags only for features that still require them. Do not remove
`--enable-preview` merely because FFM is permanent. Track JVM heap, native
allocations, and mapped pages under the Java 25 runtime used for both engines.

### 1. Integration boundary

```text
Existing Kafka / chunks / search routing
                |
      +---------+----------+
      |                    |
InfinoIndexStore     InfinoIndexSearcher
      |                    |
normalize -> batch    SearchQuery -> SQL / search predicates
      |                    |
      +------ Java FFM -----+
                |
       libkaldb_infino (Rust cdylib, C ABI)
                |
       Infino Connection / Supertable
                |
       local chunk root or S3 + cache
```

Keep one Infino table in a self-contained database root per KalDB chunk initially.
This preserves current ownership, rollover, retention, and recovery boundaries.
There is no need to redesign KalDB around shared dataset tables.

Relevant current interfaces:
[LogStore](../../astra/src/main/java/com/slack/astra/logstore/LogStore.java),
[LogIndexSearcher](../../astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcher.java),
[SearchQuery](../../astra/src/main/java/com/slack/astra/logstore/search/SearchQuery.java),
and [SearchResult](../../astra/src/main/java/com/slack/astra/logstore/search/SearchResult.java).

### 2. Infino APIs we can use directly

These are existing Rust APIs, not proposed functions:

| Infino public API | What it supplies for KalDB |
| --- | --- |
| `connect_with(uri, ConnectOptions)` | Open a local/S3 database; configure credentials, read consistency, disk cache, and connection memory budget. |
| `Connection::create_table(name, SchemaRef, IndexSpec)` / `open_table(name)` | Create/open the chunk's table. |
| `IndexSpec::fts(FtsField)`, `IndexSpec::range(column)` | Declare text and numeric/date range indexes. |
| `FtsField::positions(true)`, `stored(bool)` | Enable exact phrase positions and choose whether raw indexed text is stored. |
| `Supertable::append(&RecordBatch)` | Append a typed batch; the public append operation commits it. No per-document writer API is needed. |
| `Supertable::append_rows(&[serde_json::Value])` | Alternative JSON ingestion with Infino's own flattening/type rules; not a replacement for KalDB normalization. |
| `Connection::apply_schema(table, &SchemaPatch, expected_schema_id)` | Add/change logical columns and indexes, with optional schema-version checking. |
| `Connection::query_sql(&str)` | Execute filters, projections, ordering, limits, grouping, and SQL aggregates; returns `Vec<RecordBatch>`. |
| `query_sql_cancellable(&str, &QueryCancel)` | Run a query with an independently signalled cancellation token. |
| `Supertable::token_match(column, query, BoolMode, projection)` | Unranked matching over all matches. The implementation handles quoted exact phrases and clause sigils as well as plain tokens. |
| `Supertable::exact_match(column, value, projection)` | Raw string equality with candidate pruning and verification. |
| `Supertable::bm25_search(column, query, k, options, projection)` | Ranked retrieval, available when ranking is actually requested; not KalDB's default time-ordered search. |
| `schema()`, `Connection::schema(table)` | Physical Arrow schema and richer logical field/index metadata respectively. |
| `optimize(&OptimizeOptions)`, `gc(Duration)` | Maintenance and garbage collection, not Lucene commit/refresh equivalents. |
| `Connection::update_storage_credentials(...)` | Rotate credentials used by native storage access. |

Sources: pinned
[public API](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/public-api.txt),
[table methods](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/catalog/table.rs),
[index specification](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/catalog/index_spec.rs),
[SQL search functions](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/catalog/search_tvf.rs),
[unranked matching implementation](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/supertable/query/fts.rs),
and [connection options](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/catalog/options.rs).

Missing from that public surface: a C ABI, Lucene reader/collector interfaces,
OpenSearch DSL parsing, independent stage/commit/refresh operations, an externally
retained snapshot/export handle, and OpenSearch-compatible aggregation states.
The binding must not depend on Infino's `test-helpers` to expose private internals.

### 3. Writer: map the current contract

| Current KalDB operation | Infino mapping | What we build/change |
| --- | --- | --- |
| `addMessage(Trace.Span)` | Normalize to typed columns; accumulate; `append(RecordBatch)` on flush. | Batch encoder, row/byte cap, maximum wait, admission/backpressure, and equivalent invalid-document accounting. Avoid one append/FFM call per event. |
| `commit()` | Drain pending batches and wait for successful append publication. | Serialize flushes, propagate failures, and advance durable local progress only after success. |
| `refresh()` | Flush pending rows and make the published generation available to searches. | Infino has no public NRT refresh of uncommitted data. Refresh also commits; document this stronger durability/cost behavior. |
| `finalMerge()` | Optional `optimize` compaction after flushing. | Remove dependence on a Lucene single-segment result; snapshot correctness must not require optimization. |
| `getAstraSearcherManager()` | Return/open the Infino-backed `LogIndexSearcher`. | Replace the Lucene-specific method with `openSearcher()`; construct no Lucene manager. |
| `getDirectory()` | Chunk database root path. | Change `FSDirectory` to `Path` at this internal boundary. |
| `getIndexCommit()` / `releaseIndexCommit(...)` | A stable sealed chunk export. | Replace with a snapshot resource describing the frozen root and its recursive files; see below. |
| `getSchema()` | Existing logical field definitions plus a native schema mapping. | Keep existing field capabilities and policies; do not derive them solely from Parquet storage. |
| `close()` / `cleanup()` | Release table/connection handles; remove owned local root after readers finish. | Stop timers, drain in-flight calls, explicit flush/discard behavior, and deterministic native release. |

KalDB currently allows `refresh()` without `commit()`; Infino couples visibility
to append publication. Proposed adapter behavior is to flush on either call,
with the batch timer meeting the configured refresh bound. The commit timer can
flush earlier, so data may become visible earlier too. Reopening a separate reader
can verify visibility but cannot create independent NRT semantics. If exact
separation of uncommitted visibility and durability is a requirement, public
Infino is insufficient: it needs a staged-write/NRT extension. Do not present
a no-op refresh as equivalent.

Reuse the normalization rules in
[SchemaAwareLogDocumentBuilderImpl](../../astra/src/main/java/com/slack/astra/logstore/schema/SchemaAwareLogDocumentBuilderImpl.java)
and conversion policies in
[FieldType](../../astra/src/main/java/com/slack/astra/metadata/schema/FieldType.java).
Extract the Lucene `Document` emission into a replaceable sink; do not duplicate
hundreds of lines of validation or build a Lucene document just to read it back.

| Logical data | Proposed physical mapping / compatibility work |
| --- | --- |
| Stored `_source` | Preserve the existing serialized `LogWireMessage` envelope so hit reconstruction can stay in Java. |
| KalDB `_id` | Separate physical name, e.g. `kaldb_id`, because Infino owns a numeric `_id`. Store original ID plus an ordering key produced by existing `Uid.encodeId`; lexical ID-string sorting is not equivalent. |
| Dates and event time | Epoch-millisecond integer columns for current comparisons; preserve source timestamp representation and normalization. |
| Integer/long/float/double/boolean | Corresponding Arrow types; preserve conversion and rounding rules, including half-float/scaled-long behavior where used. |
| Keyword/string | Case-sensitive stored scalar; SQL equality/IN for whole-value matching. Do not replace keyword semantics with analyzed token matching. |
| Text and `_all` | FTS columns, exact-phrase positions where required. Build `_all` from the same fields/stringified values and honor disabled full-text search. |
| IP | Preserve original source plus a comparable encoded key for sort/equality/range/CIDR lowering. A string comparison is insufficient. |
| Dynamic fields | Apply explicit schema patches before affected appends; preserve depth/field limits, conflict conversion/duplicate fields, list handling, and index flags. |

Infino's `standard` analyzer is its own Unicode implementation; the public
`FtsField::analyzer` currently accepts only that analyzer name. Identical names
do not establish equivalence with Lucene `StandardAnalyzer`. Compare actual
token streams. Fix incompatible cases through an upstream analyzer extension
or explicitly approved behavior changes; the C shim cannot repair incompatible
postings after indexing.

### 4. Reader: conform to SearchQuery and SearchResult

Implement `InfinoIndexSearcher.search(SearchQuery)` and return the current
`SearchResult<LogMessage>`, including typed `HitSortValue` values and coverage.
The [existing searcher](../../astra/src/main/java/com/slack/astra/logstore/search/LogIndexSearcherImpl.java)
currently creates Lucene queries and collectors, then reads `_source`.
Replace that local execution; retain Java hit reconstruction, source filtering,
and the coordinator's merge.

Build one Java query translator over OpenSearch builders:

| Existing behavior | Infino execution | Adapter work / remaining gap |
| --- | --- | --- |
| Dataset scoping | Scalar equality on `service_name`. | Preserve `OpenSearchAdapter.buildQuery`'s existing logical-index restriction and selector validation; chunks can contain several indices. |
| Match all/none, term(s), range, exists | SQL predicates, IN, comparisons, null handling. | Consult logical schema; preserve indexed/doc-value presence rules rather than treating every source field as searchable. Preserve unmapped-field and empty-string behavior. |
| Boolean must/filter/should/must_not | SQL boolean expressions and membership subqueries. | Translate nested trees and `minimum_should_match`; use explicit two-valued predicates where SQL NULL would change negation. |
| Text / exact phrase | `token_match('table', 'column', 'query', 'and/or')` as a SQL table function; compose its physical IDs with scalar filters. | Quote literal text for Infino's clause parser; enable positions. Exact phrases are available; arbitrary slop/proximity equivalence is not established. |
| Query-string syntax | Parse existing field qualifiers, grouping, ranges, operators, escapes, and wildcard forms, then lower to predicates. | Infino's parser handles tokens, quoted phrases, and leading `+`/`-`; it is not OpenSearch's parser. Reuse Java parser components where they can run without a Lucene reader; do not pass the input string through unchanged. |
| Prefix/wildcard/regex/fuzzy | SQL/scalar matching or an explicit search extension, depending on field semantics. | SQL LIKE matches raw values, whereas a text wildcard matches indexed terms. Ranked `bm25_search_prefix` is not an unlimited unranked prefix predicate. Correct term expansion/unranked prefix and fuzzy/slop support may require upstream work. |
| Default and explicit sort | SQL ORDER BY over effective sort fields, LIMIT `from + size`. | Time descending plus encoded KalDB ID ascending by default; missing values, numeric sentinels, UTF-8 byte ordering, and IP keys must match current worker/coordinator behavior. Apply offset only at the global merge. |
| Returned hits | Project source and sort columns; decode into existing Java hits. | Preserve sort-value types/encodings, user-visible sort fields, 10,000-result limit, and source filters. |
| Redaction / failures | Java result handling after native execution. | Apply existing timestamp-based source redaction before projection/return; preserve its current effect on aggregations. Map native errors and missing chunks to current failures, not successful empty results. |

The catalog-level SQL functions take the table name first; the public Rust
table methods take the column first. These are distinct interfaces.

For example, the adapter could generate this time-sorted text search (physical
names are illustrative):

```sql
SELECT "_source", "_timesinceepoch", "kaldb_id_sort"
FROM token_match('chunk', '_all', 'connection timeout', 'or')
WHERE service_name = 'payments'
  AND "_timesinceepoch" >= 1700000000000
ORDER BY "_timesinceepoch" DESC, "kaldb_id_sort" ASC
LIMIT 100;
```

This uses all text matches before time sorting. A BM25 top-100 followed by a
time sort can return the wrong newest 100. Similarly, aggregation input must be
all matches rather than the displayed hit subset.

The public SQL API takes a string and materializes `Vec<RecordBatch>`; it does
not expose a prepared-parameter API or streaming query cursor at this revision.
Generate SQL only from validated builders, with centralized identifier/literal
escaping. Bound output and connection memory. Iterating materialized batches
through FFM is not streaming engine execution.

Hits and aggregations for one request must observe one chunk generation.
Until Infino exposes a public reusable read-snapshot handle, hold a per-chunk
shared read gate across those calls, excluding adapter append/schema/maintenance
publication. Concurrent searches may share the gate; flush waits for them.
No other process may write that chunk root. This is extra adapter work and a
potential ingestion-latency cost. A public pinned-session API would remove that
restriction; FFM alone does not create one.

### 5. Aggregations: reuse execution primitives, build compatible states

[OpenSearchAdapter](../../astra/src/main/java/com/slack/astra/logstore/opensearch/OpenSearchAdapter.java)
currently registers the following local aggregation families. Their collectors
depend on Lucene document IDs and doc values. They cannot collect Arrow batches
without a substantial Lucene reader emulation layer.

Instead, translate local aggregation work to SQL, then construct the existing
OpenSearch `InternalAggregation` objects from partial states:

| Current aggregation | What Infino/DataFusion can compute | What KalDB must build |
| --- | --- | --- |
| sum, min, max | SQL SUM/MIN/MAX | Result objects with existing names, metadata, formats, empty/missing rules. |
| avg | SUM + COUNT | `InternalAvg` with sum/count, never an average of chunk averages. |
| value_count | COUNT of contributing values | Preserve missing-value substitution and current field/list semantics. |
| extended_stats | Count, sum, min/max, squared sums or equivalent state | Compatible partial-state encoding, variance/sigma/empty semantics and numeric validation. |
| histogram / date_histogram | Group by computed numeric/time bucket | OpenSearch rounding, offsets, timezones/DST, calendar intervals, bounds, empty buckets, ordering, and nested results. |
| auto_date_histogram | Counts and candidate resolutions | Compatible interval-selection and reduction metadata; not just DATE_TRUNC. |
| terms / multi_terms | GROUP BY one/multiple keys and metrics | Shard candidate sizing/order, missing keys, bucket counts/error metadata, and compatible bucket objects. Do not truncate each shard to final size and assume a correct global top-k. |
| filters | Conditional/grouped aggregation over translated predicates | Named/other buckets and nested states. |
| cardinality | Distinct counting is possible | OpenSearch-compatible mergeable HLL++ state is not exposed by Infino. Build it from contributing values in Java or add compatible native state support; scalar per-chunk estimates cannot be merged. |
| percentiles | SQL percentile computation is a different result contract | Reuse OpenSearch's configured sketch implementation over returned values, or implement a compatible native sketch/state export. Final per-chunk percentiles are insufficient. |
| siblings and sub-aggregations | Multiple expressions and grouping plans | Traverse the complete builder tree, share filters/work where possible, and reconstruct the complete named result tree. |

Keep `SearchResultAggregatorImpl` and `OpenSearchInternalAggregation` transport
for compatible local states. Existing final pipeline reduction can then stay in
Java. Test the actual constructors/reducer invariants for each result type;
an object with the correct final JSON shape is not necessarily reducible.

Aggregation scripts are existing behavior, not hypothetical scope:
`LogIndexSearcherImplTest.testAggregationWithScripting` expects Painless
`return 9;` to affect an average. Infino has no Painless execution API.
Reuse the Java script engine where possible and provide the required value/doc
lookup context; scripts bound to Lucene fielddata need a new context or
execution adapter. SQL is not a general substitute. Returning matched values
to Java is a viable compatibility path for sketches/scripts, but requires
batch processing, memory limits, and measured transfer cost.

This is the largest implementation area. Infino supplies the local SQL work;
we still own OpenSearch bucket rules, partial-state construction, scripts, and
cross-chunk correctness.

### 6. FFM shim: bounded native glue

Build a Rust `cdylib` depending on the pinned Infino crate and export a small,
versioned C ABI. No C API or Java binding was identified in the reviewed repo.
Use opaque handles and C-compatible values; never pass Rust `String`, `Vec`,
`Result`, trait objects, or Arrow Rust struct layouts to Java.

Expect approximately **15–20 C exports**, grouped as follows. These names are
proposed wrapper operations, not existing Infino APIs:

| Shim group | Responsibility |
| --- | --- |
| ABI/version and database open/close | Validate version; own a `Connection`; apply storage/cache/memory options. |
| Table create/open/release and schema patch | Own `Supertable` handles; convert Arrow schema/index specifications. |
| Batch append | Decode a typed batch and invoke `Supertable::append`. |
| Query create/run/cancel/release | Own SQL and `QueryCancel`; allow cancellation from another Java thread. |
| Result batch access/release and native buffer release | Expose materialized result batches with explicit allocator/lifetime ownership. |
| Credential update | Forward refreshed credentials to the connection. |

Use Arrow IPC bytes for typed input/output initially, carried as pointer/length
buffers. Rust converts IPC batches to/from `RecordBatch`; Java builds/reads
batches and converts hits or partial states. This adds encoding/copy work and
a Java Arrow dependency. Measure it separately. The
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
can replace IPC if that overhead matters, but requires schema/buffer ownership
and release callbacks; it is not provided automatically by FFM.

Every export returns a status and an owned/explicitly released error payload.
Catch Rust unwinding panics at the boundary; never unwind into Java. Invalid
handles, close-versus-query races, double release, and cancellation have defined
outcomes. Fatal native faults still affect the JVM. Keep one owner for every
allocation and disable Infino's default `mimalloc` feature for the first
embedded build, following its
[embedding guidance](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/lib.rs).

Build/package Linux production and macOS development artifacts for supported
architectures, with native symbols for profiling. Add Rust build/version checks
to Maven/CI/container packaging. This is library integration work; it adds no
RPC server, process supervisor, or network protocol.

Allocate connection memory and per-table cache budgets from a node-wide limit;
one full budget per chunk would multiply memory/disk use by the chunk count.
Bound native calls with KalDB's query/build admission limits and measure native
thread growth. These limits are part of the adapter, not supplied by FFM.

### 7. Required Java call-site and snapshot changes

| Existing code | Concrete change |
| --- | --- |
| `logstore/LogStore.java` | Preserve write/lifecycle operations; replace searcher-manager, FSDirectory, and IndexCommit return types. Add engine-independent store identity to remove the Lucene cast. |
| `logstore/DocumentBuilder.java`, `logstore/schema/SchemaAwareLogDocumentBuilderImpl.java`, `metadata/schema/FieldType.java` | Separate normalization/logical type policy from Lucene field emission; feed Arrow batches. |
| `logstore/search/LogIndexSearcherImpl.java`, `logstore/search/AstraSearcherManager.java` | Supply native-handle reader lifecycle and Infino search execution. |
| `logstore/opensearch/OpenSearchAdapter.java` | Retain request builders/formatting as useful; replace Lucene query and collector construction with the query/aggregation adapters described above. |
| `chunk/ReadWriteChunk.java`, `chunkManager/IndexingChunkManager.java`, `chunk/RecoveryChunkFactoryImpl.java` | Construct reader/writer through the new boundary and snapshot a sealed database root. |
| `chunk/ReadOnlyChunkImpl.java` | Open Infino on a downloaded root or its S3 root with a native cache; stop constructing a Lucene directory reader. |
| `logstore/search/fieldRedaction/` | Extract existing source-redaction policy from the Lucene visitor and reuse it when decoding native results. |

Paths in this table are relative to `astra/src/main/java/com/slack/astra/`.

For schema/API discovery, the existing `LuceneFieldDef` is only a logical data
carrier; its name alone does not require replacing the persisted schema format.
Reuse its field flags where appropriate and add physical-name/type mapping in
the adapter. Native SQL must honor those flags even when a value physically
exists in Parquet.

For snapshots, stop writes and maintenance, flush successfully, then freeze the
whole chunk database root. Copy catalog pointers, manifests, summaries,
term-index objects, data, and any other referenced files recursively, alongside
KalDB's `schema.json`. Infino exposes no public `IndexCommit`-like export pin.
The sealed-root approach avoids needing one for current rollover; a live export
would require more work.

The reviewed layout uses catalog-relative table locations and table-relative
manifest/data keys, so the initial implementation should copy the frozen root
without rewriting Infino metadata. Sources:
[catalog entries](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/catalog/manifest.rs),
[manifest keys](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/supertable/manifest/commit.rs),
and [data keys](https://github.com/infino-ai/infino/blob/44dceb2fc7e6f5e414a108ee5dbc19746ca0067d/src/supertable/manifest/mod.rs).
Validate reopening at a different local path and S3 root before relying on that
inference. File counting must handle subdirectories. Keep publication after
successful upload/file verification, and add an independent reopen check for
the Infino snapshot. Account for all root bytes in chunk sizing, including
existing `chunkrollover/` directory-size consumers.

Keep Kafka offsets and recovery ownership in KalDB. A flush failure must not
publish a snapshot covering unwritten rows. An ambiguous successful append
cannot be blindly retried: Infino's generated row ID does not deduplicate Kafka
events. Rebuild the unpublished chunk from its safe replay boundary or add
verified idempotency. Use immutable per-chunk roots for retention; no shared-file
GC redesign is required.

### 8. How much we need to build

**Reuse:** Infino storage/index construction, range/text retrieval, SQL
filter/sort/group execution, S3 access/cache; KalDB ingestion/routing/coordination,
request/result wire shapes, hit merge, source model, and compatible aggregation
reducers.

**Build:** two main reader/writer implementations, one native library and FFM
binding, a typed batch codec/normalization sink, a query translator, an aggregation
translator/state adapter, source redaction integration, and snapshot/lifecycle
changes in existing classes. Avoid a general engine plugin framework.

The following are rough **engineer-week** estimates for one engineer familiar
with Java, Rust, and search internals. They are planning judgments from source
inspection, not measured delivery commitments:

| Work package | Estimate | Principal uncertainty |
| --- | --- | --- |
| Java 21 to Java 25 LTS: toolchain, containers, dependencies, structured concurrency | 1–2 | Preview API source migration and build/instrumentation compatibility. |
| C ABI + FFM + typed batch transport + packaging | 1–2 | Native lifetimes, IPC cost, platform builds. |
| Writer normalization/schema/buffering and commit/refresh mapping | 1–2 | Dynamic schema and visibility behavior. |
| Reader/query translation, hit/sort/source handling | 2–4 | Query-string grammar, ID/IP ordering, analyzer differences. |
| Aggregation states, bucket rules, scripts/sketch integration | 3–6 | Full existing aggregation surface and OpenSearch execution contexts. |
| Snapshot/cache/recovery integration | 1–2 | Root portability and ambiguous writes. |
| Cross-engine contracts, failure tests, benchmark harness | 2–3 | Native concurrency and representative workload coverage. |
| **Total including Java 25, before additional upstream work** | **11–21** | Analyzer/fuzzy/slop extensions or difficult script contexts can add work beyond this range. |

A narrow benchmark slice is about **4–7 engineer-weeks**, included in that total:
Java 25 migration, FFM binding, normalized append, exact/range/token queries, stable sorted hits,
simple histogram/sum/count/average, and a sealed local-root reopen. It is not
API-complete replacement and does not establish S3 portability.

The first implementation spike must resolve three scope multipliers:
(1) token/query semantics against current tests, (2) portable snapshot reopen,
and (3) constructing reducible aggregation states, including one sketch/script
path. If these need upstream work, revise the estimate before treating the
remaining tasks as routine binding work.

## Compatibility, Deprecation, and Migration Plan

Java 25 becomes the minimum runtime for newly built artifacts; they will not run
on Java 21. Upgrade application, gateway, load-generator, and benchmark images
and developer/CI toolchains together. Validate the Lucene-backed build on Java
25 first. Keep the previous Java 21 application image available for a runtime
rollback before any Infino-format cutover; changing the JVM cannot make an
Infino snapshot readable by the old Lucene application.

Preserve external behavior; keep Lucene dependencies needed for OpenSearch
parsing, ID/IP encoding, scripts, and result reduction. Replacing the storage
engine does not require immediately eliminating every Lucene class from the
dependency graph.

Infino cannot read Lucene index files. Reindex retained Kafka/archive data into
Infino roots for the benchmark and eventual cutover. Do not silently introduce
mixed-format query routing or permanent dual-engine support. The historical
migration and rollback policy is a separate deployment decision after the
reader/writer implementation is validated.

The known internal behavior difference is that refresh publishes durable
Infino appends. Keep that explicit in config/docs and validate its cost.
All other required semantic gaps remain replacement work, not features to drop.

## Test Plan

First build and test all modules on Java 25 with the existing Lucene engine.
Exercise distributed-query and schema fan-out success, mixed failures, timeout,
interruption, partial coverage, and cleanup after the structured-concurrency
migration. Smoke-test all updated containers and verify their runtime version
and launch flags. Include native-load/FFM tests on the supported Java 25
production/development platforms once the shim exists.

Extend existing store, searcher, OpenSearch adapter, aggregation reducer,
redaction, and `AstraTest` fixtures to execute the same inputs through Infino.
Compare normalized fields, token streams, exact hits, source, sort encodings,
unmapped/empty/null behavior, dataset scoping, and every supported aggregation.
Include the existing scripted-average and multi-chunk reduction cases.

Test FFM lifetime/error/cancellation races, close during reads, native memory
bounds, batch append failures, recovery duplicates, recursive snapshot export,
and reopen at a different path/S3 root. Prove required semantics before claiming
performance parity.

Use **Lucene on Java 25 versus Infino/FFM on Java 25** as the engine comparison.
Also retain a Lucene/Java 21 baseline to quantify the JVM upgrade separately;
do not attribute its gains or regressions to Infino. Pin the same Java 25 build
and JVM settings in the two engine runs.

Benchmark the existing Lucene reader/writer against the FFM-backed path using
identical normalized data, indexes, query mix, total process memory, CPU, and
freshness/durability targets. Measure ingestion, p50/p95/p99 queries, storage,
native RSS, IPC overhead, and snapshot/reopen time. Include local, cold/warm S3,
Lucene's existing S3 streaming, simultaneous ingestion/query, and aggregation
transfer cost. Record unsupported cases rather than excluding them from results.
No performance result or completed prototype is implied by this ADR.

## Documentation Plan

Document the Java 25 minimum version, developer/CI/container upgrade steps,
structured-concurrency migration, native-access and remaining preview flags.
Document native artifacts/toolchain, FFM flags, batching/refresh semantics,
supported query translations, known upstream gaps, snapshot format, and benchmark
commands. Update config proto comments, sample configs, and config reference
together with actual config changes. Keep this ADR's API mapping and work estimate
current as the spike resolves gaps.

## Rejected Alternatives

- **Implement Infino beneath Lucene IndexReader and keep all collectors.**
  That entails implementing terms/postings, live docs, stored fields, doc values,
  points, reader caches, and rewrite behavior. Infino's public API returns rows
  and SQL results, not those interfaces. This is a much larger compatibility
  layer than replacing KalDB's local reader/writer implementations.
- **Expose Infino through a separate worker service.** The chosen integration
  is in-process FFM; no network boundary is needed for this replacement.
- **Use BM25 top-k as the input to time sorting or aggregations.**
  It changes the result set and fails existing API semantics.
- **Rewrite coordinator/result protocols up front.** Existing partial-state
  transport/reduction can be retained where we construct equivalent states;
  change it only if a demonstrated gap warrants the additional work.

## Consequences

The reader/writer swap is feasible for a useful subset using Infino's current
public API and a small native binding. Most effort lies in preserving KalDB's
query and aggregation behavior, not in implementing storage or native calls.
Full replacement is a multi-component adapter project, with explicit upstream
risks around analysis/query features and portable snapshots.

The benchmark will establish whether that build produces better performance.
It follows a concrete reader/writer integration rather than substituting for
the API conformance work.
