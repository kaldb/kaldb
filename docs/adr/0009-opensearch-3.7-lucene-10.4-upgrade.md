# ADR 0009: Upgrade OpenSearch to 3.7 and Lucene to 10.4 and Harden the Embedded Search Boundary

## Status

Current state: `Accepted`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

Astra embeds selected OpenSearch query-parsing, mapping, scripting, and
aggregation components while storing each chunk as a Lucene index. Before this
decision, the embedded libraries and local Docker environment both used
OpenSearch `2.11.1` and Lucene `9.7.0`. This ADR advances them together to
OpenSearch `3.7.0` and Lucene `10.4.0`.

This is more than a dependency-number change. Astra does not run an OpenSearch
node. `OpenSearchAdapter` constructs a small synthetic OpenSearch environment
around an Astra-owned Lucene `IndexSearcher`. Astra therefore owns the seams
that a normal OpenSearch node would own: synthetic index settings, mapper and
field-data services, query and search contexts, cache lifecycle, scripting,
stored-field access, historical codec support, and compatibility metadata.
A major OpenSearch/Lucene upgrade is the point at which all of those seams must
be revalidated together.

That review produced the following upgrade decisions:

- adapt Astra's synthetic OpenSearch environment to the new APIs while keeping
  its execution model local to one chunk;
- make field-data ownership and cleanup match the lifetimes of the JVM, chunk
  adapter, and Lucene reader instead of constructing cache infrastructure as
  part of each query setup;
- audit both Lucene stored-field access paths so the API transition cannot
  bypass field redaction, and reuse accessors at their intended scope;
- distinguish OpenSearch builder rewriting from Lucene's reader-dependent
  query rewriting so the upgraded query optimizations are tested at the correct
  boundary;
- make old-segment readability an explicit dependency as the project crosses a
  Lucene major version;
- align the Jackson 2 runtime used at the OpenSearch serialization boundary;
- remove an unused Lucene testing framework from the production dependency
  graph while resolving the new Lucene artifacts; and
- update the local OpenSearch/Dashboards images and Astra's advertised
  compatibility versions as one coherent stack.

Some weaknesses found by this audit existed on the 2.11/9.7 line. The reason to
address them in this upgrade is that accepting new upstream API and lifecycle
contracts without fixing the ownership around them would leave the upgraded
integration difficult to reason about and validate. The goal is therefore not
merely to compile against the new versions, but to establish a safe and
supportable boundary for them.

## Questions

- Question: Which versions should the embedded libraries and local stack use?
  Answer: Use OpenSearch `3.7.0` and Lucene `10.4.0`, and use OpenSearch and
  OpenSearch Dashboards `3.7.0` images in the local Docker environment.

- Question: Should Astra continue using OpenSearch's native query and
  aggregation machinery?
  Answer: Yes. Continue using OpenSearch builders, parsers, field mappings,
  collectors, and internal aggregation results. Adapt Astra's thin context to
  the new APIs rather than reimplementing OpenSearch semantics.

- Question: How much of an OpenSearch execution environment should Astra
  emulate?
  Answer: Only the contracts needed for local searches over an Astra chunk.
  The synthetic search context has no executor, reports that it is not
  cancelled, and exposes one target slice. Astra does not acquire OpenSearch
  cluster coordination or distributed-search behavior through this upgrade.

- Question: At what scopes should OpenSearch mapping and field-data objects
  live?
  Answer: Share the field-data cache and values-source registry across the JVM.
  Give each `OpenSearchAdapter` its own `IndexSettings`, `SimilarityService`,
  `MapperService`, and `IndexFieldDataService`. Keep reader-generation entries
  inside the shared cache until reader closure, adapter closure, periodic
  cleanup, or size eviction removes them.

- Question: Why does every adapter need a different synthetic index identity?
  Answer: Cache entry lookup already includes the field-data cache object and
  Lucene reader identity, so the UUID is not needed to prevent values from one
  reader being returned for another. It is needed for cleanup: OpenSearch's
  `IndexFieldDataService.close()` clears entries by index identity. Distinct
  UUIDs ensure closing one chunk adapter does not clear entries owned by other
  live adapters.

- Question: Who closes an `OpenSearchAdapter`?
  Answer: A `LogIndexSearcherImpl` owns one adapter for its chunk schema and
  closes it as part of searcher shutdown. Adapter closure closes its
  index-scoped field-data service and queues removal of that adapter's cache
  entries. Lucene reader close listeners independently queue removal of entries
  for obsolete reader generations.

- Question: How should field-data cleanup run?
  Answer: Use a JVM-owned OpenSearch thread pool to support the upgraded
  field-data APIs and run a cleaner once per minute. Cleanup is asynchronous so
  reader or chunk shutdown is not blocked by a shared-cache scan. A cleanup
  failure is logged and contained so later cleanup attempts can still run.

- Question: How should stored fields and field redaction work with Lucene 10?
  Answer: Preserve redaction through both the sequential stored-fields reader
  and `storedFields().document(...)`. Delegate `prefetch(...)` to the wrapped
  accessor. During result materialization, obtain the top-level `StoredFields`
  accessor once per search and reuse per-leaf accessors rather than rebuilding
  them for every hit.

- Question: Where should query rewriting happen?
  Answer: Keep the OpenSearch `QueryBuilder.rewrite(QueryShardContext)` step
  before conversion to a Lucene `Query`. Let `IndexSearcher.search(...)`
  perform Lucene's separate reader-dependent rewrite in production. Tests that
  assert the final optimized Lucene query class explicitly call
  `IndexSearcher.rewrite(...)` because they inspect the query before executing
  a search.

- Question: How should Astra read chunks written by Lucene 9?
  Answer: Declare `lucene-backward-codecs` directly. Historical chunk
  readability is a core storage requirement and must remain explicit even when
  OpenSearch currently brings the same module transitively.

- Question: How should Jackson be mediated at the new OpenSearch boundary?
  Answer: Align all Jackson 2 modules to OpenSearch 3.7's `2.21.3` line with the
  Jackson BOM, using `2.21` for `jackson-annotations` because that artifact now
  uses a two-component version. Keep the separately coordinated and namespaced
  Jackson 3 dependencies; Jackson 2 and 3 can coexist, while mixed Jackson 2
  minor versions at one serialization boundary are the risk being removed.

- Question: Should Astra retain `lucene-test-framework`?
  Answer: No. Astra does not use its test base classes or randomized runner,
  and the dependency was on the main compile/runtime classpath. Removing it
  during the Lucene dependency transition keeps test-only infrastructure and
  its transitive dependencies out of the deployed artifact.

- Question: What versions should Astra advertise to OpenSearch Dashboards?
  Answer: Report OpenSearch `3.7.0` and Lucene `10.4.0` from Astra's synthetic
  root and node metadata. Those values identify the compatibility target for
  the upgraded stack; they do not promise support for every OpenSearch API.

## Public Interfaces

- The local OpenSearch and OpenSearch Dashboards Docker images change from
  `2.11.1` to `3.7.0`.
- Astra's OpenSearch-compatible root and node metadata report OpenSearch
  `3.7.0` and Lucene `10.4.0`.
- Existing OpenSearch-compatible query, aggregation, and response shapes are
  preserved.
- Existing Lucene 9-era chunks remain supported inputs to Lucene 10 through
  the explicit backward-codecs dependency.
- No new configuration, persisted Astra metadata format, or client migration
  is introduced.
- `OpenSearchAdapter` gains an internal lifecycle contract: its owner must
  close it when the corresponding chunk searcher closes.

## Proposed Changes

### OpenSearch and Lucene integration boundary

Upgrade the embedded dependencies and local Docker images together. Use the
OpenSearch 3.7 `JsonXContent` parser factory, the `PainlessModulePlugin`
integration, updated mapper/query/search context constructors, and Lucene 10's
collector and stored-fields APIs.

OpenSearch 3.7 requires Astra's minimal search context to answer additional
execution questions. Astra performs a local search over one supplied Lucene
`IndexSearcher`, so the context uses no OpenSearch executor, returns `false`
from cancellation checks, and reports a target maximum slice count of one.
Field-data services receive the JVM-owned thread pool required by the upgraded
API. Sorted searches create their collector through Lucene's
`TopFieldCollectorManager` API.

These values describe Astra's real execution model. They avoid inventing
cluster capabilities merely to satisfy an upstream interface.

### Query construction and rewriting

There are two rewrite layers with different inputs and responsibilities:

1. OpenSearch rewrites a `QueryBuilder` using mappings and index settings from
   `QueryShardContext`, then converts it to a Lucene `Query`.
2. Lucene rewrites that query against the actual `IndexReader`, where it can
   choose reader- and index-sort-specific implementations.

Astra continues performing the first rewrite explicitly in
`OpenSearchAdapter.buildQuery(...)`. The normal `IndexSearcher.search(...)`
path performs the second rewrite during execution. This keeps production code
on Lucene's standard search path.

OpenSearch 3.7's `DateFieldMapper` wraps searchable date ranges in an
`ApproximateScoreQuery`. Its original query still contains the
`IndexSortSortedNumericDocValuesRangeQuery` that OpenSearch 2.11 returned
directly, while its second branch lets OpenSearch's `ContextIndexSearcher`
select an approximate point-range strategy when appropriate. Astra uses a
regular Lucene `IndexSearcher`, so rewriting the wrapper selects and rewrites
the original query. Production search already performs that rewrite as part of
`IndexSearcher.search(...)`. Tests concerned with the timestamp index-sort
optimization call `IndexSearcher.rewrite(...)` explicitly because they inspect
the query before executing a search. They then assert the same optimized
`IndexSortSortedNumericDocValuesRangeQuery` that Lucene executes instead of
treating OpenSearch's new outer wrapper as part of Astra's contract.

### Field-data cache ownership and lifecycle

Before this decision, every query/aggregation context construction created a
new `IndicesFieldDataCache`, `IndexFieldDataService`, and
`ValuesSourceRegistry`. That prevented field-data and global-ordinal reuse
between requests. OpenSearch 3.7 also requires a thread pool in this path and
warns when an `IndicesFieldDataCache` is constructed without a cluster
service. The warning made the repeated construction visible, while the API
transition required Astra to decide who owns and closes the new resources.

The upgraded design uses three nested lifetimes:

```text
JVM
├── IndicesFieldDataCache
├── ValuesSourceRegistry
├── field-data ThreadPool and periodic cleaner
└── OpenSearchAdapter (one per chunk searcher)
    ├── distinct synthetic IndexSettings / index UUID
    ├── SimilarityService
    ├── MapperService for the chunk schema
    ├── IndexFieldDataService
    └── Lucene reader generations
        └── cached field data and global ordinals
```

The JVM-wide cache supplies the sharing needed for reuse and applies
OpenSearch's default field-data cache size limit, currently 35% of heap. The
values-source registry is also immutable after construction and is shared
rather than rebuilt for each query.

Each adapter owns the services that depend on its chunk schema and synthetic
index identity. `AstraIndexSettings` therefore separates identity-free shared
service settings from an index-settings template. `create()` adds a fresh UUID
only when creating an adapter's `IndexSettings`; shared settings do not contain
an ownerless random index identity.

Cache lookup remains isolated by field-data cache object and Lucene reader
identity. Index UUIDs form a separate cleanup namespace. When the owning
`LogIndexSearcherImpl` closes, it closes its adapter, which closes the
`IndexFieldDataService` and queues an index-scoped removal. When a reader
generation closes, its close listener queues the narrower reader-scoped
removal. A JVM-level cleaner drains queued removals once per minute. Normal
size-based eviction may remove entries earlier.

Cleanup is deliberately asynchronous: closing a reader or chunk does not scan
the shared cache on the lifecycle thread. The cleaner catches and logs
failures, and the JVM shutdown hook cancels the cleaner, invalidates the cache,
and shuts down its thread pool.

This design matters on both indexer and cache nodes. Indexers query live chunks
while refreshes replace multi-segment readers, and cache nodes query loaded
historical chunks. Both can reuse field data while a reader is live, and both
need closed-reader and closed-chunk entries to leave the shared cache without
affecting other chunks.

### Stored-fields access and redaction

The Lucene 10 adaptation centers stored document retrieval on a `StoredFields`
accessor. That made both correctness and accessor lifetime part of the upgrade
review.

`RedactionLeafReader` protects both supported access paths:

- its sequential reader returns a `RedactedFieldReader`; and
- `storedFields()` returns a wrapper whose `document(...)` installs a
  `RedactionStoredFieldVisitor` and whose `prefetch(...)` delegates to Lucene's
  underlying accessor.

Covering both paths makes redaction independent of which stored-fields API a
current or future search path uses. Delegating prefetch preserves Lucene's I/O
optimization instead of accidentally suppressing it at the wrapper boundary.

`LogIndexSearcherImpl` obtains `IndexSearcher.storedFields()` once per search.
Lucene's returned accessor caches the appropriate per-leaf accessors as hits are
materialized. Reusing it matches Lucene's intended accessor scope and avoids
recreating the top-level accessor, leaf lookup, and redaction wrapper for each
hit.

### Dependency boundary

Crossing a Lucene major version makes historical codec support an explicit
storage concern. Astra directly declares `lucene-backward-codecs` `10.4.0` so
the ability to open supported Lucene 9-era chunks survives future changes to
OpenSearch's transitive dependency graph.

OpenSearch 3.7 requests Jackson 2 `2.21.3`, while Astra previously pinned its
direct Jackson 2 modules to `2.19.1`. Maven would otherwise mediate versions
artifact by artifact, allowing a mixed Jackson 2 runtime at the request and
response serialization boundary. Importing the Jackson 2 BOM aligns direct
and transitive modules. `jackson-annotations` uses `2.21`, matching that
artifact's versioning convention. Separately namespaced Jackson 3 artifacts
remain alongside Jackson 2 because they do not create the same classpath
identity problem.

The Lucene version transition also re-resolves the Lucene dependency graph.
`lucene-test-framework` is removed because no Astra source uses it and it was
declared at compile scope, bringing randomized-test and JUnit 4 infrastructure
onto the production classpath. Keeping the deployed dependency graph limited
to libraries Astra uses reduces conflict and maintenance risk at the upgraded
boundary.

### Compatibility metadata

The local OpenSearch and Dashboards containers move to `3.7.0` with the
embedded libraries. Astra's synthetic root and node responses move from
OpenSearch `2.11.1` / Lucene `9.7.0` to OpenSearch `3.7.0` / Lucene `10.4.0` so
Dashboards sees metadata consistent with the compatibility environment it is
connecting to.

The reported number is compatibility metadata, not proof that Astra is a full
OpenSearch node. The supported endpoint and query surface remains the one
implemented and tested by Astra's compatibility layer.

## Compatibility, Deprecation, and Migration Plan

No Astra metadata or request-format migration is required. Lucene 10 writes
new chunks in its current format and uses the explicit backward-codecs module
to read supported Lucene 9-era segments.

Storage compatibility is directional. The upgraded Lucene 10 runtime can read
the supported older segments, but the Lucene 9 runtime must not be assumed to
read segments written by Lucene 10. During a rolling deployment, readers must
be capable of reading Lucene 10 segments before upgraded writers produce them,
or the exact mixed-version sequence must be validated separately. After
Lucene 10 segments have been written, rolling the runtime back to Lucene 9 is
not automatically storage-safe even though Astra's own metadata schema has not
changed.

The field-data change affects allocation and cleanup, not query semantics. A
closed reader or adapter queues removal, so entries can remain until the next
one-minute cleanup pass or an earlier size-based eviction. Operators receive
reuse across requests and bounded cache retention without a new Astra setting.

The Dashboards image and advertised compatibility metadata must move together.
Rolling either one back requires restoring a mutually compatible pair and
revalidating the startup handshake.
