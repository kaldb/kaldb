# ADR 0009: Replace Lucene With Embedded Luxir Through Java FFM

## Status

Current state: `Draft`

Date: `2026-10-28`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

KalDB uses Lucene for indexing, local search, stored document retrieval, and
immutable chunk persistence. OpenSearch supplies query translation, aggregation
collectors, and distributed aggregation reduction. KalDB supplies Kafka ingestion,
chunk rollover, metadata, distributed routing, S3 persistence, cache assignment,
and recovery.

We want to replace the local Lucene engine with Luxir while retaining KalDB's
distributed architecture and supported client behavior. Luxir's native execution
and memory management are reasons to evaluate this replacement; improvements in
throughput, latency, memory use, and storage cost must be measured on KalDB
workloads rather than assumed.

Luxir exposes HTTP and gRPC services, but a separate server is not required for
this architecture. KalDB already embeds its local engine, and Java's Foreign
Function and Memory API (FFM) provides a way to call a native library directly.
Upgrading Java is acceptable. Missing embedding, snapshot, and reduction
operations will be implemented in Luxir and tested as part of the replacement.

## Questions

- Question: How will Java call Luxir?
  Answer: Through FFM and a small C ABI around the C++ engine. Luxir's HTTP/gRPC
  server is not part of the local execution path.
- Question: Can the Java version change?
  Answer: Yes. Select the latest generally available JDK when implementation
  starts, then pin the exact toolchain and runtime versions in builds, CI, and
  deployment images. FFM was finalized in Java 22; KalDB currently targets 21.
- Question: Are missing Luxir operations architectural blockers?
  Answer: No. Implement and validate the required native operations. Unit tests
  cover individual contracts; integration and failure tests exercise FFM,
  persistence, recovery, and distributed behavior.
- Question: Does this introduce a permanent choice of storage engines?
  Answer: No. The target is a Luxir-backed KalDB. A comparison harness during
  development does not imply a permanent dual-engine production architecture.

## Public Interfaces

- Preserve KalDB's supported OpenSearch and Zipkin request/response behavior,
  including field filtering, redaction, sorting, aggregation semantics, and
  logical shard coverage accounting. This does not promise the entire OpenSearch
  API or introduce vector search as part of the replacement.
- Introduce a versioned C ABI for Java/native interoperability. This is an
  internal integration contract, not a client-facing search API.
- Replace persisted Lucene chunks with a versioned Luxir chunk format. Record
  engine/format identity, schema, commit identity, file sizes/checksums, and the
  Kafka replay boundary needed to restore a chunk safely.
- Upgrade the Java build and runtime together. Package the native library and
  dependencies for each supported OS/CPU target, with explicit native-access
  configuration and startup ABI compatibility checks.
- Initially hydrate complete chunks from S3 onto local storage. The existing
  Lucene-specific S3 range-read implementation is not reusable through FFM.
  Reject unsupported streaming configuration rather than silently ignoring it.
- Replace Lucene-specific configuration and metrics with settings and metrics
  for Luxir memory, ingestion, commits, searches, exports, and native failures.
  Document exact names and defaults alongside their implementation.

## Proposed Changes

### Engine Boundary

```text
KalDB Java: ingestion, chunks, metadata, routing, distributed reduction, S3
    -> Java FFM
    -> versioned C ABI in a shared native library
    -> Luxir C++: indexing, local queries, segments, commits, aggregate state
```

Use one shared native engine runtime per worker JVM, with independent handles
for each chunk/index. Share the scheduler and memory budget across chunks rather
than creating a thread pool per chunk. Query-only coordinators need the native
runtime only when their reduction work requires native aggregate operations.

The C ABI exposes coarse operations: initialize/shut down the runtime, create or
open a chunk, append a batch, publish a searchable commit, execute a search,
merge/finalize native aggregate state, seal/export a chunk, release an export,
and close a chunk. Sealing must not require stopping other chunks or the JVM.

Keep C++ objects, exceptions, and allocator types behind the ABI. Expose opaque
handles, fixed-width scalar types, explicit buffer lengths, and structured error
codes. Include an ABI version handshake. Catch C++ exceptions at every exported
entry point and translate them into errors that Java can classify.

Define ownership for every handle and buffer. Java owns input memory through
call completion; native work must copy it before retaining it asynchronously,
or expose an explicit completion contract that extends its lifetime. Native
result buffers have an explicit release operation, and Java must not access
them after release. Closing a chunk must reject new work and drain or cancel
outstanding work before reclaiming its resources.

Start with batched protobuf request/result buffers where existing Luxir messages
fit. This avoids network transport but still requires serialization. Introduce
typed native buffers only when profiling demonstrates a useful improvement.
Avoid per-document or per-hit callbacks across FFM.

Build a shared library with position-independent dependencies. Disable Luxir's
process-wide allocator replacement initially so embedding does not replace the
JVM process allocator. Account for Java heap, native allocations, and mapped-file
resident memory together when sizing worker containers.

### KalDB Integration

Replace the Lucene-specific capabilities exposed by
[`LogStore`](../../astra/src/main/java/com/slack/astra/logstore/LogStore.java).
Callers should work with chunk lifecycle and search operations rather than
`FSDirectory`, `IndexCommit`, or `SearcherManager` objects. Remove concrete
`LuceneIndexStoreImpl` casts from chunk construction.

Replace local Lucene search and OpenSearch collector execution with translation
from KalDB's supported query/aggregation request model into Luxir operations.
OpenSearch parsing and response formatting may remain where useful, but local
execution must not depend on Lucene readers or collectors. Remove direct Lucene
dependencies when production execution no longer needs them; assess separately
any transitive dependency retained by OpenSearch parsing libraries.

Map schema types and analyzers explicitly. Preserve dataset filters, time
boundaries, timestamp precision, keyword versus analyzed text behavior, missing
and null values, empty strings, field names, source retrieval, and supported
numeric/IP representations. Luxir defaults must not silently change these
semantics. Preserve append behavior and define replay deduplication explicitly;
Luxir's default replacement by document ID must not change ingestion semantics.

Maintain KalDB's effective sort order, including timestamp/ID tie breakers and
their comparison semantics. Each leaf returns the required `from + size` hits;
the coordinator applies the final global offset. Native errors must preserve
the distinction between invalid requests, worker failures, and missing logical
snapshot coverage.

### Commits, Snapshots, and Recovery

Implement commit pinning and file export inside Luxir rather than inspecting a
live directory from Java. An immutable segment format alone does not prevent
background merges from deleting files during upload.

The rollover protocol is:

1. Stop admitting writes to the chunk and finish accepted batches.
2. Publish a durable searchable commit and associate its Kafka replay boundary
   and schema with that generation. Acknowledge exports only for successful
   native operations; do not advance the durable recovery boundary ahead of
   the data it represents.
3. Pin the commit and obtain a manifest of all files required to reopen it.
4. Upload those files and the manifest under an immutable chunk identity.
   Publish queryable snapshot metadata only after upload validation succeeds.
5. Release the pin on success or failure. An unsuccessful export must remain
   retryable and must not publish an incomplete snapshot.

Cache and recovery workers download and validate the manifest's files, open the
chunk independently, and publish search availability only after it is usable.
Eviction closes the handle after outstanding readers finish. Reopening or
evicting one chunk must not restart the shared native runtime.

Crash recovery reopens the last durable commit and replays the required Kafka
range. Define replay boundaries and duplicate handling together so retries
neither lose records nor unintentionally multiply them. Keep live-to-persisted
snapshot handoff consistent with KalDB's existing coverage model.

### Distributed Aggregations

Replace the assumption that every leaf returns OpenSearch
`InternalAggregations` with explicit, versioned partial aggregation results.
Java retains distributed orchestration. Native aggregate implementations may
expose merge/finalize operations through the same C ABI, avoiding a second
implementation of sketch algorithms in Java.

- Merge average using sum/count, not averages of leaf averages.
- Merge bucket keys/counts before final limits and ordering. Local top buckets
  alone are insufficient for globally correct terms or multi-terms results;
  use complete candidates or a defined refinement protocol.
- Carry mergeable state for cardinality and percentiles. Preserve documented
  approximation behavior and accuracy rather than merging finalized scalars.
- Preserve nested and sibling aggregations, missing-value behavior, date/time
  zones, and supported metric ordering. Run pipeline operations at the correct
  final reduction stage.

Basic metrics and date histograms can establish the first implementation slice.
The replacement is not complete until the existing supported query and
aggregation surface has equivalent coverage. Reject unsupported operations
explicitly during development; do not silently return incomplete results.

### Implementation Sequence

1. Upgrade Java and produce a loadable Luxir shared library with the C ABI and
   lifecycle tests. Adapt existing Java preview API usage as required.
2. Prove batch ingestion, local search, commit pinning, export, and reopening
   through FFM, including failure and concurrent-close behavior.
3. Integrate chunk rollover, S3 hydration, metadata publication, and Kafka
   recovery using that native lifecycle.
4. Complete query translation, schema behavior, distributed sorting, and
   aggregation reduction against KalDB's existing compatibility tests.
5. Compare representative workloads, backfill the target deployment, validate
   coverage, and cut traffic over once correctness and operational criteria pass.

## Compatibility, Deprecation, and Migration Plan

Treat Lucene and Luxir index files as incompatible. Reindex retained data from
Kafka where retention permits, or export source documents from existing chunks
and ingest them into Luxir. Confirm a complete migration source before cutover;
Kafka alone may not contain historical data still retained in S3.

The target does not retain a Lucene reader or add mixed-engine query support.
Use a separately populated Luxir deployment for validation and cutover. Any
requirement to serve both formats within one cluster needs a separate explicit
decision. New chunk metadata and internal aggregation payloads also require
coordinated deployment; do not assume compatibility with old workers.

Keep the previous deployment and its data available for the agreed rollback
window. Rolling back a binary does not make Luxir files readable by Lucene.
Rollback requires routing to the previous deployment and replaying or otherwise
recovering writes received after cutover. Establish that replay window and source
before switching traffic. Remove old storage only after migration verification
and the rollback window are complete.

## Test Plan

Extend relevant existing test suites where the coverage belongs. Add native ABI
tests for the new library boundary and run them in CI alongside Java tests.

- Unit tests: ABI version checks, error translation, ownership/release rules,
  schema/query translation, sort comparisons, partial aggregation merging, and
  commit manifest/replay-boundary invariants.
- FFM integration tests: load the actual shared library and execute batch
  ingestion, searches, commits, export, reopen, and release. Exercise malformed
  input, repeated lifecycle operations, concurrency, cancellation, and close
  while requests are outstanding. Mocks alone do not validate the boundary.
- Persistence and failure tests: upload/download through S3Mock, reopen on a
  different worker, interrupt export and hydration, fail native operations,
  crash before/after commit and metadata publication, and replay Kafka. Assert
  complete coverage and the defined duplicate behavior.
- Compatibility tests: compare hits, stable ordering, pagination, source fields,
  dataset isolation, redaction, empty/missing/null values, and supported
  aggregations with the current KalDB implementation or an appropriate
  OpenSearch reference. Include supported Zipkin behavior.
- Distributed tests: compare multi-chunk and multi-worker results with a
  single-index reference, including skewed top terms, weighted averages,
  nested/sibling/pipeline operations, sketch accuracy, and missing shard coverage.
- Native diagnostics: run lifecycle/concurrency tests with supported sanitizers
  to detect leaks, use-after-free, and data races that Java checks cannot detect.
- Performance tests: measure ingestion, commit visibility, p50/p95/p99 search
  latency, CPU, total RSS, index size, hydration, and recovery under realistic
  concurrency. Include FFM serialization and cold-cache behavior. Agree on
  workload-specific acceptance thresholds before production cutover.
- Migration tests: verify retained data coverage and rehearse cutover and
  rollback with writes arriving during the transition.

## Documentation Plan

- Update build, local development, Docker, and CI instructions for the pinned
  JDK and native packages.
- Document the C ABI, ownership, concurrency, cancellation, and version contract
  beside its implementation.
- Update architecture, schema, query, aggregation, cache, and recovery topics.
- Update configuration reference and sample files with every proto/configuration
  change; comment all added proto fields.
- Add migration, rollback, native crash diagnosis, memory sizing, and snapshot
  validation procedures, including the initial full-hydration requirement.

## Rejected Alternatives

- **Luxir sidecar over gRPC:** an existing interface and a useful process
  isolation boundary, but it adds transport and separate process management
  when KalDB needs an embedded local engine. It also does not supply the missing
  snapshot and reduction contracts by itself.
- **JNI wrapper:** supports embedding, but FFM provides the chosen Java/native
  boundary without JNI-specific object/reference plumbing. Both still require
  an explicit wrapper around C++ ownership and lifecycle.
- **Direct binding to C++ classes:** exposes compiler-specific object layouts,
  name mangling, exceptions, and ownership across the boundary. A small C ABI
  keeps those details inside the native implementation.
- **Change only the index writer:** leaves Lucene query execution, OpenSearch
  collectors, snapshot handling, redaction readers, and S3 reads coupled to the
  old engine. The replacement must cover the complete chunk lifecycle.
- **Retain Lucene indefinitely as a fallback engine:** adds permanent execution,
  storage, migration, and test complexity. Use the previous deployment for a
  bounded migration rollback window instead.

## Consequences

KalDB keeps ownership of distribution and recovery while Luxir owns local
execution. FFM removes local network transport and permits direct native buffer
access where useful, without requiring it in the initial implementation.

KalDB also takes responsibility for a native embedding ABI, supported platform
artifacts, memory accounting, and patches to Luxir. Native crashes share the
worker JVM's failure domain. Snapshot/recovery and query compatibility remain
engineering work with explicit test coverage; neither follows automatically
from using FFM. Performance benefits remain subject to measurement.

## References

- [Luxir architecture](https://luxir.org/docs/latest/design/architecture/)
- [Luxir operations and current server limitations](https://luxir.org/docs/latest/guide/operations/)
- [Luxir build configuration](https://github.com/luxir-search/luxir/blob/main/CMakeLists.txt)
- [Luxir gRPC interface](https://luxir.org/docs/latest/guide/grpc/)
- [Java FFM native linker](https://docs.oracle.com/en/java/javase/22/docs/api/java.base/java/lang/foreign/Linker.html)
- [JDK 22 release notes: finalized FFM API](https://www.oracle.com/java/technologies/javase/22-relnote-issues.html)
- [KalDB aggregation reference](../topics/Aggregations.md)
- [Logical shard coverage accounting](../adrs/failed-shards-accounting.md)
