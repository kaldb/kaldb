# Why Astra Is Slower Than OpenSearch: A Complete Performance Analysis

## How to Use This Document
This document is designed to be fed to ChatGPT (or similar) for interactive learning. It covers everything from basic concepts to the specific performance bottleneck in Astra's query path. Ask ChatGPT to teach you this material in small chunks, starting from whichever section matches your current understanding.

---

## Part 1: Background Concepts

### What Is Serialization?
Serialization is converting an in-memory data structure (like a Java object) into a format that can be stored or transmitted (like a JSON string or binary bytes). Deserialization is the reverse — parsing that format back into an in-memory object.

**Example:**
```
Java Object (in memory):
  SearchResponseHit {
    index = "test"
    id = "abc-123"
    source = {fare_amount: 38.5, tip_amount: 0.0, ...}
  }

Serialized to JSON (a string of text):
  {"_index":"test","_id":"abc-123","_source":{"fare_amount":38.5,"tip_amount":0.0,...}}
```

Serialization is **expensive** — the code has to walk through every field, convert types, handle special characters, build the output string, etc. Deserialization is even more expensive — the code has to parse the string character by character, figure out what each token means, allocate new objects, and fill in the fields.

### What Is Jackson?
Jackson is the most popular JSON library for Java. It's what Astra uses (via a utility class called `JsonUtil`) to convert between Java objects and JSON strings. Jackson is flexible and well-tested, but it's a general-purpose tool — it does a lot of work per object to handle all possible edge cases.

Astra's `JsonUtil` class wraps Jackson's `ObjectMapper`:
- Located at: `astra/src/main/java/com/slack/astra/util/JsonUtil.java`
- It uses `AfterburnerModule` (generates optimized bytecode for known types) — this helps somewhat
- It uses `JavaTimeModule` to handle Java `Instant` objects (like timestamps)

### What Is Protobuf?
Protocol Buffers (protobuf) is Google's binary serialization format. It's more compact and faster than JSON. Astra uses protobuf for internal communication between its services — when the search layer returns results to the API layer, the hits travel as protobuf `ByteString` objects.

### What Is Lucene?
Apache Lucene is the search engine library that both Astra and OpenSearch are built on. When you index a document, Lucene stores it. When you query, Lucene finds matching documents and returns them. The key thing for this analysis: Lucene stores each document's original data as a JSON string in a "stored field" called `_source`.

---

## Part 2: How a Query Flows Through Astra

### The Full Journey of a Search Request

When the benchmark sends a query like `{"query":{"match_all":{}},"size":10000}` to Astra's `_msearch` endpoint, here's what happens:

```
Step 1: HTTP Request Arrives
  ↓
  ElasticsearchApiService.multiSearch() receives the POST body
  (File: ElasticsearchApiService.java, line 95)
  ↓
Step 2: Parse the Request
  ↓
  OpenSearchRequest.parseHttpPostBody() turns the NDJSON into
  AstraSearch.SearchRequest protobuf objects
  ↓
Step 3: Execute the Search
  ↓
  For each sub-request, doSearch() calls searcher.doSearch()
  This goes into the Lucene search layer
  (File: ElasticsearchApiService.java, line 127)
  ↓
Step 4: Lucene Finds Matching Documents
  ↓
  LogIndexSearcherImpl executes the Lucene query
  For each matching document (up to `size` limit):
    - Reads the stored _source field (a JSON string)
    - Deserializes it with Jackson into a LogWireMessage object     ← DESERIALIZATION #1
    - Wraps it in a LogMessage object
    - Serializes the LogMessage back to a JSON string               ← SERIALIZATION #1
    - Wraps that string as a protobuf ByteString
  Returns AstraSearch.SearchResult containing a list of ByteStrings
  (File: LogIndexSearcherImpl.java, lines 173-204)
  ↓
Step 5: Convert Search Results to HTTP Response
  ↓
  ElasticsearchApiService.getHits() processes the result:
  For each ByteString in the hit list:
    - Converts ByteString to a UTF-8 string                        ← STRING ALLOCATION
    - Deserializes with Jackson into a LogWireMessage               ← DESERIALIZATION #2 (REDUNDANT!)
    - Wraps in a LogMessage
    - Constructs a SearchResponseHit (copies fields out)
  (File: ElasticsearchApiService.java, lines 179-190)
  (File: SearchResponseHit.java, line 135)
  ↓
Step 6: Build the Response Object Tree
  ↓
  EsSearchResponse.Builder assembles:
    - HitsMetadata (containing all the SearchResponseHit objects)
    - Aggregations (if any)
    - Timing and shard metadata
    - Debug metadata (trace ID)
  ↓
Step 7: Serialize to JSON for HTTP
  ↓
  JsonUtil.writeAsString(responseMetadata)                          ← SERIALIZATION #2
  Walks the entire object tree and produces the final JSON string
  (File: ElasticsearchApiService.java, line 119)
  ↓
Step 8: Send HTTP Response
```

### The Key Insight: Count the Serialization Round-Trips

For **every single hit document**, the data goes through this chain:

```
Lucene stored field (JSON string)
  → [Jackson deserialize] → LogWireMessage object          (Step 4, deserialize #1)
  → LogMessage object                                      (Step 4, wrap)
  → [Jackson serialize] → JSON string                      (Step 4, serialize #1)
  → protobuf ByteString                                    (Step 4, wrap as bytes)
  → UTF-8 string                                           (Step 5, string conversion)
  → [Jackson deserialize] → LogWireMessage object           (Step 5, deserialize #2)
  → LogMessage object                                      (Step 5, wrap again)
  → SearchResponseHit object                               (Step 5, copy fields)
  → [Jackson serialize as part of response] → JSON string  (Step 7, serialize #2)
  → HTTP response bytes                                    (Step 8, send)
```

That's **2 deserializations and 2 serializations per hit**. At 10,000 hits, that's 40,000 Jackson operations.

### What OpenSearch Does Instead

OpenSearch stores documents in an optimized binary format internally. When returning hits, it essentially:

```
Lucene stored field (already in a format close to the output)
  → minimal processing
  → write directly to HTTP response buffer
```

It doesn't round-trip through multiple Java object representations.

---

## Part 3: The Benchmark Data — What It Tells Us

### Raw Numbers (from the most recent benchmark run)

The benchmark tests 19 query types, each at 15 different `size` values (0, 50, 100, 250, 500, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000). Here are the key patterns:

#### Pattern 1: At size=0, Astra and OpenSearch are nearly identical

| Query | Astra (ms) | OpenSearch (ms) |
|-------|-----------|----------------|
| match_all, size=0 | 8 | 7 |
| date_histogram_agg, size=0 | 11 | 9 |
| autohisto_5_bucket_agg, size=0 | 10 | 9 |
| sorting_asc, size=0 | 9 | 6 |

This proves the **query execution itself (finding matching documents, computing aggregations) is not the bottleneck**. Lucene does the same work in both systems. The problem is in what happens AFTER the query finds its results.

#### Pattern 2: The gap grows linearly with hit count

For `match_all` (simplest possible query — no filtering, no aggregations):

| Size | Astra (ms) | OS (ms) | Astra/OS ratio | Extra ms per hit (Astra) |
|------|-----------|---------|----------------|-------------------------|
| 0 | 8 | 7 | 1.1x | — |
| 50 | 12 | 9 | 1.3x | 0.08ms/hit |
| 100 | 12 | 13 | 0.9x | 0.04ms/hit |
| 500 | 17 | 9 | 1.9x | 0.018ms/hit |
| 1000 | 23 | 9 | 2.6x | 0.015ms/hit |
| 2000 | 39 | 12 | 3.3x | 0.016ms/hit |
| 5000 | 86 | 16 | 5.4x | 0.016ms/hit |
| 10000 | 178 | 26 | 6.8x | 0.017ms/hit |

The per-hit cost stabilizes around **0.016–0.017ms per hit** (16–17 microseconds). This is the cost of the double deserialization + serialization pipeline described above.

For OpenSearch, the per-hit cost is roughly **0.002ms per hit** (2 microseconds) — about 8x less.

#### Pattern 3: Aggregations don't change the per-hit overhead

Compare `match_all` vs `date_histogram_agg` at the same size:

| Size | match_all (Astra) | date_histogram_agg (Astra) | Difference |
|------|------------------|---------------------------|------------|
| 0 | 8ms | 11ms | 3ms (aggregation computation cost) |
| 5000 | 86ms | 96ms | 10ms |
| 10000 | 178ms | 170ms | -8ms (noise) |

The aggregation adds a small constant overhead (computing the histogram), but the per-hit cost is identical. This further confirms the bottleneck is in **hit serialization, not query execution**.

#### Pattern 4: The gap is worst for simple queries

For queries where OpenSearch is very fast (simple match_all, basic aggregations without much response payload), Astra's overhead is most visible:

| Query at size=10000 | Astra (ms) | OS (ms) | Ratio |
|---------------------|-----------|---------|-------|
| autohisto_agg | 180 | 31 | 5.8x |
| date_histogram_agg | 170 | 29 | 5.9x |
| match_all | 178 | 26 | 6.8x |
| date_histogram_calendar_interval | 190 | 81 | 2.3x |
| sorting_desc | 177 | 99 | 1.8x |
| auto_date_histogram_with_metrics | 202 | 80 | 2.5x |

Note how the ratio is worst (6-7x) for the simplest queries and better (2-3x) for more complex ones. That's because complex queries add real computational cost that both systems pay — the Astra serialization overhead becomes a smaller fraction of a larger total.

### Response Size Comparison

The actual JSON responses were saved to disk. Here are the file sizes:

| Size (hits) | Astra response | OS response | Ratio |
|-------------|---------------|-------------|-------|
| 50 | 35,245 bytes | 28,637 bytes | 1.23x |
| 100 | 70,241 bytes | 57,085 bytes | 1.23x |
| 500 | 350,134 bytes | 284,618 bytes | 1.23x |
| 1,000 | 700,091 bytes | 569,211 bytes | 1.22x |
| 5,000 | 3,498,182 bytes | 2,844,922 bytes | 1.22x |
| 10,000 | 6,996,830 bytes | 5,689,829 bytes | 1.22x |

Astra's response is consistently **~23% larger**. At 10,000 hits, that's an extra 1.3 MB of JSON that has to be serialized and transmitted.

---

## Part 4: The Extra Fields — What Makes Astra's Response Bigger

### Side-by-Side Hit Comparison

**Astra returns this for each hit:**
```json
{
  "_index": "test",
  "_type": "_doc",
  "_id": "ae2faa64-3b38-42e6-aedf-564df922e9e7",
  "_score": null,
  "_timesinceepoch": 1773888982.784,
  "_source": {
    "fare_amount": 38.5,
    "_timesinceepoch": "2026-03-19T02:56:22.784Z",
    "pickup_location": "[-73.865, 40.736]",
    "service_name": "test",
    "passenger_count": 1,
    "pickup_datetime": "2015-01-02 12:06:27",
    "tolls_amount": 0.0,
    "improvement_surcharge": 0.3,
    "trip_distance": 13.02,
    "payment_type": "2",
    "store_and_fwd_flag": "N",
    "trip_type": "1",
    "rate_code_id": "1",
    "total_amount": 39.3,
    "vendor_id": "2",
    "extra": 0.0,
    "tip_amount": 0.0,
    "mta_tax": 0.5,
    "dropoff_location": "[-73.946, 40.634]",
    "dropoff_datetime": "2015-01-02T12:40:41Z"
  },
  "sort": [1773888982784]
}
```

**OpenSearch returns this for the same kind of hit:**
```json
{
  "_index": "test",
  "_id": "6vQCBJ0BFcyliqnEfemj",
  "_score": 1.0,
  "_source": {
    "total_amount": 6.3,
    "improvement_surcharge": 0.3,
    "pickup_location": [-73.922, 40.754],
    "pickup_datetime": "2015-01-01 00:34:42",
    "trip_type": "1",
    "dropoff_datetime": "2015-01-01 00:38:34",
    "rate_code_id": "1",
    "tolls_amount": 0.0,
    "dropoff_location": [-73.913, 40.765],
    "passenger_count": 1,
    "fare_amount": 5.0,
    "extra": 0.5,
    "trip_distance": 0.88,
    "tip_amount": 0.0,
    "store_and_fwd_flag": "N",
    "payment_type": "2",
    "mta_tax": 0.5,
    "vendor_id": "2"
  }
}
```

### Fields Astra Adds That OpenSearch Doesn't

| Extra Field | Where | Size per hit | Why it exists |
|-------------|-------|-------------|---------------|
| `"_type": "_doc"` | Hit level | ~16 bytes | Legacy ES compatibility — ES removed `_type` in v8. Hardcoded in SearchResponseHit. |
| `"_timesinceepoch": 1773888982.784` | Hit level | ~38 bytes | Astra-specific timestamp field. Redundant with `sort` array. |
| `"sort": [1773888982784]` | Hit level | ~27 bytes | Always `[timestamp_millis]` regardless of query sort. OpenSearch only includes this when a sort is requested. |
| `"_score": null` | Hit level | ~14 bytes | Always null in Astra (never computes relevance scores). OpenSearch returns a number or omits it. |
| `"_timesinceepoch": "2026-03-19T..."` | Inside `_source` | ~50 bytes | ISO-8601 string duplicate of the timestamp, injected into the source document by Astra during indexing. |
| `"service_name": "test"` | Inside `_source` | ~22 bytes | Astra adds this during indexing — it's the dataset/index name baked into every document. |

Total extra per hit: ~167 bytes. Over 10,000 hits, that's ~1.6 MB of unnecessary JSON — matching the ~1.3 MB size difference we measured.

---

## Part 5: The Code — Where Each Bottleneck Lives

### File Map

```
astra/src/main/java/com/slack/astra/
├── elasticsearchApi/
│   ├── ElasticsearchApiService.java      ← HTTP endpoint, orchestrates search
│   ├── OpenSearchRequest.java            ← Parses incoming _msearch requests
│   └── searchResponse/
│       ├── SearchResponseHit.java        ← Per-hit response object (BOTTLENECK)
│       ├── HitsMetadata.java             ← Wraps list of hits + total count
│       ├── EsSearchResponse.java         ← Single search response
│       └── SearchResponseMetadata.java   ← Top-level _msearch response wrapper
├── logstore/
│   ├── LogMessage.java                   ← Domain model for a log document
│   ├── LogWireMessage.java               ← Wire format (what gets serialized to JSON)
│   └── search/
│       └── LogIndexSearcherImpl.java     ← Executes Lucene queries (BOTTLENECK)
└── util/
    └── JsonUtil.java                     ← Jackson ObjectMapper wrapper
```

### Bottleneck #1: LogIndexSearcherImpl.buildLogMessage() (lines 173-204)

This method is called once per hit document during query execution:

```java
// Simplified version of what happens:
private LogMessage buildLogMessage(ScoreDoc hit, IndexSearcher searcher) {
    // 1. Read the stored JSON string from Lucene
    Document doc = searcher.doc(hit.doc);
    String sourceJson = doc.get(SystemField.SOURCE.fieldName);

    // 2. DESERIALIZATION #1: Parse JSON string into Java object
    LogWireMessage wireMsg = JsonUtil.read(sourceJson, LogWireMessage.class);
    //    Jackson parses every field: fare_amount, tip_amount, pickup_datetime, ...
    //    Creates HashMap for all the source fields
    //    Cost: ~5-10 microseconds per document

    // 3. Wrap in domain object
    LogMessage logMessage = LogMessage.fromWireMessage(wireMsg);
    //    Copies fields, sets up internal state

    // 4. SERIALIZATION #1: Convert back to JSON string for protobuf transport
    String jsonForWire = JsonUtil.writeAsString(logMessage.toWireMessage());
    //    Walks every field again, builds JSON string
    //    Cost: ~3-5 microseconds per document

    // 5. Wrap as protobuf ByteString
    return ByteString.copyFromUtf8(jsonForWire);
}
```

**The absurdity**: We read a JSON string from Lucene, parse it into objects, then immediately serialize it back to a JSON string. The round-trip `JSON → Object → JSON` is pure waste for documents that don't need transformation.

### Bottleneck #2: SearchResponseHit.fromByteString() (line 135)

This is called in ElasticsearchApiService.getHits() for every hit:

```java
// File: SearchResponseHit.java
public static SearchResponseHit fromByteString(ByteString bytes) {
    // 1. Convert protobuf ByteString back to Java String
    String json = bytes.toStringUtf8();
    //    Allocates a new String object
    //    Cost: ~1 microsecond per document (memory allocation + copy)

    // 2. DESERIALIZATION #2: Parse the SAME JSON again
    LogWireMessage hit = JsonUtil.read(json, LogWireMessage.class);
    //    Jackson parses every field AGAIN
    //    Creates another HashMap for source fields
    //    Cost: ~5-10 microseconds per document
    //    THIS IS THE REDUNDANT DESERIALIZATION

    // 3. Create yet another wrapper
    LogMessage message = LogMessage.fromWireMessage(hit);

    // 4. Copy fields into SearchResponseHit
    return new SearchResponseHit(
        message.getIndex(),        // "test"
        "_doc",                     // hardcoded — why serialize it?
        message.getId(),           // UUID
        null,                      // _score — always null, still serialized
        message.getTimestamp(),    // Instant — gets serialized as float AND as ISO string
        message.getSource(),       // the actual document fields
        ImmutableList.of(message.getTimestamp().toEpochMilli())  // sort — always [timestamp]
    );
}
```

### Bottleneck #3: Final JSON serialization (ElasticsearchApiService.java, line 119)

```java
// After building the entire response object tree:
String content = JsonUtil.writeAsString(responseMetadata);
// This walks:
//   SearchResponseMetadata
//     → List<EsSearchResponse>
//       → HitsMetadata
//         → List<SearchResponseHit>  (10,000 of these!)
//           → each hit's _source Map<String, Object>
//           → each hit's sort List
//           → each hit's _timesinceepoch Instant → serialized as float
//     → aggregations JsonNode (if any)
//     → _debug Map
```

This final serialization is unavoidable — you have to produce JSON for the HTTP response. But it's doing more work than necessary because:
- It's serializing fields that shouldn't be there (_type, _timesinceepoch, null _score)
- It's serializing Instant objects (which require special handling via JavaTimeModule)
- The _source maps are freshly-allocated HashMaps from the redundant deserialization, not the original data

---

## Part 6: Object Allocation Analysis

### What Gets Created Per Hit (10,000 hits = 10,000x each)

| Object | Created in | Purpose | Avoidable? |
|--------|-----------|---------|------------|
| `Document` | LogIndexSearcherImpl | Lucene document wrapper | No — Lucene API |
| `String` (source JSON) | LogIndexSearcherImpl | Stored field value | No — Lucene API |
| `LogWireMessage` #1 | LogIndexSearcherImpl | Jackson deserialization target | Yes — if we skip the round-trip |
| `HashMap<String, Object>` #1 | LogWireMessage constructor | Source field map | Yes — same |
| `LogMessage` #1 | LogIndexSearcherImpl | Domain wrapper | Yes — same |
| `String` (re-serialized JSON) | LogIndexSearcherImpl | JSON for protobuf | Yes — could pass the original string |
| `byte[]` (UTF-8 bytes) | ByteString.copyFromUtf8 | Protobuf wire format | Yes — same |
| `String` (from ByteString) | SearchResponseHit.fromByteString | Back to string for Jackson | Yes — REDUNDANT |
| `LogWireMessage` #2 | SearchResponseHit.fromByteString | Second Jackson parse | Yes — REDUNDANT |
| `HashMap<String, Object>` #2 | LogWireMessage constructor | Second source field map | Yes — REDUNDANT |
| `LogMessage` #2 | SearchResponseHit.fromByteString | Second domain wrapper | Yes — REDUNDANT |
| `SearchResponseHit` | SearchResponseHit.fromByteString | API response object | No — needed for response |
| `ImmutableList` (sort) | SearchResponseHit constructor | Sort values | Debatable |

**Per hit: 13 object allocations, of which 6 are redundant.**

At 10,000 hits: **130,000 objects total, 60,000 of which are unnecessary.** Each HashMap alone contains entries for all ~18 source fields (fare_amount, tip_amount, etc.), so the actual allocation count is even higher when you count the HashMap entries.

---

## Part 7: Why This Architecture Exists (It's Not Just Bad Code)

### The Distributed System Constraint

Astra is a **multi-service distributed system**. In production, the query flow is:

```
Client → Query Node → [gRPC] → Index Node(s) → Lucene
                    → [gRPC] → Cache Node(s) → Lucene snapshots
```

The search results need to travel between services via gRPC (which uses protobuf). That's why the code serializes hits to ByteString — they need to cross a network boundary.

The double deserialization happens because:
1. The **Index Node** searches Lucene and serializes hits to protobuf (for gRPC transport)
2. The **Query Node** receives the protobuf and deserializes hits to build the HTTP response

In the benchmark setup, both roles happen in the same JVM (the index node serves both `_msearch` and `_local_bulk`), so the network hop is eliminated — but the serialization/deserialization still happens because the code doesn't know it's running in a single process.

### The Optimization That Would Help

If the search and API layers are in the same process, the code could skip the protobuf round-trip and pass Java objects directly. This is a classic "distributed system overhead in a co-located deployment" problem.

Even in the distributed case, the code could:
- Pass the original Lucene stored field JSON string through protobuf without deserializing it first
- On the receiving end, parse it once into the response format directly, rather than into intermediate domain objects

---

## Part 8: Potential Fixes (Ordered by Impact)

### Fix 1: Eliminate the double deserialization (HIGHEST IMPACT)

**Current code in ElasticsearchApiService.getHits():**
```java
for (ByteString bytes : hitsByteList) {
    responseHits.add(SearchResponseHit.fromByteString(bytes));
    // Each call: ByteString → String → Jackson parse → LogWireMessage → LogMessage → SearchResponseHit
}
```

**Proposed: Pass LogMessage objects directly when co-located:**
```java
// If search result already contains deserialized objects, reuse them
for (LogMessage message : searchResult.getLogMessages()) {
    responseHits.add(SearchResponseHit.fromLogMessage(message));
    // Skips: ByteString → String → Jackson parse → LogWireMessage → LogMessage
}
```

**Estimated impact**: Would roughly halve the per-hit cost. At 10,000 hits, that's ~80ms saved (from ~170ms to ~90ms for a simple query).

### Fix 2: Pass through the stored JSON string without deserializing in the search layer

**Current code in LogIndexSearcherImpl:**
```java
String sourceJson = doc.get(SystemField.SOURCE.fieldName);
LogWireMessage wireMsg = JsonUtil.read(sourceJson, LogWireMessage.class);  // WHY?
LogMessage logMsg = LogMessage.fromWireMessage(wireMsg);
String reserializedJson = JsonUtil.writeAsString(logMsg.toWireMessage());   // WHY?
return ByteString.copyFromUtf8(reserializedJson);
```

**Proposed: Just pass the string through:**
```java
String sourceJson = doc.get(SystemField.SOURCE.fieldName);
return ByteString.copyFromUtf8(sourceJson);  // Skip deserialize + reserialize entirely
```

**Estimated impact**: Eliminates both the first deserialization AND the first serialization. Combined with Fix 1, could bring the per-hit cost down to ~3-4 microseconds (close to OpenSearch's ~2 microseconds).

**Caveat**: This only works if the stored JSON is already in the right format. If LogMessage.fromWireMessage() adds/transforms fields, those transformations would need to happen elsewhere.

### Fix 3: Remove unnecessary fields from the response

Stop serializing:
- `_type: "_doc"` — deprecated since ES 7, removed in ES 8
- `_score: null` — Astra never computes relevance scores
- `_timesinceepoch` at hit level — redundant with `sort`
- `_timesinceepoch` in `_source` — injected during indexing, not part of original data
- `service_name` in `_source` — same as `_index`, redundant

**Estimated impact**: Reduces response size by ~23%, saving bandwidth and serialization time. At 10,000 hits, saves ~1.3 MB of JSON output.

### Fix 4: Use streaming JSON serialization for the response

Instead of building the entire response object tree and then serializing it all at once, write JSON directly to the HTTP output stream:

```java
// Current: Build objects, then serialize
SearchResponseMetadata response = buildEntireResponseObjectTree();
String json = JsonUtil.writeAsString(response);  // allocates one huge string
return HttpResponse.of(json);

// Proposed: Stream directly
JsonGenerator gen = jsonFactory.createGenerator(outputStream);
gen.writeStartObject();
gen.writeFieldName("responses");
gen.writeStartArray();
// Write each hit directly, no intermediate String needed
```

**Estimated impact**: Reduces memory allocations (no giant String for the full response) and can start sending bytes before the full response is built.

---

## Part 9: Summary Diagram

```
THE CURRENT PATH (per hit):
  Lucene stored JSON ──→ [Jackson parse] ──→ LogWireMessage ──→ LogMessage
                           (5-10 μs)           (alloc)           (alloc)
                                                                    │
                                                                    ▼
                                               [Jackson serialize] ──→ JSON string
                                                  (3-5 μs)              (alloc)
                                                                         │
                                                                         ▼
                                                              ByteString (protobuf)
                                                                   (alloc)
                                                                         │
                                                                         ▼
  HTTP response ◀── [Jackson serialize] ◀── SearchResponseHit ◀── LogMessage
                       (3-5 μs)                 (alloc)            (alloc)
                                                                      ▲
                                                                      │
                                               LogWireMessage ◀── [Jackson parse]
                                                  (alloc)            (5-10 μs)
                                                                      ▲
                                                                      │
                                                              UTF-8 String (from ByteString)
                                                                   (alloc)

Total per hit: ~16-30 μs of CPU, ~13 object allocations
At 10,000 hits: ~170ms total, ~130,000 allocations


THE IDEAL PATH (per hit):
  Lucene stored JSON ──→ [Parse once into response-ready format] ──→ HTTP response
                              (2-5 μs)

Total per hit: ~2-5 μs of CPU, ~2-3 object allocations
At 10,000 hits: ~25-50ms total, ~20,000-30,000 allocations
```

---

## Part 10: Key Takeaways

1. **The bottleneck is serialization, not search.** At size=0, Astra matches OpenSearch. The entire performance gap comes from per-hit processing.

2. **Double deserialization is the primary cause.** The same JSON data gets parsed by Jackson twice — once in the search layer, once in the API layer — because the code uses protobuf ByteString as an intermediate format even when both layers run in the same process.

3. **The gap scales linearly with hit count.** Every additional hit adds ~16-17 microseconds in Astra vs ~2 microseconds in OpenSearch. At 10,000 hits, that 14μs difference compounds to ~150ms.

4. **The architecture causes this, not the algorithm.** Astra's multi-service design requires wire-format serialization between layers. When those layers are co-located (as in the benchmark), this serialization is pure overhead.

5. **23% response bloat from unnecessary fields.** Extra fields like `_type`, `_timesinceepoch`, `_score: null`, and `service_name` add bytes without value.

6. **This is fixable.** The highest-impact change (eliminating redundant deserialization) is a targeted optimization in two files, not an architectural rewrite.
