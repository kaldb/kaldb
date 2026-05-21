# ClickBench Parity: Background Tutorial

This is a from-scratch tutorial for someone who is working on the ClickBench
parity effort in this repo but hasn't built up the background. It explains the
concepts the [parity report](clickbench-parity-report.md) assumes you already
know.

Read it in order. Each section builds on the previous one. Every concept is
tied to a concrete file in this repo so you can go look at the real code.

---

## 1. What is ClickBench, and why do we care?

[ClickBench](https://benchmark.clickhouse.com/) is a public benchmark that
compares analytics databases (ClickHouse, DuckDB, Elasticsearch, etc.) on
**43 SQL queries** run against a **~100M-row web-analytics dataset** called
`hits`. Each row is a page view, with columns like `URL`, `UserID`,
`SearchPhrase`, `EventTime`, `RegionID`, `ResolutionWidth`, etc.

The 43 queries are deliberately varied:

- Simple counts: `SELECT COUNT(*) FROM hits;`
- Group-bys: `SELECT URL, COUNT(*) FROM hits GROUP BY URL ORDER BY COUNT(*) DESC LIMIT 10;`
- HAVING clauses: `... GROUP BY CounterID HAVING COUNT(*) > 100000 ...`
- Expressions in the group key: `GROUP BY length(URL)`
- Pagination with OFFSET: `... LIMIT 10 OFFSET 1000`

Why does Astra care? Astra exposes an **OpenSearch-compatible API**. People
already run ClickBench against Elasticsearch and OpenSearch. If Astra can
answer the same 43 queries — translated to OpenSearch DSL — with the same
results, then any tool that talks OpenSearch (Grafana, Dashboards,
clickbench's own harness) works out of the box. Parity is the proof.

The work tracked in this directory is **not** about being fast on ClickBench
yet. It's about being **correct**: same input, same answer. Performance comes
after correctness.

---

## 2. SQL → OpenSearch DSL: the fundamental translation

Astra doesn't speak SQL. It speaks OpenSearch **DSL** (Domain-Specific
Language), which is a JSON request body. Every ClickBench SQL query has to be
rewritten into a DSL JSON before Astra can answer it.

That rewrite tool lives at `tools/clickbench/compare_opensearch.py`. It holds
a Python dict for each of the 43 queries — the DSL — and a runner that sends
the DSL to both a real OpenSearch container and to Astra, then diffs the
responses.

Example: `SELECT COUNT(*) FROM hits;` becomes

```json
{
  "size": 0,
  "track_total_hits": true,
  "query": { "match_all": {} }
}
```

The result we look at is `hits.total.value`.

A slightly harder one: `SELECT URL, COUNT(*) c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10`
becomes

```json
{
  "size": 0,
  "aggs": {
    "by_url": {
      "terms": { "field": "URL", "size": 10, "order": { "_count": "desc" } }
    }
  }
}
```

The result we look at is `aggregations.by_url.buckets`.

Two ideas to internalize from this section:

1. **`size: 0`** means "I don't want the individual matching documents — just
   give me the aggregations." Most ClickBench queries set this.
2. **Aggregations live under `aggs`**, and each one has a name (`"by_url"`
   here) that you pick. The same name shows up in the response.

---

## 3. Queries vs aggregations: two different things in one request

Every OpenSearch request can have two sections:

- **`query`** — a filter. "Which documents are we looking at?" Returns a set
  of matching documents.
- **`aggs`** — what to compute over those matching documents. Counts, sums,
  group-bys, etc.

Think of `query` as the `WHERE` clause and `aggs` as everything after
`SELECT` and `GROUP BY` combined. A typical ClickBench query has both:

```sql
SELECT SearchPhrase, COUNT(*) c
FROM hits
WHERE SearchPhrase <> ''
GROUP BY SearchPhrase
ORDER BY c DESC LIMIT 10;
```

becomes

```json
{
  "size": 0,
  "query": {
    "bool": {
      "must_not": [ { "term": { "SearchPhrase": "" } } ]
    }
  },
  "aggs": {
    "phrases": {
      "terms": { "field": "SearchPhrase", "size": 10, "order": { "_count": "desc" } }
    }
  }
}
```

`bool` is the boolean combinator. It has `must`, `must_not`, `should`, and
`filter` arrays. Most of the WHERE clauses in ClickBench translate to one or
two `bool` clauses.

---

## 4. Aggregations: buckets vs metrics

There are two kinds of aggregations, and ClickBench needs both.

**Metric aggs** compute a number over the documents. They return a scalar.
Examples:

- `sum` — total of a numeric field
- `avg` — average
- `min`, `max` — extremes
- `value_count` — number of documents that have a value for that field
- `cardinality` — approximate `COUNT(DISTINCT field)` (uses HyperLogLog)

```json
{
  "aggs": {
    "total_users":  { "cardinality": { "field": "UserID" } },
    "avg_duration": { "avg":         { "field": "Duration" } }
  }
}
```

**Bucket aggs** split documents into groups. They return a list of buckets,
each with its own `doc_count` and (optionally) its own sub-aggregations.
Examples:

- `terms` — group by the values of a single field (SQL `GROUP BY col`)
- `multi_terms` — group by the combined values of several fields
  (SQL `GROUP BY a, b`)
- `date_histogram` — group by time intervals (e.g. one bucket per minute)
- `range` — group by numeric ranges

**Nesting** is the killer feature. A metric agg can live *inside* a bucket
agg. That gives you SQL's "aggregate within each group":

```sql
SELECT RegionID, COUNT(DISTINCT UserID) u
FROM hits GROUP BY RegionID
ORDER BY u DESC LIMIT 10;
```

```json
{
  "aggs": {
    "by_region": {
      "terms": { "field": "RegionID", "size": 10, "order": { "u": "desc" } },
      "aggs": {
        "u": { "cardinality": { "field": "UserID" } }
      }
    }
  }
}
```

Read that nested structure carefully: outer `by_region` (bucket), inner `u`
(metric). The outer `order` clause refers to the inner agg by its name. That
is how the engine knows to sort buckets by the cardinality value, not by the
default `_count`.

This is the shape Astra has supported for a while, and it covers about half
of ClickBench (Q0–Q14, Q19, Q33, Q36, Q37, Q42 in 0-indexed terms).

---

## 5. Pipeline aggregations: aggregations on top of aggregations

Some ClickBench queries can't be expressed by bucket + metric alone. Take:

```sql
... GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY ...
```

The HAVING clause filters *after* the GROUP BY runs. You need to compute the
buckets, then throw away the ones whose `doc_count` is too small. This is
what **pipeline aggregations** do: they take the output of another
aggregation and produce a derived result.

There are two flavors, and the difference matters:

### 5a. Sibling pipeline aggs

A sibling pipeline agg sits at the same level as the bucket agg it
references, and produces a *single new top-level value*. Example:
`max_bucket` finds the bucket with the highest count.

```json
{
  "aggs": {
    "by_region": { "terms": { "field": "RegionID" } },
    "biggest":   { "max_bucket": { "buckets_path": "by_region>_count" } }
  }
}
```

Sibling reduction is conceptually simple: after the bucket agg is done,
walk its buckets once and produce one value. Astra implements this — that's
what "sibling-pipeline reduction landed" means in the parity report.

### 5b. Child (parent) pipeline aggs

A child pipeline agg sits *inside* another bucket agg and either rewrites
that agg's buckets or filters them. `bucket_selector` and `bucket_sort` are
the two we care about for ClickBench:

- **`bucket_selector`** = SQL `HAVING`. It removes buckets whose values fail
  a Painless predicate.

  ```json
  {
    "aggs": {
      "by_counter": {
        "terms": { "field": "CounterID", "size": 25 },
        "aggs": {
          "having_filter": {
            "bucket_selector": {
              "buckets_path": { "c": "_count" },
              "script": "params.c > 100000"
            }
          }
        }
      }
    }
  }
  ```

- **`bucket_sort`** = SQL `ORDER BY ... OFFSET ... LIMIT ...` applied to
  buckets. (`terms.order` only sorts the top-N candidates from each shard;
  `bucket_sort` is the proper final-stage sort and supports an offset.)

The reason these are hard: they live *inside* the bucket agg, so they have
to run **after** the bucket agg has been reduced across all shards. In a
distributed search, the engine has to thread the pipeline-aggregator tree
through the reduce phase and apply each child pipeline at the right level.
This is the bug **Probe A** in the parity report is pointing at:
the predicate parses fine, but the reduce code path in
`SearchResultAggregatorImpl` doesn't apply the embedded child pipeline. The
rare bucket that should have been filtered out is still in the response.

---

## 6. Painless: the scripting language

A few ClickBench queries need expressions the DSL can't express
declaratively — for example `GROUP BY length(URL)` or
`SELECT SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), ...`. OpenSearch
ships with a sandboxed scripting language called **Painless** for exactly
this. A Painless script looks like JavaScript with Java types:

```text
doc['URL'].value.length()
```

```text
params.count > 100000
```

```text
doc['ResolutionWidth'].value + 1
```

There are three places Painless shows up in ClickBench DSL:

1. **Scripted metric** — e.g. `sum` whose value comes from a script per
   document. Astra supports this; Q29 ("many sibling resolution-width sums")
   is the proof.
2. **Scripted `terms` source** — group by the result of a script per
   document. Probe B in the report proved this works end-to-end (key `23` =
   length of `"https://popular.example"`, etc.).
3. **`bucket_selector` predicate** — the HAVING expression above. Probe A
   showed parsing/compilation works but the reduce step doesn't apply it.

The wiring is `astra/.../logstore/opensearch/ScriptServiceProvider.java` —
a singleton that holds an OpenSearch `ScriptService` with `PainlessPlugin`
already registered. `OpenSearchAdapter` passes this into the
`QueryShardContext`, which is what aggregations consult when they need to
compile a script.

---

## 7. How Astra executes a query: parse → per-shard → reduce

To know *where* the bugs live, you have to know the three stages a search
request goes through. Use the parity-report code references as anchors.

### Stage 1: Parse the JSON into typed builders

When a request comes in:

- `OpenSearchRequest.java` parses the top-level shape (queries, sorts,
  pagination).
- `SearchResultUtils.java` parses the `aggs` subtree using OpenSearch's own
  `AggregatorFactories.parseAggregators` (so the parser understands every
  built-in agg without re-implementation).

Both of those files instantiate a `SearchModule` to get the
`NamedXContentRegistry` — that registry is the table mapping JSON keys like
`"terms"`, `"bucket_selector"`, `"avg"` to the right builder class. If a
script lives inside a `bucket_selector`, the parser doesn't compile it yet —
it just records "there's a script here" in the builder tree.

### Stage 2: Per-shard execution

Each Lucene index in the cluster is a "shard". For each shard:

- `OpenSearchAdapter.java` builds a `QueryShardContext` (this is where
  `ScriptServiceProvider` gets handed in, so any script can compile).
- It runs the built aggregator tree against the Lucene index.
- It produces an `InternalAggregations` object — a typed in-memory tree of
  partial results.

These partial results come back to whichever node is coordinating.

### Stage 3: Reduce

This is the step that turns N per-shard partial results into one final
answer. It happens in `SearchResultAggregatorImpl.java`. For each kind of
agg:

- `terms` merges the top-N buckets from each shard, re-sums their counts,
  and picks the global top-N.
- Metric aggs (`sum`, `avg`, `cardinality`) combine their partial state.
- **Pipeline aggs** are applied as a final pass over the reduced tree.
  Sibling pipelines (max_bucket etc.) walk the reduced buckets and emit a
  new value. Child pipelines (bucket_selector, bucket_sort) walk *inside*
  a reduced bucket agg and rewrite its bucket list.

The Probe A finding pinpoints stage 3: when the predicate is a child of a
`terms` agg, the reducer isn't traversing into that child during
`reducePipelines`. The rare bucket survives even though
`params.count > 1` should have dropped it.

---

## 8. The hard parts of ClickBench parity (the report, decoded)

With the model above, you can read the parity report in plain English.

- **"`bucket_selector` (HAVING) — UNVERIFIED → FAIL"** means: the request is
  parsed and HTTP 200 comes back, but stage 3 doesn't apply the predicate.
  Q27 and Q28 both have a `HAVING COUNT > 100000`, so neither can be
  correct until that reducer is fixed.

- **"Scripted `terms` — PASS"** means stages 1, 2, and 3 already work when
  the script is the bucket key (because `terms` already handles a scripted
  values source out of the box once Painless is wired). So queries that need
  things like `GROUP BY length(URL)` or `GROUP BY ClientIP-1` are now
  unblocked at the engine level — the only remaining work is writing the
  DSL into the Python tool.

- **"MIN(URL) via `terms(size=1, order=_key asc)` — PASS"** is a clever
  reformulation. OpenSearch doesn't have a "min over a keyword field"
  metric agg out of the box. But `MIN(keyword)` is just "the
  lexicographically smallest value". And a `terms` agg sorted by `_key asc`
  with `size=1` returns exactly that one bucket. So Q21 and Q22 don't need
  a new aggregator — they need a DSL trick. That's a 0-day engine fix.

- **"`bucket_sort` for OFFSET"** — Q38/Q39/Q40/Q41 all need
  `LIMIT 10 OFFSET 1000` (etc.) at the bucket level. `terms.size` is the
  per-shard candidate count and isn't enough on its own; you also need a
  final-stage `bucket_sort` with the right `from`. The branch already does
  this for Q38; the others are mechanical DSL extensions.

---

## 9. Reading the Java tests

`astra/src/test/java/com/slack/astra/elasticsearchApi/ClickBenchCompatibilityTest.java`
has one `@Test` per ClickBench query (`q0`, `q1`, ..., `q42`). Each test
follows a builder DSL:

```java
ClickBenchSpec.builder()
    .expects()                              // describe expected response
    .givenRows(...)                         // fixture documents
    .whenAstraReceivesEquivalentOpenSearch( // the DSL to send
        "{ \"size\": 0, \"aggs\": { ... } }")
    .thenResponseContains(...)              // assertions
    .run();
```

Numbering is **0-indexed (Q0..Q42)** everywhere — the public ClickBench list,
these Java tests (`q0..q42`), and the Python tool's labels all line up, so a
given `Qn` means the same query in all three. When you fix something, you
usually:

1. Add (or unblock) the DSL entry in `compare_opensearch.py`.
2. Add a Java test in `ClickBenchCompatibilityTest.java` that exercises the
   same shape against an in-process Astra.
3. Run both — the Python tool against a Docker pair (Astra vs real
   OpenSearch), and the Java test against the in-process build.

---

## 10. Where to go next

- Browse `tools/clickbench/compare_opensearch.py` and find the
  `QUERIES = {...}` dict. Pick any query and trace the DSL.
- Open `ClickBenchCompatibilityTest.java`, search for `q9` and read the
  test. That's a clean nested terms+cardinality example.
- Open `SearchResultAggregatorImpl.java` and search for `reducePipelines`.
  That's the function the Probe A bug lives in.
- Re-read the parity report — every column should now make sense.

Once you can read the report end to end without looking anything up, you
have the background to actually pick up one of the open tasks.
