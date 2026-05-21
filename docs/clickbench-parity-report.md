# ClickBench Parity Report

Snapshot of where Astra's OpenSearch-compatible search API stands against the
43 ClickBench queries at benchmark.clickhouse.com.

Numbering: everything here is **0-indexed, Q0..Q42** — the public ClickBench
list, Astra's Java tests (`q0..q42`), and `compare_opensearch.py` all agree.

## Capability inventory (state of `clickbench-compat` branch)

| Capability | Status | Evidence |
| --- | --- | --- |
| `terms` agg over keyword/integer | shipped | ClickBenchCompatibilityTest covers Q7, Q33 |
| `cardinality` (COUNT DISTINCT) | shipped | Q8, Q10 |
| `value_count`, `sum`, `avg`, `min`, `max` (numeric) | shipped | Q0, Q2, Q6 |
| Nested metric inside `terms` | shipped | Q9, Q10 |
| `multi_terms` (compound bucket key) | shipped on `clickbench-compat` | Q11 |
| Hit sort by field (asc/desc) | shipped on `clickbench-compat` | Q23, Q24, Q25, Q26 |
| Root `from` + `size` hit pagination | shipped on `clickbench-compat` | top-level hit sort + from |
| `bucket_sort` for bucket-level OFFSET | **at risk** — `from=0` truncation works; `from>0` rides the same reduce path probe A showed broken, and has no special-casing in `astra/src/main` (see Remaining risk) | Q38 fixture path |
| Painless metric `sum` script | shipped on `clickbench-compat` | q29ManySiblingResolutionWidthSums |
| `terms` with `script` source (grouping by expression) | **shipped** — probe B PASS | length / arithmetic / CASE / constant group keys |
| `script` source inside `multi_terms` | **valid in OpenSearch** (Q18/Q34/Q35/Q39 return 200, validated); **Astra comparison pending** | 2.11.1 field config accepts `field`/`missing`/`script`/`time_zone` — **no value-type-hint key exists**; a scripted source needs no type hint |
| `bucket_selector` (HAVING) | **BROKEN** — probe A FAIL | child pipeline not applied during reduce |
| Painless **regex** (`REGEXP_REPLACE`) | **not enabled** | `script.painless.regex.enabled` is never set in `AstraIndexSettings`; off/limited by default. Needed only by Q28 |
| `MIN`/`MAX` over keyword (native) | **deliberate non-goal** | OpenSearch itself rejects it; use `terms(size=1, order=_key asc)` (probe C PASS) |
| `_msearch` shape | shipped | UI/Dashboards work |

## Probe results

Three diagnostic tests added to `ClickBenchCompatibilityTest.java` on
`clickbench-compat` (not committed) and executed via
`mvn -pl astra test -Dtest=ClickBenchCompatibilityTest#probeA...+probeB...+probeC...`:

| Probe | Result | Finding | Implication |
| --- | --- | --- | --- |
| A — `bucket_selector` HAVING (terms + having_filter doc_count > 1) | **FAIL** | Request parses, returns HTTP 200, but the response still contains all 3 buckets (popular, middle, rare). Selector predicate `params.count > 1` never excludes the rare bucket. | The pipeline-tree reduction in `SearchResultAggregatorImpl` is calling `reducePipelines` but child pipeline aggregators embedded in a `terms` agg are not being applied. Fix likely lives in how the `PipelineAggregator.PipelineTree` is constructed/threaded into the parent agg during final reduce. Estimate: 3–5 days. |
| B — scripted `terms` (`script.source = doc['URL'].value.length()`) | **PASS** | Bucket keys `23` (3 docs), `22` (2 docs), `20` (1 doc) all returned. Painless compiled and ran on the search path. | Scripted grouping works end-to-end with no code changes. Unlocks queries that need grouping by an expression: Q18, Q34 (clean), Q35, Q39. Q27/Q28 still need bucket_selector. Estimate: 0 days for the engine; the only work is adding DSL for these in the Python tool. |
| C — `MIN(URL)` workaround via `terms(size=1, order=_key asc)` | **PASS** | Single bucket `"https://middle.example"` with doc_count 2 returned. Ordering by `_key` over a keyword works. | MIN/MAX over keyword reformulates as `terms(size=1)` with no engine work. Q21, Q22 reformulate cleanly. For Q27/Q28 the MIN projection needs to live as a sibling sub-agg, which is also fine via the same pattern. Estimate: 0 days engine; 0.5 day to write the DSL into the Python tool. No custom Lucene aggregator required. |

ANTLR version mismatch warnings showed up in the Painless compile path
(`ANTLR Tool version 4.11.1 ... runtime version 4.13.2`). The script still
executed, so this is not a blocker, but it's worth pinning ANTLR
consistently before relying on scripts in production.

## How the comparison harness works (and why it shapes the gaps)

`tools/clickbench/compare_opensearch.py` does **no SQL translation**. The SQL
strings in the file are human-readable labels only. The actual request is the
hand-written `clickbench_query.dsl` dict, and the **same dict** is sent to both
real OpenSearch and Astra at the `_search` endpoint. Pass/fail is byte-equality
of the normalized responses **and** both sides returning HTTP 200 — a non-200
from either side is a failure even if the bodies match, so an error pair can
never read as green.

Two consequences that drive everything below:

- **All 43 queries now carry DSL and are run** (the old `dsl=None` skip path and
  `UNSUPPORTED_REASONS` were removed). The 9 previously-skipped queries got
  best-effort, OpenSearch-idiomatic DSL; the ones needing unfinished engine work
  (`bucket_selector` HAVING, `bucket_sort` offset, Painless regex) are *expected
  to mismatch* until that work lands, rather than being hidden as "unsupported".
  These backfilled shapes are **not yet verified against live backends.**
- Any DSL must be something **real OpenSearch also accepts**, or the comparison
  diverges. This is why some apparent features are non-goals (next section).

## MIN/MAX over keyword is a deliberate non-goal

OpenSearch's `min`/`max` aggregators are registered only for `NUMERIC`,
`DATE`, and `BOOLEAN` value sources (`OpenSearchAdapter.java:289-290` calls
`MinAggregationBuilder.registerAggregators`, which does not cover
`KEYWORD`/`BYTES`); the aggregator computes a `double` and has no path for
`BytesRef`. A real OpenSearch cluster therefore **rejects**
`{"min":{"field":"URL"}}` with an error.

Because the harness compares against real OpenSearch, a custom keyword-min
aggregator would make Astra answer (HTTP 200 + value) where OpenSearch errors
(HTTP 400) → **guaranteed mismatch**. The `terms(size=1, order=_key asc)`
form (probe C) is not a hack; it is the idiomatic OpenSearch expression of
`MIN(keyword)`, and it is the only parity-correct option. `top_hits` (size 1,
sort asc) is an alternative native idiom but is not wired in Astra today
(no `TopHits` references in `astra/src/main`).

## Derived group keys (CASE, length, arithmetic) → scripted terms/multi_terms

A SQL expression in a `GROUP BY` (e.g. `length(URL)`, `ClientIP - 1`, a
`CASE WHEN ... THEN ... ELSE ... END`) is just a per-document value computed
at query time. It maps to a Painless script used as a `terms` source (single
key) or as one entry in a `multi_terms` `terms` array (compound key). Example
for Q39's CASE:

```json
{"script": {"source": "if (doc['SearchEngineID'].value == 0 && doc['AdvEngineID'].value == 0) { return doc['Referer'].value } else { return '' }"}}
```

A `multi_terms` source takes `field` *or* `script` and **no value-type hint**
(2.11 rejects `user_value_type_hint`). Probe B proved the single-key `terms`
path; the scripted `multi_terms` path now returns 200 from real OpenSearch
(Q18/Q34/Q35/Q39), with the Astra comparison still pending. Caveat: scripts
read `doc['Field']`, which needs **doc values** — keyword/numeric fields have
them, a bare `text` field does not.

## Field typing and ingest mapping

`MIN`/scripted access to a string field requires it to be `keyword`
(doc-valued), not analyzed `text`. The two sides differ:

- **Astra:** JSON strings default to `KEYWORD` (`SpanFormatter.java:213`
  fallback when no schema default maps `match_mapping_type: "string"` to
  `TEXT`). So `doc['Referer'].value` works out of the box; no action needed
  unless a dataset schema overrides strings to `text`.
- **OpenSearch:** the harness sends an **explicit** mapping
  (`compare_opensearch.py:67-92`). Fields used by gap queries are missing
  there (`Referer`, `TraficSourceID`, `RefererHash`) — without an entry a
  string dynamically maps to `text` + `.keyword`, and `doc['Referer'].value`
  fails. Add `"Referer": {"type": "keyword"}` etc., consistent with how
  `URL`/`Title`/`SearchPhrase` are already mapped.
- **ClickBench spec:** does **not** dictate OpenSearch field types (its
  canonical schema types `Referer` as ClickHouse `String`). Choosing
  `keyword` is our harness decision.

## Per-query status

Columns: **CB#** ClickBench query number (0-indexed; same in the Java tests
and the Python tool).
**Status:** `done` = passes the relevant compat test on `clickbench-compat`;
`mapped` = covered by the Python tool's DSL list but no Java assertion yet;
`gap` = currently unsupported. **Capability:** the missing feature, if any.

| CB# | SQL shape | Status | Capability needed | Notes |
| --- | --- | --- | --- | --- |
| Q0 | COUNT(*) | done | — | value_count over @timestamp |
| Q1 | COUNT(*) WHERE AdvEngineID<>0 | done | — | bool must_not term |
| Q2 | SUM, COUNT, AVG | done | — | three metric aggs |
| Q3 | AVG(UserID) | done | — | |
| Q4 | COUNT(DISTINCT UserID) | done | — | cardinality |
| Q5 | COUNT(DISTINCT SearchPhrase) | done | — | cardinality on keyword |
| Q6 | MIN/MAX(EventDate) | done | — | min/max on date is numeric-backed |
| Q7 | AdvEngineID GROUP BY ORDER BY count | done | — | terms+order |
| Q8 | RegionID, COUNT(DISTINCT UserID) ORDER BY u | done | — | terms+cardinality+order on sub-agg |
| Q9 | RegionID + sibling metrics ORDER BY c | done | — | sibling-pipeline reduction landed |
| Q10 | MobilePhoneModel ORDER BY u | done | — | terms+cardinality |
| Q11 | MobilePhone, Model ORDER BY u | done | — | multi_terms |
| Q12 | SearchPhrase ORDER BY count | done | — | terms+order |
| Q13 | SearchPhrase ORDER BY distinct user | done | — | terms+cardinality |
| Q14 | SearchEngineID, SearchPhrase ORDER BY count | done | — | multi_terms |
| Q15 | UserID GROUP BY ORDER BY count | mapped | — | terms on UserID |
| Q16 | UserID, SearchPhrase ORDER BY count | mapped | — | multi_terms |
| Q17 | UserID, SearchPhrase LIMIT 10 | mapped | — | multi_terms, no order |
| Q18 | UserID, extract(minute), SearchPhrase ORDER BY count | gap | scripted `terms` (or runtime field) | needs minute-of-day projection in grouping key |
| Q19 | UserID = literal | done | — | term lookup |
| Q20 | URL LIKE '%google%' | mapped | — | wildcard query, count via value_count |
| Q21 | MIN(URL), SearchPhrase GROUP BY ORDER BY count | gap | MIN(keyword) | Java q21 test omits MIN(URL); probe C tests workaround |
| Q22 | MIN(URL), MIN(Title), SearchPhrase with wildcard filters | gap | MIN(keyword) | Java q22 test omits MIN(URL/Title); probe C tests workaround |
| Q23 | URL LIKE '%google%' ORDER BY EventTime LIMIT 10 | done | — | hit sort by @timestamp asc |
| Q24 | SearchPhrase != '' ORDER BY EventTime LIMIT 10 | done | — | hit sort |
| Q25 | SearchPhrase != '' ORDER BY SearchPhrase LIMIT 10 | done | — | hit sort by keyword |
| Q26 | SearchPhrase != '' ORDER BY EventTime, SearchPhrase LIMIT 10 | done | — | multi-field hit sort |
| Q27 | CounterID, AVG(length(URL)), COUNT HAVING COUNT>100000 ORDER BY l DESC LIMIT 25 | gap | bucket_selector (broken) + scripted metric + order-by-submetric | gated on reduce fix (risk 1) + compound ordering (risk 6) |
| Q28 | REGEXP_REPLACE(Referer), AVG(length), COUNT, MIN(Referer) HAVING COUNT>100000 ORDER BY l DESC LIMIT 25 | gap | + Painless **regex** (not enabled) + MIN(keyword) workaround | hardest query; stacks risks 1, 3, 6 |
| Q29 | many SUM(ResolutionWidth + N) | done | — | scripted metric sum proven by q29 test |
| Q30 | SearchEngineID, ClientIP + sibling metrics ORDER BY count LIMIT 10 | mapped | — | multi_terms |
| Q31 | WatchID, ClientIP + sibling metrics WHERE SearchPhrase!='' ORDER BY count LIMIT 10 | mapped | — | multi_terms |
| Q32 | WatchID, ClientIP + sibling metrics ORDER BY count LIMIT 10 | mapped | — | multi_terms |
| Q33 | URL GROUP BY ORDER BY count LIMIT 10 | done | — | terms |
| Q34 | 1, URL GROUP BY ORDER BY count LIMIT 10 | done* | — | Java test reformulates with synthetic `ConstantOne` field; clean fix needs scripted `terms` with constant script |
| Q35 | ClientIP, ClientIP-1, ClientIP-2, ClientIP-3 GROUP BY ... ORDER BY count LIMIT 10 | gap | scripted `terms` (arithmetic group keys) | |
| Q36 | URL with page-view filters ORDER BY PV LIMIT 10 | done | — | terms with bool filter |
| Q37 | Title with page-view filters ORDER BY PV LIMIT 10 | done | — | terms with bool filter |
| Q38 | URL with refresh/link/download filters ORDER BY PV LIMIT 10 OFFSET 1000 | mapped | bucket_sort `from=1000` (at risk — risk 2) | needs `terms.size` ≥ 1010; `from>0` offset unverified |
| Q39 | TraficSourceID, SearchEngineID, AdvEngineID, CASE..., URL ORDER BY PV LIMIT 10 OFFSET 1000 | gap | scripted `multi_terms` (CASE, probeD) + bucket_sort offset (risk 2) | |
| Q40 | URLHash, EventDate ORDER BY PV LIMIT 10 OFFSET 100 | mapped | bucket_sort `from=100` (at risk — risk 2) | multi_terms + bucket_sort |
| Q41 | WindowClientWidth, Height ORDER BY PV LIMIT 10 OFFSET 10000 | mapped | bucket_sort `from=10000` (at risk — risk 2) | multi_terms + bucket_sort; deep offset → shard_size accuracy |
| Q42 | date_trunc('minute', EventTime), filters, ORDER BY minute, LIMIT 10 OFFSET 1000 | done | — | date_histogram interval=1m + bucket_sort |

## Capability map → query coverage

Grouped by what actually has to happen:

**Already free** (scripted grouping + keyword-min workaround, both
probe-proven) — needs only DSL backfill in the Python tool:
1. **Scripted `terms`** (expression/runtime group key) → Q18, Q34 (clean), Q35.
2. **`terms(size=1)` for MIN(keyword)** → Q21, Q22.
3. **Scripted `multi_terms`** (probeD to confirm) → Q39's CASE key.

**Gated on the one child-pipeline reduce fix** — `bucket_selector` and
`bucket_sort` are both child pipeline aggregators sharing the same broken
reduce path (see Remaining risk, item 1):
4. **`bucket_selector` (HAVING)** → Q27, Q28.
5. **`bucket_sort` with non-zero `from`** → Q38 (deep offset), Q39, Q40, Q41.

**Gated on a config change:**
6. **Painless regex** (`script.painless.regex.enabled`) → Q28's REGEXP_REPLACE.

## Remaining risk (confidence < 90%)

Everything not listed here is proven (shipped+tested, or probe A/B/C) or
mechanical (DSL/mapping backfill). The items below are the real risk surface,
ranked.

1. **`bucket_selector` (HAVING) child-pipeline reduction — works today: ~0%;
   fix is contained: ~50%.** Probe A proved it broken. The reduce builds the
   pipeline tree from only `aggregationBuilders.iterator().next()`
   (`SearchResultAggregatorImpl.java:62`) and applies it via `reducePipelines`
   only when `finalAggregation` is true. Child pipelines embedded in a `terms`
   agg need the tree from the full `AggregatorFactories.Builder`, applied at
   the right nesting level and **only at the final cross-chunk/cross-node
   reduce** (apply it in a partial reduce and HAVING filters buckets before
   they are fully summed → wrong answers). Risk is whether Astra's multi-stage
   reduce can do this without a refactor. Blocks Q27, Q28.

2. **`bucket_sort` with non-zero OFFSET — ~50–60%.** `bucket_sort` is *also* a
   child pipeline aggregator and has **zero special-casing** in
   `astra/src/main`, so it rides the same `reducePipelines` path as item 1.
   The "partially shipped" status is almost certainly `from=0` size-truncation
   via `terms.size`; the actual `from>0` offset likely fails for the same root
   cause. Likely **one fix with item 1**, not two. Deep offset (10000) adds a
   secondary `shard_size` accuracy risk. Blocks Q38 (deep), Q39, Q40, Q41.

3. **Painless regex for `REGEXP_REPLACE` (Q28) — ~20%.** Confirmed
   `script.painless.regex.enabled` is never set in `AstraIndexSettings`, so
   regex is off/limited by default. Probe B exercised `length()`/arithmetic,
   which don't touch the regex engine. Needs a settings change in
   `ScriptServiceProvider`/`AstraIndexSettings` plus a semantics check vs
   ClickHouse. Small but currently unwired. Blocks Q28.

4. **Scripted source inside `multi_terms`, end-to-end in Astra — ~85%.**
   Now confirmed valid in real OpenSearch — Q18/Q34/Q35/Q39 return 200 (no
   value-type hint needed). Astra's multi_terms wiring still hasn't been
   exercised with a script; running the backfilled DSL against Astra flips this
   to near-certain. Blocks Q18, Q39, clean Q34/Q35.

5. **Byte-for-byte response equality on new shapes — ~70–80% per query.** Not
   a feature; a verification tax. The harness demands exact normalized
   equality. Risks: float formatting in `AVG(length(URL))`, scripted /
   empty-string CASE bucket keys, multi_terms key-array ordering, and
   `sum_other_doc_count` / `doc_count_error_upper_bound` on large-`size`
   terms.

6. **Compound HAVING + ORDER BY derived sub-metric + LIMIT for Q27/Q28 —
   ~60%.** Even after item 1, ordering buckets by a nested metric (`l DESC`)
   *while* selector-filtering and size-limiting, all at one final reduce, is
   an unexercised combined path. Q27/Q28 stack items 1+3+6 (+min-keyword
   sibling), so they are the lowest-confidence queries overall.

**Bottom line:** the genuine engineering collapses to **one hard thing** —
child pipeline aggregators (`bucket_selector` *and* `bucket_sort`) applying
correctly in Astra's distributed reduce (items 1+2+6) — plus **one small
config item** (item 3). The rest is a quick probe (item 4) or per-query
verification (item 5). If item 1 needs reduce-staging changes rather than just
passing the right `PipelineTree`, the 1.5-week estimate is the part most
likely to slip.

## Effort estimate (revised after probes)

- **~1.5 weeks of focused work** to reach full Python-tool parity (zero
  mismatches), broken down:
  - **Child-pipeline reduction fix (covers `bucket_selector` *and*
    `bucket_sort` offset):** 4–6 days. One root cause (Remaining risk 1+2),
    fixed once, with regression tests for HAVING (Q27/Q28) and deep-offset
    (Q38–Q41) shapes. This is the slip-prone item.
  - **Enable Painless regex:** 0.5 day. Set `script.painless.regex.enabled`
    (and a sane `regex.limit-factor`) in `AstraIndexSettings`, document the
    new setting, verify `REGEXP_REPLACE` semantics for Q28.
  - **Verify the backfilled DSL:** 1 day. All 9 previously-skipped queries now
    have best-effort DSL in the tool (Q18/Q34/Q35/Q39 scripted `multi_terms`,
    Q21/Q22/Q27/Q28 `terms`+nested, Q29 the 90-way scripted sum). Run them
    against live OpenSearch+Astra and triage: the probe-proven ones (Q21, Q22,
    Q29) should match immediately; Q18/Q34/Q35 confirm scripted `multi_terms`
    end-to-end (the old `probeD`); Q27/Q28/Q39 are expected to mismatch until
    the reduce fix + regex land.
  - **Distributed parity sweep:** 1–2 days. Mirror the new shapes in
    `ClickBenchDistributedCompatibilityTest`, watching the byte-equality risks
    in Remaining risk 5.
- Prerequisite: merge of `clickbench-compat` to `kaldb-main` (out-of-scope of
  the 1.5-week estimate).

What we no longer need to do (probe-disproved hypotheses):
- ❌ Register `PainlessPlugin` in the search-side `SearchModule` — probes B
  and C show scripts already compile and run via the existing
  `ScriptServiceProvider` wiring (regex is a separate setting, not a plugin).
- ❌ Write a custom keyword min/max aggregator — probe C shows
  `terms(size=1, order=_key asc)` is a valid substitute, and a native
  keyword min/max would actively break parity (see non-goal section above).
- ❌ Add a scripted terms ValuesSource — probe B shows it's already wired, and
  the `multi_terms` script path is library-verified via `resolveUnregistered`.

## Open follow-ups (not in critical path)

- The Python tool ignores `_id`, `_index`, `_score`, `_shards`, `doc_count_error_upper_bound`, `sum_other_doc_count`, `timed_out`, `took`. Anything outside that allowlist must match byte-for-byte after normalization — beware subtle order-of-keys differences in nested aggregations.
- ClickBench has 43 queries; the Python tool and the Java tests are both Q0..Q42 = 43 entries. Verify the count matches by running both end-to-end before declaring parity.
- `track_total_hits` and accurate `hits.total` semantics are still listed as known limitations in the Dashboards-support commit body. Not currently exercised by ClickBench.
