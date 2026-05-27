# ADR 0008: Support OpenSearch Empty String Field Semantics

## Status

Current state: `Draft`

Discussion thread: `n/a`

Issue: `n/a`

PR: `n/a`

Supersedes: `n/a`

Superseded by: `n/a`

## Motivation

OpenSearch treats an empty keyword value as a real field value. It is not the
same thing as a missing field or a `null` field.

That distinction matters for ClickBench-style queries such as:

```sql
SELECT COUNT(DISTINCT SearchPhrase)
FROM hits
```

and:

```sql
SELECT SearchPhrase
FROM hits
WHERE SearchPhrase <> ''
ORDER BY SearchPhrase
LIMIT 10
```

If KalDB drops empty strings during ingest or indexing, then the query layer
cannot recover OpenSearch-compatible behavior later. Cardinality, `terms`,
`exists`, `term`, sort, and `must_not term ""` queries can all observe the
difference.

## Questions

- Question: Is an empty string a missing value?
  Answer: No. For keyword-style fields, an empty string is a concrete value.
  Missing fields and `null` values are absence; `""` is present.

- Question: Should `term: { "SearchPhrase": "" }` match documents where the
  field is present with an empty string?
  Answer: Yes, for fields indexed with a keyword-style whole-value
  representation.

- Question: Should `must_not term ""` exclude documents whose field is the empty
  string?
  Answer: Yes. This is how OpenSearch-compatible `field <> ''` filters are
  represented in the ClickBench DSL.

- Question: Should empty strings participate in aggregations?
  Answer: Yes. Empty strings should be visible to `cardinality`, `terms`,
  `value_count`, and sorting where the field type supports those operations.

- Question: Should KalDB preserve empty strings for analyzed text fields?
  Answer: This ADR targets keyword-style whole-value fields used for
  OpenSearch-compatible filtering, sorting, and aggregations. Analyzed text
  behavior can remain analyzer-dependent.

## Public Interfaces

- Ingested documents may contain string fields with value `""`.
- For keyword-style fields, `""` is indexed as a real value.
- `term` queries for `""` match documents containing the empty string.
- `exists` queries treat a keyword field containing `""` as present.
- `terms`, `multi_terms`, `cardinality`, `value_count`, and sort operations see
  `""` as a real value where the field type supports those operations.
- Missing fields and explicit `null` values remain distinct from `""`.

## Proposed Changes

### Summary

Preserve empty string values for keyword-style fields through ingestion,
schema handling, Lucene indexing, doc values, query execution, aggregation, and
response serialization. Do not normalize `""` to missing.

If a field has no predeclared schema type, an empty string value MUST infer the field as
  `KEYWORD`; it must not be treated as missing, null, unknown, or insufficient evidence for
  type inference.

### Detailed Design

The ingest and indexing path should distinguish three states:

```text
field absent
field present with null
field present with empty string ""
```

For keyword-style fields:

- absent means no indexed term and no doc-values entry;
- `null` means no indexed term and no doc-values entry
- `""` means an indexed whole-string term and a doc-values entry containing the
  empty string.

Query semantics should then follow naturally from the index:

- `term ""` matches the empty-string term;
- `must_not term ""` removes empty-string documents from the matched set;
- `exists` matches because the field has a value;
- `cardinality` counts `""` as one distinct value;
- `terms` can return an empty-string bucket;
- sort treats `""` as a real string value rather than as missing.

The implementation should avoid query-time special cases where possible. The
more robust fix is to index and store the value correctly so Lucene and
OpenSearch aggregation code see the same field state that OpenSearch would see.

### Existing Data

If existing KalDB chunks already dropped empty strings at indexing time, those
chunks cannot be made fully OpenSearch-compatible by a query-only change. The
empty value is not present in the index or doc values, so term queries and
aggregations cannot distinguish "empty string" from "missing".

## Rejected Alternatives

- Treat empty strings as missing.
  Rejected because it diverges from OpenSearch keyword-field behavior and
  breaks distinct counts and `field <> ''` filters.

- Patch only the query layer to special-case `term ""`.
  Rejected because aggregations, doc values, sorting, and `exists` would still
  see the wrong field state if indexing dropped the value.

- Require clients to avoid empty strings in indexed documents.
  Rejected because OpenSearch-compatible clients and benchmarks may legitimately
  ingest empty strings and expect standard OpenSearch semantics.

## Consequences

Benefits:

- KalDB distinguishes empty strings from missing fields.
- ClickBench queries involving `SearchPhrase` distinct counts and non-empty
  filters can match OpenSearch semantics.
- Query behavior becomes more predictable because field presence is decided at
  ingest/index time rather than patched per query.

Costs:

- Empty-string values consume index and doc-values space for fields where they
  were previously dropped.
