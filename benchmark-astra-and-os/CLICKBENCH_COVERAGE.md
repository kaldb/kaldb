# ClickBench Coverage Notes

The ClickBench benchmark harness derives its request set from
`astra/src/test/java/com/slack/astra/server/ClickBenchCompatibilityTest.java`.
This is intentional: the benchmark should time the same OpenSearch-compatible
requests that the E2E compatibility tests validate.

## Current Coverage

The local compatibility file currently defines these ClickBench cases:

```text
Q0 Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20 Q21 Q22 Q23 Q24 Q25 Q26 Q29 Q30 Q31 Q32 Q33 Q34 Q35 Q36 Q37 Q38 Q40 Q41 Q42
```

The missing ClickBench IDs are:

```text
Q27 Q28 Q39
```

The harness cannot run those three queries until they are represented in
`ClickBenchCompatibilityTest.java`.

## Missing Query Shapes

The local test labels are zero-based: local `Q0` is the first SQL statement in
the upstream file. From the upstream ClickBench SQL:
`https://github.com/ClickHouse/ClickBench/blob/main/clickhouse/queries.sql`

- Q27 groups by `CounterID`, computes `AVG(length(URL))`, applies a count filter,
  and sorts by the average URL length.
- Q28 extracts a referer domain with `REGEXP_REPLACE`, groups by that derived
  value, computes `AVG(length(Referer))`, applies a count filter, and sorts by
  the average referer length.
- Q39 groups by `TraficSourceID`, `SearchEngineID`, `AdvEngineID`, a conditional
  referer source expression, and URL destination, then applies `LIMIT 10 OFFSET
  1000`.

There is no local comment documenting why these were skipped. Treat that as a
coverage gap, not as proof that KalDB or OpenSearch cannot run them.

## Q13

Q13 is present in the compatibility test. Some local 10M-row benchmark runs
excluded it because the high-cardinality `SearchPhrase` aggregation exhausted
the available OpenSearch memory on the benchmark host.

That is a run-level exclusion. It is separate from the Q27/Q28/Q39 compatibility
coverage gap.
