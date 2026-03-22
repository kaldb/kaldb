# Enterprise Fraud Demo

This demo is built for large organizations that have centralized observability across multiple
business units after acquisitions.

## Story

A global bank is investigating a fraud campaign that crosses consumer banking, cards, and treasury
systems. The platform team needs to prove three things in one flow:

1. Teams can keep shipping data even when field types drift during integration work.
2. A failure in one KalDB component does not become a full search outage.
3. Older evidence can stay cold until investigators explicitly restore it.

## Customer-facing UI

The demo uses OpenSearch Dashboards:

- Dashboard: `http://localhost:5601/app/dashboards#/view/enterprise-fraud-investigation`
- Conflict examples: `http://localhost:5601/app/discover#/view/enterprise-fraud-conflict-examples?_g=(time:(from:'now-90d',to:'now%2B15m'))`
- Historical window: `http://localhost:5601/app/discover#/view/enterprise-fraud-historical-window?_g=(time:(from:'now-90d',to:'now%2B15m'))`

The dashboard and direct Discover links intentionally use a `now-90d` to `now+15m` time range.

- Before restore, only the `live` window is visible.
- After restore, the `historical` window appears in both the time series and the saved search.

## Quick start

```bash
bash scripts/run-enterprise-fraud-demo.sh
```

That command:

- rebuilds the local Astra image
- starts the local stack with restore-friendly rollover settings
- ingests a synthetic fraud dataset into a fresh timestamped index
- creates the Dashboards saved objects

For this demo, the cache tier is forced into the fixed-slot path by setting
`ASTRA_CACHE_JAVA_TOOL_OPTIONS=-Dastra.ng.dynamicChunkSizes=false`.

## Suggested flow

1. Open the dashboard and point out that only the current investigation window is visible.
2. Open the conflict examples saved search and show `risk_score_keyword`,
   `customer_tier_integer`, and `mfa_required_keyword` fields.
3. Simulate a cache-tier failure:

```bash
bash scripts/fail-enterprise-demo-component.sh --index <index> --down
```

4. Refresh the dashboard and show that the live fraud view still works.
5. Bring the cache tier back:

```bash
bash scripts/fail-enterprise-demo-component.sh --index <index> --up
```

6. Restore the older attack window:

```bash
bash scripts/restore-enterprise-fraud-window.sh --index <index>
```

7. Refresh the dashboard and show the historical evidence appear.

## Implementation notes

- The first partition assignment now starts at epoch `1`, which allows true historical restore
  demos instead of forcing all demo timestamps into the current day.
- The enterprise demo intentionally routes automatic replica creation into a non-searchable replica
  set, so cold snapshots stay out of the UI until the explicit restore step.
- The demo uses fixed cache slots instead of cache-node byte assignments so the restore behavior is
  stable and easier to explain live.
- The dataset is generated in two windows:
  - `historical`: older fraud evidence that rolls off hot storage
  - `live`: current investigation activity that remains searchable during the fault drill
