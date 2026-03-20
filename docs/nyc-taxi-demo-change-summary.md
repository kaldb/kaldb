# NYC Taxi Demo Change Summary

This note explains the demo-related changes in the repo and what problem each one solves.

## Quick start

For a customer-facing setup, the simplest path is now:

```bash
bash scripts/run-nyc-taxi-demo.sh
```

That wrapper:

- builds the local `slackhq/astra` and `kaldb/dashboards-gateway:local` images
- starts the local stack
- recreates the indexer so the one-chunk setting is definitely active
- ingests the taxi data into a fresh timestamped index name by default
- creates the Dashboards saved objects against that index

## Current demo path

The current preferred demo path is:

1. Rewrite the ingested taxi timestamps into a synthetic recent window that fits within the current UTC day.
2. Keep the entire dataset in one live chunk on the indexer.
3. Use the normal Astra live-index search path.
4. Save the dashboard with a relative time range of `now/d` to `now+1h` so the live chunk metadata overlaps the query window.

Why this path:

- It keeps the global time picker working.
- It avoids the historical-data routing edge cases.
- It avoids the rollover/S3/cache machinery entirely for the demo.
- It removes most of the Astra-side code churn.

## Change 1: Historical partition fix

Files that were touched during an abandoned path:

- `astra/src/main/java/com/slack/astra/server/ManagerApiGrpc.java`
- `astra/src/test/java/com/slack/astra/server/ManagerApiGrpcTest.java`

What "historical partition fix" meant:

- Astra's first dataset partition normally starts at dataset-creation time.
- For true historical backfills, that means a query for old data can be filtered out before chunk selection even starts.
- The proposed fix was to start the very first partition at epoch `1` instead.

Status now:

- Removed from the worktree.
- Not needed for the current demo path because the ingested timestamps are rewritten into the recent window after dataset creation.

## Change 2: S3 custom endpoint fix

Files that were touched during an abandoned path:

- `astra/src/main/java/com/slack/astra/blobfs/S3AsyncUtil.java`
- `astra/src/main/java/com/slack/astra/blobfs/BlobStore.java`

What it was for:

- The rollover/S3/cache demo path needed Astra to upload finalized chunks to the local mock S3 service reliably.

Status now:

- Removed from the worktree.
- Not needed for the current demo path because the demo stays on the live indexer chunk and does not depend on rollover.

## Change 3: Demo compose settings

File:

- `docker-compose.yml`

Problem:

- Default rollover thresholds were too small for the taxi ingest.

Changes:

- `INDEXER_MAX_MESSAGES_PER_CHUNK=1000000`
- `INDEXER_MAX_BYTES_PER_CHUNK=1000000000`
- `INDEXER_MAX_TIME_PER_CHUNK_SECONDS=86400`
- `INDEXER_CREATE_RECOVERY_TASKS_ON_START=false`

Why `1000000`:

- The dataset has `375000` docs total.
- The goal is to keep the entire demo dataset in one live chunk and avoid rollover during the demo ingest.

Status:

- Kept.

## Change 4: Ingest script cleanup

File:

- `scripts/ingest-nyc-taxis.sh`

Problems fixed:

- The final ingest counters were wrong because the script used a pipe into `while`, which ran in a subshell.
- The compressed source had a non-NDJSON trailer line that needed to be filtered out.
- The script was too trusting about preexisting dataset state.
- The original taxi timestamps were too old for Astra's live query routing and validation behavior.

Changes:

- Uses `exec 3< <(stream_ndjson)` instead of a piped `while`, so counters stay correct.
- Filters the input through `stream_ndjson`.
- Verifies dataset partition state before reuse.
- Refuses obviously incompatible or duplicate visible data.
- Rewrites bulk action-line `_index` values so `INDEX=...` actually targets the requested dataset.
- Rewrites `@timestamp`, `pickup_datetime`, and `dropoff_datetime` into a synthetic recent window from the start of the current UTC day to the ingest time.
- Adds a short future buffer to the synthetic window so the dashboard time range overlaps the live chunk start time.
- Waits for time-filtered search to become ready after ingest.
- Uses the synthetic time window for readiness checks instead of hardcoded January 2015 dates.
- Expects the full dataset to be queryable.

Status:

- Kept.

## Short version

The current solution is intentionally simple:

1. Rewrite the taxi timestamps into today's window during ingest.
2. Keep all `375000` docs in one live chunk.
3. Use a dashboard time range of `now/d` to `now+1h`.

The earlier historical-partition and S3/cache changes explain the dead-end path we explored, but they are no longer required for the demo.
