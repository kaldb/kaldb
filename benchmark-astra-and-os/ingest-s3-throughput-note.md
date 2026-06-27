# KalDB 10M Ingest: S3 Throughput Note

During the 10M ClickBench benchmark load, KalDB ingestion failed at chunk rollover before the benchmark queries could run.

KalDB writes incoming documents into a live Lucene chunk. With this benchmark config, `INDEXER_MAX_MESSAGES_PER_CHUNK=1000000`, so every 1M indexed rows the current chunk is closed and a new chunk starts. Closing a chunk is the rollover path.

Rollover does the following:

1. Finish writing and committing Lucene files for the closed chunk.
2. Upload those chunk files to object storage.
3. Record snapshot metadata so query/cache can treat the chunk as searchable.
4. Continue indexing new rows into the next live chunk.

For this local benchmark, object storage is MinIO running in Docker and pretending to be S3. The first failure was that the bucket `test-s3-bucket` did not exist, so the snapshot upload failed immediately. After creating the bucket, the next failure was from the AWS CRT S3 client:

```text
Http connection channel shut down due to failure to meet throughput minimum
```

KalDB treats a failed chunk snapshot as fatal because a rolled chunk that cannot be safely published would risk data loss or inconsistent query behavior. The indexer therefore halted the process, and the in-flight `_bulk` request saw an empty HTTP response.

The relevant config is `s3TargetThroughputGbps`, exposed through `S3_TARGET_THROUGHPUT_GBPS`. The default in `config/config.yaml` is `25`, which is too aggressive for local MinIO in Docker. Setting `S3_TARGET_THROUGHPUT_GBPS=1` made the local S3 client behavior less aggressive.

After lowering that setting, the first 1M-row chunk completed rollover successfully. This was verified by:

```text
Finished RW chunk snapshot to S3
```

and by confirming MinIO contained 17 uploaded objects for that closed chunk.

This is a local benchmark ingest infrastructure fix. It does not change query behavior, ClickBench compatibility, sort/pagination logic, or the benchmark result comparison itself.
