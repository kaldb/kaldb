package com.slack.astra.metadata.snapshot;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraPartitionedMetadata;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Objects;

/**
 * The SnapshotMetadata class contains all the metadata related to a snapshot.
 *
 * <p>Currently, we also assume that the name of the node is the same as the snapshotId. We make
 * this distinction to allow a multiple snapshots to point to same data. However, we don't need that
 * functionality yet. So, we don't expose this flexibility to the application for now.
 *
 * <p>Note: Currently, we assume that we will always index partitions linearly. So, we only store
 * the endOffset for a Kafka partition. The starting offset would be the continuation from the
 * previous offset (except in case of a recovery task). Since this info is only used for debugging
 * for now, this should be fine. If this is inconvenient, consider adding a startOffset field also
 * here.
 */
public class SnapshotMetadata extends AstraPartitionedMetadata {
  public static final String DEFAULT_VERSION = "1";

  public enum SnapshotType {
    LIVE,
    SEALED
  }

  public enum IndexType {
    LUCENE,
    DUCKDB,
    ROCKSDB
  }

  public final String snapshotId;
  public final String snapshotPath;
  public final SnapshotType snapshotType;
  public final IndexType indexType;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final String partitionId;
  public long maxOffset;
  public long sizeInBytesOnDisk;
  public final long snapshotGeneration;
  public final String version;

  private static String normalizeSnapshotPath(
      String snapshotPath, String snapshotId, SnapshotType snapshotType) {
    if (snapshotPath != null && !snapshotPath.isBlank()) {
      return snapshotPath;
    }
    return snapshotType == SnapshotType.LIVE ? "" : snapshotId;
  }

  private static String normalizeVersion(String version) {
    if (version == null || version.isBlank()) {
      return DEFAULT_VERSION;
    }
    return version;
  }

  public SnapshotMetadata(
      String snapshotId,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId,
      long sizeInBytesOnDisk) {
    this(
        snapshotId,
        startTimeEpochMs,
        endTimeEpochMs,
        maxOffset,
        partitionId,
        sizeInBytesOnDisk,
        sizeInBytesOnDisk == 0 ? SnapshotType.LIVE : SnapshotType.SEALED,
        IndexType.LUCENE,
        sizeInBytesOnDisk == 0 ? "" : snapshotId,
        0,
        DEFAULT_VERSION);
  }

  public SnapshotMetadata(
      String snapshotId,
      long startTimeEpochMs,
      long endTimeEpochMs,
      long maxOffset,
      String partitionId,
      long sizeInBytesOnDisk,
      SnapshotType snapshotType,
      IndexType indexType,
      String snapshotPath,
      long snapshotGeneration,
      String version) {
    super(snapshotId);
    checkArgument(snapshotId != null && !snapshotId.isEmpty(), "snapshotId can't be null or empty");
    checkArgument(startTimeEpochMs > 0, "start time should be greater than zero.");
    checkArgument(endTimeEpochMs > 0, "end time should be greater than zero.");
    checkArgument(
        endTimeEpochMs >= startTimeEpochMs,
        "start time should be greater than or equal to endtime");
    checkArgument(maxOffset >= 0, "max offset should be greater than or equal to zero.");
    checkArgument(
        partitionId != null && !partitionId.isEmpty(), "partitionId can't be null or empty");
    checkArgument(snapshotGeneration >= 0, "snapshotGeneration must be greater than or equal to 0");

    this.snapshotId = snapshotId;
    this.snapshotType = Objects.requireNonNull(snapshotType, "snapshotType");
    this.indexType = Objects.requireNonNull(indexType, "indexType");
    this.snapshotPath = normalizeSnapshotPath(snapshotPath, snapshotId, this.snapshotType);
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.maxOffset = maxOffset;
    this.partitionId = partitionId;
    this.sizeInBytesOnDisk = sizeInBytesOnDisk;
    this.snapshotGeneration = snapshotGeneration;
    this.version = normalizeVersion(version);
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SnapshotMetadata that)) return false;
    if (!super.equals(o)) return false;

    return startTimeEpochMs == that.startTimeEpochMs
        && endTimeEpochMs == that.endTimeEpochMs
        && maxOffset == that.maxOffset
        && sizeInBytesOnDisk == that.sizeInBytesOnDisk
        && snapshotGeneration == that.snapshotGeneration
        && snapshotId.equals(that.snapshotId)
        && snapshotPath.equals(that.snapshotPath)
        && snapshotType == that.snapshotType
        && indexType == that.indexType
        && partitionId.equals(that.partitionId)
        && version.equals(that.version);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + snapshotId.hashCode();
    result = 31 * result + snapshotPath.hashCode();
    result = 31 * result + snapshotType.hashCode();
    result = 31 * result + indexType.hashCode();
    result = 31 * result + Long.hashCode(startTimeEpochMs);
    result = 31 * result + Long.hashCode(endTimeEpochMs);
    result = 31 * result + Long.hashCode(maxOffset);
    result = 31 * result + partitionId.hashCode();
    result = 31 * result + Long.hashCode(sizeInBytesOnDisk);
    result = 31 * result + Long.hashCode(snapshotGeneration);
    result = 31 * result + version.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "SnapshotMetadata{"
        + "snapshotId='"
        + snapshotId
        + '\''
        + ", snapshotPath='"
        + snapshotPath
        + '\''
        + ", snapshotType="
        + snapshotType
        + ", indexType="
        + indexType
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", maxOffset="
        + maxOffset
        + ", partitionId='"
        + partitionId
        + '\''
        + ", sizeInBytesOnDisk="
        + sizeInBytesOnDisk
        + ", snapshotGeneration="
        + snapshotGeneration
        + ", version='"
        + version
        + '\''
        + ", name='"
        + name
        + '\''
        + '}';
  }

  @Override
  public String getPartition() {
    if (isLive()) {
      // this keeps all the live snapshots in a single partition - this is important as their stored
      // startTimeEpochMs is not stable, and will be updated. This would cause an update to a live
      // node to fail with a partitioned metadata store as it cannot change the path of the znode.
      return "LIVE";
    } else {
      ZonedDateTime snapshotTime = Instant.ofEpochMilli(startTimeEpochMs).atZone(ZoneOffset.UTC);
      return String.format(
          "%s_%s",
          snapshotTime.getLong(ChronoField.EPOCH_DAY),
          snapshotTime.getLong(ChronoField.HOUR_OF_DAY));
    }
  }

  public boolean isLive() {
    return this.snapshotType == SnapshotType.LIVE;
  }
}
