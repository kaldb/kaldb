package com.slack.astra.chunkManager;

import com.slack.astra.blobfs.nrt.NrtSnapshotPublisher;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import java.time.Instant;
import java.util.Objects;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class NrtPublishController<T> {
  private static final Logger LOG = LoggerFactory.getLogger(NrtPublishController.class);

  private final long nrtPublishIntervalMs;
  private OptionalLong activeNrtStartOffsetInclusive;
  private NrtSnapshotPublisher nrtSnapshotPublisher;
  private long lastNrtPublishEpochMs;
  private long lastNrtPublishedOffset;

  NrtPublishController(long nrtPublishIntervalMs) {
    this.nrtPublishIntervalMs = nrtPublishIntervalMs;
    activeNrtStartOffsetInclusive = OptionalLong.empty();
    nrtSnapshotPublisher = null;
    lastNrtPublishEpochMs = 0;
    lastNrtPublishedOffset = -1;
  }

  void enable(NrtSnapshotPublisher nrtSnapshotPublisher) {
    this.nrtSnapshotPublisher =
        Objects.requireNonNull(nrtSnapshotPublisher, "nrtSnapshotPublisher");
  }

  void onActiveChunkCreated() {
    activeNrtStartOffsetInclusive = OptionalLong.empty();
  }

  void onChunkRolledOver() {
    activeNrtStartOffsetInclusive = OptionalLong.empty();
  }

  void onMessageIndexed(ReadWriteChunk<T> currentChunk, long offset) {
    if (nrtSnapshotPublisher == null) {
      return;
    }

    if (activeNrtStartOffsetInclusive.isEmpty()) {
      activeNrtStartOffsetInclusive = OptionalLong.of(offset);
    }

    maybePublish(currentChunk);
  }

  private void maybePublish(ReadWriteChunk<T> currentChunk) {
    if (activeNrtStartOffsetInclusive.isEmpty() || currentChunk.isReadOnly()) {
      return;
    }

    long currentMaxOffset = currentChunk.info().getMaxOffset();
    if (currentMaxOffset <= lastNrtPublishedOffset) {
      return;
    }

    long nowEpochMs = Instant.now().toEpochMilli();
    if (lastNrtPublishEpochMs > 0 && nowEpochMs - lastNrtPublishEpochMs < nrtPublishIntervalMs) {
      return;
    }

    try {
      SnapshotMetadata liveSnapshotMetadata = currentChunk.getLiveSnapshotMetadata();
      SnapshotMetadata currentLiveSnapshotMetadata =
          new SnapshotMetadata(
              liveSnapshotMetadata.snapshotId,
              currentChunk.info().getDataStartTimeEpochMs(),
              currentChunk.info().getDataEndTimeEpochMs(),
              currentChunk.info().getMaxOffset(),
              currentChunk.info().getKafkaPartitionId(),
              liveSnapshotMetadata.sizeInBytesOnDisk,
              liveSnapshotMetadata.snapshotType,
              liveSnapshotMetadata.indexType,
              liveSnapshotMetadata.snapshotPath,
              liveSnapshotMetadata.snapshotGeneration,
              liveSnapshotMetadata.version);
      currentChunk.setLiveSnapshotMetadata(
          nrtSnapshotPublisher.publish(
              currentChunk.getLogStore(),
              currentLiveSnapshotMetadata,
              activeNrtStartOffsetInclusive.getAsLong()));
      lastNrtPublishEpochMs = nowEpochMs;
      lastNrtPublishedOffset = currentChunk.info().getMaxOffset();
    } catch (RuntimeException e) {
      LOG.warn("Failed to publish NRT snapshot for chunk={}", currentChunk.info(), e);
    }
  }
}
