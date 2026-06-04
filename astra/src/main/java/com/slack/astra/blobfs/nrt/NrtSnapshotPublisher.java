package com.slack.astra.blobfs.nrt;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.chunk.ReadWriteChunk;
import com.slack.astra.logstore.LogStore;
import com.slack.astra.metadata.schema.ChunkSchema;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Publishes a committed live Lucene index point as a stable NRT manifest. */
public class NrtSnapshotPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(NrtSnapshotPublisher.class);
  private static final int MANIFEST_VERSION = 1;

  private final BlobStore blobStore;
  private final NrtBlobStore nrtBlobStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final String writerNodeId;

  private static void writeSchemaFile(LogStore logStore, String snapshotId, Path indexDirectory)
      throws Exception {
    ChunkSchema chunkSchema =
        new ChunkSchema(snapshotId, logStore.getSchema(), new ConcurrentHashMap<>());
    File schemaFile = new File(indexDirectory + "/" + ReadWriteChunk.SCHEMA_FILE_NAME);
    ChunkSchema.serializeToFile(chunkSchema, schemaFile);
  }

  private static List<NrtBlobStore.FileEntry> buildFileEntries(
      String filesPath, Path indexDirectory, Iterable<String> fileNames) throws Exception {
    List<NrtBlobStore.FileEntry> files = new ArrayList<>();
    for (String fileName : fileNames) {
      files.add(buildFileEntry(filesPath, indexDirectory, fileName));
    }
    return files;
  }

  private static long calculateTotalSizeInBytes(
      NrtBlobStore.FileEntry schemaFile, List<NrtBlobStore.FileEntry> files) {
    return schemaFile.length() + files.stream().mapToLong(NrtBlobStore.FileEntry::length).sum();
  }

  private static NrtBlobStore.FileEntry buildFileEntry(
      String filesPath, Path indexDirectory, String fileName) throws Exception {
    Path filePath = indexDirectory.resolve(fileName);
    long length = Files.size(filePath);
    return new NrtBlobStore.FileEntry(
        fileName, String.format("%s/%s", filesPath, fileName), length, checksum(filePath));
  }

  private static String checksum(Path filePath) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream inputStream = Files.newInputStream(filePath)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        digest.update(buffer, 0, bytesRead);
      }
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  /** Creates a publisher for a single live snapshot writer. */
  public NrtSnapshotPublisher(
      BlobStore blobStore,
      NrtBlobStore nrtBlobStore,
      SnapshotMetadataStore snapshotMetadataStore,
      String writerNodeId) {
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
    this.nrtBlobStore = Objects.requireNonNull(nrtBlobStore, "nrtBlobStore");
    this.snapshotMetadataStore =
        Objects.requireNonNull(snapshotMetadataStore, "snapshotMetadataStore");
    checkArgument(writerNodeId != null && !writerNodeId.isBlank(), "writerNodeId is required");
    this.writerNodeId = writerNodeId;
  }

  /** Commits, uploads, writes the manifest, and updates the live snapshot pointer. */
  public SnapshotMetadata publish(
      LogStore logStore, SnapshotMetadata liveSnapshotMetadata, long startOffsetInclusive) {
    Objects.requireNonNull(logStore, "logStore");
    Objects.requireNonNull(liveSnapshotMetadata, "liveSnapshotMetadata");
    checkArgument(liveSnapshotMetadata.isLive(), "Only live snapshots can be published as NRT");
    checkArgument(startOffsetInclusive >= 0, "startOffsetInclusive must be non-negative");

    long manifestGeneration = liveSnapshotMetadata.snapshotGeneration + 1;
    String filesPath =
        NrtBlobStore.filesPath(liveSnapshotMetadata.partitionId, liveSnapshotMetadata.snapshotId);

    logStore.commit();
    logStore.refresh();

    IndexCommit indexCommit = null;
    try {
      Path indexDirectory = logStore.getDirectory().getDirectory().toAbsolutePath();
      writeSchemaFile(logStore, liveSnapshotMetadata.snapshotId, indexDirectory);

      indexCommit = logStore.getIndexCommit();
      if (indexCommit == null) {
        throw new IllegalStateException("No Lucene commit is available for NRT publish");
      }

      List<NrtBlobStore.FileEntry> files =
          buildFileEntries(filesPath, indexDirectory, indexCommit.getFileNames());
      NrtBlobStore.FileEntry schemaFile =
          buildFileEntry(filesPath, indexDirectory, ReadWriteChunk.SCHEMA_FILE_NAME);
      long sizeInBytesOnDisk = calculateTotalSizeInBytes(schemaFile, files);
      NrtBlobStore.NrtManifest manifest =
          new NrtBlobStore.NrtManifest(
              MANIFEST_VERSION,
              liveSnapshotMetadata.snapshotId,
              liveSnapshotMetadata.partitionId,
              writerNodeId,
              manifestGeneration,
              System.currentTimeMillis(),
              indexCommit.getGeneration(),
              startOffsetInclusive,
              liveSnapshotMetadata.maxOffset,
              sizeInBytesOnDisk,
              liveSnapshotMetadata.startTimeEpochMs,
              liveSnapshotMetadata.endTimeEpochMs,
              schemaFile,
              files);

      blobStore.upload(filesPath, indexDirectory);
      String snapshotPath = nrtBlobStore.writeManifest(manifest);
      SnapshotMetadata updatedSnapshotMetadata =
          new SnapshotMetadata(
              liveSnapshotMetadata.snapshotId,
              liveSnapshotMetadata.startTimeEpochMs,
              liveSnapshotMetadata.endTimeEpochMs,
              liveSnapshotMetadata.maxOffset,
              liveSnapshotMetadata.partitionId,
              manifest.sizeInBytesOnDisk(),
              liveSnapshotMetadata.snapshotType,
              liveSnapshotMetadata.indexType,
              snapshotPath,
              manifestGeneration,
              liveSnapshotMetadata.version);
      snapshotMetadataStore.updateSync(updatedSnapshotMetadata);
      LOG.info(
          "Published NRT snapshot partition={} snapshot={} generation={} path={} offset={}",
          updatedSnapshotMetadata.partitionId,
          updatedSnapshotMetadata.snapshotId,
          updatedSnapshotMetadata.snapshotGeneration,
          updatedSnapshotMetadata.snapshotPath,
          updatedSnapshotMetadata.maxOffset);
      return updatedSnapshotMetadata;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to publish NRT snapshot", e);
    } finally {
      logStore.releaseIndexCommit(indexCommit);
    }
  }
}
