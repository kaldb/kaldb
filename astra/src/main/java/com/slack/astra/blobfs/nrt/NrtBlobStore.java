package com.slack.astra.blobfs.nrt;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.blobfs.BlobStore;
import java.util.List;
import java.util.Objects;

/** Blob-store primitives for near-real-time snapshot manifest replication. */
public class NrtBlobStore {
  private static final String NRT_ROOT = "nrt";
  private static final String NRT_VERSION = "v1";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final BlobStore blobStore;

  private static String chunkPrefix(String partitionId, String snapshotId) {
    validateKeyPart(partitionId, "partitionId");
    validateKeyPart(snapshotId, "snapshotId");
    return String.format(
        "%s/%s/partitions/%s/chunks/%s", NRT_ROOT, NRT_VERSION, partitionId, snapshotId);
  }

  /** Returns the blob path that stores the immutable files for one NRT snapshot. */
  public static String filesPath(String partitionId, String snapshotId) {
    return String.format("%s/files", chunkPrefix(partitionId, snapshotId));
  }

  static String manifestPath(
      String partitionId, String snapshotId, long generation, String writerNodeId) {
    checkArgument(generation > 0, "generation must be positive");
    validateKeyPart(writerNodeId, "writerNodeId");
    return String.format(
        "%s/manifests/%020d-%s.json",
        chunkPrefix(partitionId, snapshotId), generation, writerNodeId);
  }

  static NrtManifest deserializeManifest(String manifestJson) {
    checkArgument(manifestJson != null && !manifestJson.isBlank(), "manifestJson is required");
    try {
      return OBJECT_MAPPER.readValue(manifestJson, NrtManifest.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid NRT manifest JSON", e);
    }
  }

  static String serializeManifest(NrtManifest manifest) {
    Objects.requireNonNull(manifest, "manifest");
    try {
      return OBJECT_MAPPER.writeValueAsString(manifest);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to serialize NRT manifest", e);
    }
  }

  private static void validateKeyPart(String value, String fieldName) {
    checkArgument(value != null && !value.isBlank(), "%s is required", fieldName);
    checkArgument(
        !value.contains("/") && !value.equals(".") && !value.equals(".."),
        "%s must be a single path segment",
        fieldName);
  }

  private static void validateFileEntry(FileEntry fileEntry, String fieldName) {
    Objects.requireNonNull(fileEntry, fieldName);
    validateFileEntry(
        fileEntry.name(), fileEntry.key(), fileEntry.length(), fileEntry.checksum(), fieldName);
  }

  private static void validateFileEntry(
      String name, String key, long length, String checksum, String fieldName) {
    validateKeyPart(name, fieldName + ".name");
    checkArgument(key != null && !key.isBlank(), "%s.key is required", fieldName);
    checkArgument(length >= 0, "%s.length must be non-negative", fieldName);
    checkArgument(checksum != null && !checksum.isBlank(), "%s.checksum is required", fieldName);
  }

  /**
   * Creates NRT-specific blob-store operations backed by the shared Astra blob store.
   *
   * @param blobStore blob store used for manifest reads and writes
   */
  public NrtBlobStore(BlobStore blobStore) {
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
  }

  /** Reads and validates the manifest referenced by SnapshotMetadata.snapshotPath. */
  public NrtManifest readManifest(String snapshotPath) {
    checkArgument(snapshotPath != null && !snapshotPath.isBlank(), "snapshotPath is required");
    String data = blobStore.readFileData(snapshotPath, false);
    if (data == null) {
      return null;
    }
    return deserializeManifest(data);
  }

  /**
   * Writes an immutable manifest object and returns the path to commit through SnapshotMetadata
   * CAS.
   */
  public String writeManifest(NrtManifest manifest) {
    String snapshotPath =
        manifestPath(
            manifest.partitionId(),
            manifest.snapshotId(),
            manifest.manifestGeneration(),
            manifest.writerNodeId());
    blobStore.uploadData(snapshotPath, serializeManifest(manifest), false);
    return snapshotPath;
  }

  /** File metadata for one object required by an NRT manifest. */
  public record FileEntry(String name, String key, long length, String checksum) {
    public FileEntry {
      validateFileEntry(name, key, length, checksum, "file");
    }
  }

  /** Complete manifest for one Lucene commit point of a live snapshot. */
  public record NrtManifest(
      int version,
      String snapshotId,
      String partitionId,
      String writerNodeId,
      long manifestGeneration,
      long createdAtEpochMs,
      long luceneCommitGeneration,
      long startOffsetInclusive,
      long maxIndexedOffsetInclusive,
      long startTimeEpochMs,
      long endTimeEpochMs,
      FileEntry schemaFile,
      List<FileEntry> files) {
    public NrtManifest {
      checkArgument(version > 0, "version must be positive");
      validateKeyPart(snapshotId, "snapshotId");
      validateKeyPart(partitionId, "partitionId");
      checkArgument(writerNodeId != null && !writerNodeId.isBlank(), "writerNodeId is required");
      checkArgument(manifestGeneration > 0, "manifestGeneration must be positive");
      checkArgument(createdAtEpochMs > 0, "createdAtEpochMs must be positive");
      checkArgument(luceneCommitGeneration >= 0, "luceneCommitGeneration must be non-negative");
      checkArgument(startOffsetInclusive >= 0, "startOffsetInclusive must be non-negative");
      checkArgument(
          maxIndexedOffsetInclusive >= startOffsetInclusive,
          "maxIndexedOffsetInclusive must be greater than or equal to startOffsetInclusive");
      checkArgument(startTimeEpochMs > 0, "startTimeEpochMs must be positive");
      checkArgument(
          endTimeEpochMs >= startTimeEpochMs,
          "endTimeEpochMs must be greater than or equal to startTimeEpochMs");
      validateFileEntry(schemaFile, "schemaFile");
      checkArgument(files != null && !files.isEmpty(), "files is required");
      files = List.copyOf(files);
      for (FileEntry file : files) {
        validateFileEntry(file, "files");
      }
    }
  }
}
