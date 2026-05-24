package com.slack.astra.blobfs.nrt;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.blobfs.BlobStore;
import java.util.List;
import java.util.Objects;

/** Blob-store primitives for near-real-time snapshot manifest replication. */
public class NrtBlobStore {
  private static final String MANIFEST_FILE_NAME = "manifest.json";
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

  private static String manifestKey(String partitionId, String snapshotId) {
    return String.format("%s/%s", chunkPrefix(partitionId, snapshotId), MANIFEST_FILE_NAME);
  }

  /** Deserializes and validates an NRT manifest JSON payload. */
  public static NrtManifest deserializeManifest(String manifestJson) {
    checkArgument(manifestJson != null && !manifestJson.isBlank(), "manifestJson is required");
    try {
      return OBJECT_MAPPER.readValue(manifestJson, NrtManifest.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid NRT manifest JSON", e);
    }
  }

  /** Serializes a validated NRT manifest to JSON. */
  public static String serializeManifest(NrtManifest manifest) {
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

  public NrtBlobStore(BlobStore blobStore) {
    this.blobStore = Objects.requireNonNull(blobStore, "blobStore");
  }

  /** Returns the stable manifest path that SnapshotMetadata.snapshotPath should point at. */
  public String getManifestPath(String partitionId, String snapshotId) {
    return manifestKey(partitionId, snapshotId);
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

  /** Conditionally writes the stable manifest object and returns the new version token. */
  public String writeManifest(
      String snapshotPath, NrtManifest manifest, String expectedVersionToken) {
    checkArgument(snapshotPath != null && !snapshotPath.isBlank(), "snapshotPath is required");
    return blobStore.uploadDataWithVersionToken(
        snapshotPath, serializeManifest(manifest), false, expectedVersionToken);
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
      long writerEpoch,
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
      checkArgument(writerEpoch >= 0, "writerEpoch must be non-negative");
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
