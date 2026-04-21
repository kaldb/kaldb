package com.slack.astra.server;

import static com.slack.astra.metadata.dataset.DatasetMetadataSerializer.toDatasetMetadataProto;
import static com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataSerializer.toRedactedFieldMetadataProto;
import static com.slack.astra.metadata.partition.PartitionMetadataSerializer.toPartitionMetadataProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.slack.astra.chunk.ChunkInfo;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataSerializer;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataSerializer;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.metadata.partition.PartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.server.partitionassignment.InvalidPartitionAssignmentStateException;
import com.slack.astra.server.partitionassignment.LivePartitionState;
import com.slack.astra.server.partitionassignment.PartitionAssignmentService;
import com.slack.astra.server.partitionassignment.PartitionAssignmentService.DedicatedPartitionModeOverride;
import com.slack.astra.server.partitionassignment.PartitionOccupancy;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.SizeLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Administration API for managing dataset configurations, including throughput and partition
 * assignments. This API is available only on the cluster manager service, and the data created is
 * consumed primarily by the pre-processor and query services. Mutating RPCs are synchronized to
 * serialize dataset and partition-assignment changes within the singleton manager process.
 */
public class ManagerApiGrpc extends ManagerApiServiceGrpc.ManagerApiServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(ManagerApiGrpc.class);
  private final DatasetMetadataStore datasetMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;

  /** Open-ended upper bound for manager query windows and snapshot scans. */
  public static final long MAX_TIME = ChunkInfo.MAX_FUTURE_TIME;

  private final ReplicaRestoreService replicaRestoreService;
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;
  private final PartitionMetadataStore partitionMetadataStore;
  private final LivePartitionStateLoader livePartitionStateLoader;
  private final PartitionAssignmentUpdater partitionAssignmentUpdater;

  public ManagerApiGrpc(
      DatasetMetadataStore datasetMetadataStore,
      PartitionMetadataStore partitionMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      ReplicaRestoreService replicaRestoreService,
      FieldRedactionMetadataStore fieldRedactionMetadataStore,
      int minNumberOfPartitions) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicaRestoreService = replicaRestoreService;
    this.fieldRedactionMetadataStore = fieldRedactionMetadataStore;
    this.partitionMetadataStore =
        Objects.requireNonNull(partitionMetadataStore, "partitionMetadataStore");
    this.partitionAssignmentService =
        new PartitionAssignmentService(
            datasetMetadataStore, this.partitionMetadataStore, minNumberOfPartitions);
  }

  /** Initializes a new dataset in the metadata store with no initial allocated capacity */
  @Override
  public synchronized void createDatasetMetadata(
      ManagerApi.CreateDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      datasetMetadataStore.createSync(
          new DatasetMetadata(
              request.getName(),
              request.getOwner(),
              0L,
              Collections.emptyList(),
              request.getServiceNamePattern(),
              false));
      responseObserver.onNext(
          toDatasetMetadataProto(datasetMetadataStore.getSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error creating new dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Updates an existing dataset with new metadata */
  @Override
  public synchronized void updateDatasetMetadata(
      ManagerApi.UpdateDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      DatasetMetadata existingDatasetMetadata = datasetMetadataStore.getSync(request.getName());

      DatasetMetadata updatedDatasetMetadata =
          new DatasetMetadata(
              existingDatasetMetadata.getName(),
              request.getOwner(),
              existingDatasetMetadata.getThroughputBytes(),
              existingDatasetMetadata.getPartitionConfigs(),
              request.getServiceNamePattern(),
              existingDatasetMetadata.isUsingDedicatedPartitions());
      datasetMetadataStore.updateSync(updatedDatasetMetadata);
      responseObserver.onNext(toDatasetMetadataProto(updatedDatasetMetadata));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error updating existing dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns a single dataset metadata by name */
  @Override
  public void getDatasetMetadata(
      ManagerApi.GetDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      responseObserver.onNext(
          toDatasetMetadataProto(datasetMetadataStore.getSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Deletes an existing dataset by name, rejecting if snapshots still reference its partitions */
  @Override
  public synchronized void deleteDatasetMetadata(
      ManagerApi.DeleteDatasetMetadataRequest request,
      StreamObserver<Metadata.DatasetMetadata> responseObserver) {

    try {
      if (!datasetMetadataStore.hasSync(request.getName())) {
        LOG.warn("Dataset not found during delete: {}", request.getName());
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("Dataset not found: " + request.getName())
                .asException());
        return;
      }
      DatasetMetadata datasetToDelete = datasetMetadataStore.getSync(request.getName());

      List<SnapshotMetadata> snapshotsForDataset =
          calculateRequiredSnapshots(
              snapshotMetadataStore.listSync(),
              datasetMetadataStore,
              0L,
              MAX_TIME,
              request.getName());
      if (!snapshotsForDataset.isEmpty()) {
        responseObserver.onError(
            Status.FAILED_PRECONDITION
                .withDescription(
                    String.format(
                        "Cannot delete dataset '%s': %d snapshot(s) still reference its partitions. "
                            + "Clean up snapshots first before deleting the dataset.",
                        request.getName(), snapshotsForDataset.size()))
                .asException());
        return;
      }

      datasetMetadataStore.deleteSync(request.getName());
      responseObserver.onNext(toDatasetMetadataProto(datasetToDelete));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error deleting dataset", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns all available datasets from the metadata store */
  @Override
  public void listDatasetMetadata(
      ManagerApi.ListDatasetMetadataRequest request,
      StreamObserver<ManagerApi.ListDatasetMetadataResponse> responseObserver) {
    // todo - consider adding search/pagination support
    try {
      responseObserver.onNext(
          ManagerApi.ListDatasetMetadataResponse.newBuilder()
              .addAllDatasetMetadata(
                  datasetMetadataStore.listSync().stream()
                      .map(DatasetMetadataSerializer::toDatasetMetadataProto)
                      .collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting datasets.", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Allocates a new partition assignment for a dataset. If partition IDs are provided, it uses
   * those IDs as the current allocation. If no partition IDs are provided, it auto-assigns from the
   * partition catalog using the requested throughput and dedication requirement.
   */
  @Override
  public synchronized void updatePartitionAssignment(
      ManagerApi.UpdatePartitionAssignmentRequest request,
      StreamObserver<ManagerApi.UpdatePartitionAssignmentResponse> responseObserver) {

    try {
      List<String> assignedPartitionIds =
          partitionAssignmentService.updateAssignment(
              request.getName(),
              request.getThroughputBytes(),
              request.getPartitionIdsList(),
              toDedicatedPartitionModeOverride(request));

      responseObserver.onNext(
          ManagerApi.UpdatePartitionAssignmentResponse.newBuilder()
              .addAllAssignedPartitionIds(assignedPartitionIds)
              .build());
      responseObserver.onCompleted();
    } catch (InvalidPartitionAssignmentStateException e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
    } catch (IllegalArgumentException e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
    } catch (StatusRuntimeException e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Error updating partition assignment", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void restoreReplica(
      ManagerApi.RestoreReplicaRequest request,
      StreamObserver<ManagerApi.RestoreReplicaResponse> responseObserver) {
    try {
      Preconditions.checkArgument(
          request.getStartTimeEpochMs() < request.getEndTimeEpochMs(),
          "Start time must not be after end time");
      Preconditions.checkArgument(
          !request.getServiceName().isEmpty(), "Service name must not be empty");

      List<SnapshotMetadata> snapshotsToRestore =
          calculateRequiredSnapshots(
              snapshotMetadataStore.listSync(),
              datasetMetadataStore,
              request.getStartTimeEpochMs(),
              request.getEndTimeEpochMs(),
              request.getServiceName());

      replicaRestoreService.queueSnapshotsForRestoration(snapshotsToRestore);

      responseObserver.onNext(
          ManagerApi.RestoreReplicaResponse.newBuilder().setStatus("success").build());
      responseObserver.onCompleted();
    } catch (SizeLimitExceededException e) {
      LOG.error(
          "Error handling request: number of replicas requested exceeds maxReplicasPerRequest limit",
          e);
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()).asException());
    } catch (Exception e) {
      LOG.error("Error handling request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void restoreReplicaIds(
      ManagerApi.RestoreReplicaIdsRequest request,
      StreamObserver<ManagerApi.RestoreReplicaIdsResponse> responseObserver) {
    try {
      List<SnapshotMetadata> snapshotsToRestore =
          calculateRequiredSnapshots(
              request.getIdsToRestoreList(), snapshotMetadataStore.listSync());

      replicaRestoreService.queueSnapshotsForRestoration(snapshotsToRestore);

      responseObserver.onNext(
          ManagerApi.RestoreReplicaIdsResponse.newBuilder().setStatus("success").build());
      responseObserver.onCompleted();
    } catch (SizeLimitExceededException e) {
      LOG.error(
          "Error handling request: number of replicas requested exceeds maxReplicasPerRequest limit",
          e);
      responseObserver.onError(
          Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()).asException());
    } catch (Exception e) {
      LOG.error("Error handling request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /**
   * Determines all SnapshotMetadata between startTimeEpochMs and endTimeEpochMs that contain data
   * from the queried service
   *
   * @return List of SnapshotMetadata that are within specified timeframe and from queried service
   */
  protected static List<SnapshotMetadata> calculateRequiredSnapshots(
      List<SnapshotMetadata> snapshotMetadataList,
      DatasetMetadataStore datasetMetadataStore,
      long startTimeEpochMs,
      long endTimeEpochMs,
      String datasetName) {
    // FIXME: This over-includes snapshots when partition IDs are reassigned across datasets over
    // time. We currently flatten to partition IDs only; this should be replaced with a
    // partition-ownership-window-aware match in a follow-up PR.
    Set<String> partitionIdsWithQueriedData = new HashSet<>();
    List<DatasetPartitionMetadata> partitionMetadataList =
        DatasetPartitionMetadata.findPartitionsToQuery(
            datasetMetadataStore, startTimeEpochMs, endTimeEpochMs, datasetName);

    // flatten all partition ids into one list
    for (DatasetPartitionMetadata datasetPartitionMetadata : partitionMetadataList) {
      partitionIdsWithQueriedData.addAll(datasetPartitionMetadata.partitions);
    }

    List<SnapshotMetadata> snapshotMetadata = new ArrayList<>();

    for (SnapshotMetadata snapshot : snapshotMetadataList) {
      if (snapshotContainsRequestedDataAndIsWithinTimeframe(
          startTimeEpochMs, endTimeEpochMs, partitionIdsWithQueriedData, snapshot)) {
        snapshotMetadata.add(snapshot);
      }
    }

    return snapshotMetadata;
  }

  /**
   * Determines all SnapshotMetadata that match the IDs in snapshotIds
   *
   * @return List of SnapshotMetadata that are within specified timeframe and from queried service
   */
  protected static List<SnapshotMetadata> calculateRequiredSnapshots(
      List<String> snapshotIds, List<SnapshotMetadata> snapshotMetadataList) {
    Set<String> matchingSnapshots =
        Sets.intersection(
            Sets.newHashSet(snapshotIds),
            Sets.newHashSet(
                snapshotMetadataList.stream()
                    .map((snapshot) -> snapshot.snapshotId)
                    .collect(Collectors.toList())));

    return snapshotMetadataList.stream()
        .filter((snapshot) -> matchingSnapshots.contains(snapshot.snapshotId))
        .collect(Collectors.toList());
  }

  /**
   * Returns true if the given Snapshot: 1. contains data between startTimeEpochMs and
   * endTimeEpochMs; AND 2. is from one of the partitions containing data from the queried service
   */
  private static boolean snapshotContainsRequestedDataAndIsWithinTimeframe(
      long startTimeEpochMs,
      long endTimeEpochMs,
      Set<String> partitionIdsWithQueriedData,
      SnapshotMetadata snapshot) {
    return ChunkInfo.containsDataInTimeRange(
            snapshot.startTimeEpochMs, snapshot.endTimeEpochMs, startTimeEpochMs, endTimeEpochMs)
        && partitionIdsWithQueriedData.contains(snapshot.partitionId);
  }

  @Override
  public void resetPartitionData(
      ManagerApi.ResetPartitionDataRequest request,
      StreamObserver<ManagerApi.ResetPartitionDataResponse> responseObserver) {
    List<SnapshotMetadata> snapshotMetadataList = snapshotMetadataStore.listSync();

    int resetCount = 0;
    for (SnapshotMetadata snapshotMetadata : snapshotMetadataList) {
      if (Objects.equals(snapshotMetadata.partitionId, request.getPartitionId())) {
        if (!request.getDryRun()) {
          snapshotMetadata.maxOffset = 0;
          snapshotMetadataStore.updateSync(snapshotMetadata);
        }
        resetCount++;
      }
    }

    if (request.getDryRun()) {
      responseObserver.onNext(
          ManagerApi.ResetPartitionDataResponse.newBuilder()
              .setStatus(
                  String.format(
                      "%s snapshots matching partitionId '%s' out of %s total snapshots, none were reset as this was a dry-run.",
                      resetCount, request.getPartitionId(), snapshotMetadataList.size()))
              .build());
    } else {
      responseObserver.onNext(
          ManagerApi.ResetPartitionDataResponse.newBuilder()
              .setStatus(
                  String.format(
                      "Reset %s snapshots matching partitionId '%s' out of %s total snapshots.",
                      resetCount, request.getPartitionId(), snapshotMetadataList.size()))
              .build());
    }

    responseObserver.onCompleted();
  }

  @Override
  public synchronized void createPartition(
      ManagerApi.CreatePartitionRequest request,
      StreamObserver<Metadata.PartitionMetadata> responseObserver) {
    try {
      Preconditions.checkArgument(
          !request.getPartitionId().isBlank(), "Partition ID must not be blank");
      Preconditions.checkArgument(
          request.getMaxCapacity() > 0, "Max capacity must be set when creating a new partition");

      if (partitionMetadataStore.hasSync(request.getPartitionId())) {
        String msg = "Partition with id '%s' already exists".formatted(request.getPartitionId());
        LOG.error(msg);
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription(msg).asException());
        return;
      }

      PartitionMetadata newPartitionMetadata =
          new PartitionMetadata(request.getPartitionId(), request.getMaxCapacity());
      partitionMetadataStore.createSync(newPartitionMetadata);
      responseObserver.onNext(toPartitionMetadataProto(newPartitionMetadata));
      responseObserver.onCompleted();
      LOG.info(
          "Created partition: {}, max capacity: {}",
          request.getPartitionId(),
          request.getMaxCapacity());
    } catch (IllegalArgumentException e) {
      LOG.error("Error creating new partition", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
    } catch (Exception e) {
      LOG.error("Error creating new partition", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public synchronized void deletePartition(
      ManagerApi.DeletePartitionRequest request,
      StreamObserver<ManagerApi.DeletePartitionResponse> responseObserver) {
    try {
      if (!partitionMetadataStore.hasSync(request.getPartitionId())) {
        String msg = "Partition with id '%s' does not exist".formatted(request.getPartitionId());
        responseObserver.onError(Status.NOT_FOUND.withDescription(msg).asException());
        return;
      }
      boolean partitionIsReferenced =
          datasetMetadataStore.listSync().stream()
              .flatMap(dataset -> dataset.getPartitionConfigs().stream())
              .flatMap(partitionConfig -> partitionConfig.getPartitions().stream())
              .anyMatch(request.getPartitionId()::equals);
      if (partitionIsReferenced) {
        String msg =
            "Partition with id '%s' is still referenced by a dataset assignment"
                .formatted(request.getPartitionId());
        responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(msg).asException());
        return;
      }

      partitionMetadataStore.deleteSync(request.getPartitionId());
      responseObserver.onNext(
          ManagerApi.DeletePartitionResponse.newBuilder()
              .setStatus(
                  String.format("Deleted partition %s successfully", request.getPartitionId()))
              .build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      LOG.error("Error deleting partition", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Error deleting partition", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public synchronized void listPartition(
      ManagerApi.ListPartitionRequest request,
      StreamObserver<ManagerApi.ListPartitionMetadataResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ManagerApi.ListPartitionMetadataResponse.newBuilder()
              .addAllPartitionMetadata(
                  partitionAssignmentService.listLivePartitionStates().stream()
                      .map(ManagerApiGrpc::toCalculatedPartitionMetadataProto)
                      .toList())
              .build());
      responseObserver.onCompleted();
    } catch (InvalidPartitionAssignmentStateException e) {
      LOG.error("Error fetching partition list", e);
      responseObserver.onError(
          Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
    } catch (StatusRuntimeException e) {
      LOG.error("Error fetching partition list", e);
      responseObserver.onError(e);
    } catch (Exception e) {
      LOG.error("Error fetching partition list", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  private static ManagerApi.CalculatedPartitionMetadata toCalculatedPartitionMetadataProto(
      LivePartitionState metadata) {
    ManagerApi.CalculatedPartitionMetadata.Builder builder =
        ManagerApi.CalculatedPartitionMetadata.newBuilder()
            .setPartitionId(metadata.getPartitionID())
            .setProvisionedCapacity(metadata.getProvisionedCapacity())
            .setMaxCapacity(metadata.getMaxCapacity());

    PartitionOccupancy occupancy = metadata.getOccupancy();
    if (occupancy instanceof PartitionOccupancy.Empty) {
      builder.setEmpty(ManagerApi.EmptyPartitionOccupancy.newBuilder().build());
    } else if (occupancy instanceof PartitionOccupancy.Shared shared) {
      builder.setShared(
          ManagerApi.SharedPartitionOccupancy.newBuilder()
              .addAllDatasets(shared.datasets())
              .build());
    } else if (occupancy instanceof PartitionOccupancy.Dedicated dedicated) {
      builder.setDedicated(
          ManagerApi.DedicatedPartitionOccupancy.newBuilder()
              .setDataset(dedicated.dataset())
              .build());
    }
    return builder.build();
  }

  private static DedicatedPartitionModeOverride toDedicatedPartitionModeOverride(
      ManagerApi.UpdatePartitionAssignmentRequest request) {
    if (!request.hasRequireDedicatedPartition()) {
      return DedicatedPartitionModeOverride.PRESERVE_EXISTING;
    }
    return request.getRequireDedicatedPartition()
        ? DedicatedPartitionModeOverride.REQUIRE_DEDICATED
        : DedicatedPartitionModeOverride.REQUIRE_SHARED;
  }

  /** Creates a new field redaction */
  @Override
  public void createFieldRedaction(
      ManagerApi.CreateFieldRedactionRequest request,
      StreamObserver<Metadata.RedactedFieldMetadata> responseObserver) {
    try {
      fieldRedactionMetadataStore.createSync(
          new FieldRedactionMetadata(
              request.getName(),
              request.getFieldName(),
              request.getStartTimeEpochMs(),
              request.getEndTimeEpochMs()));
      responseObserver.onNext(
          toRedactedFieldMetadataProto(fieldRedactionMetadataStore.getSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error creating new field redaction", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns a single field redaction by name */
  @Override
  public void getFieldRedaction(
      ManagerApi.GetFieldRedactionRequest request,
      StreamObserver<Metadata.RedactedFieldMetadata> responseObserver) {

    try {
      responseObserver.onNext(
          toRedactedFieldMetadataProto(fieldRedactionMetadataStore.getSync(request.getName())));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting field redaction", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Deletes a single field redaction by name */
  @Override
  public void deleteFieldRedaction(
      ManagerApi.DeleteFieldRedactionRequest request,
      StreamObserver<Metadata.RedactedFieldMetadata> responseObserver) {

    try {
      FieldRedactionMetadata deletedFieldRedaction =
          fieldRedactionMetadataStore.getSync(request.getName());
      fieldRedactionMetadataStore.deleteSync(request.getName());
      responseObserver.onNext(toRedactedFieldMetadataProto(deletedFieldRedaction));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error deleting field redaction", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  /** Returns all existing field redactions from the metadata store */
  @Override
  public void listFieldRedactions(
      ManagerApi.ListFieldRedactionsRequest request,
      StreamObserver<ManagerApi.ListFieldRedactionsResponse> responseObserver) {
    try {
      responseObserver.onNext(
          ManagerApi.ListFieldRedactionsResponse.newBuilder()
              .addAllRedactedFields(
                  fieldRedactionMetadataStore.listSync().stream()
                      .map(FieldRedactionMetadataSerializer::toRedactedFieldMetadataProto)
                      .collect(Collectors.toList()))
              .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error getting field redactions", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }
}
