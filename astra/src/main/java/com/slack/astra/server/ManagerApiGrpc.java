package com.slack.astra.server;

import static com.slack.astra.metadata.dataset.DatasetMetadataSerializer.toDatasetMetadataProto;
import static com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataSerializer.toRedactedFieldMetadataProto;
import static com.slack.astra.metadata.partition.PartitionMetadataSerializer.toPartitionMetadataProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
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
import com.slack.astra.server.partitionassignment.LivePartitionState;
import com.slack.astra.server.partitionassignment.PartitionOccupancy;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  public static final long MAX_TIME = Long.MAX_VALUE;
  private final ReplicaRestoreService replicaRestoreService;
  private final FieldRedactionMetadataStore fieldRedactionMetadataStore;
  private final PartitionMetadataStore partitionMetadataStore;
  private final int minNumberOfPartitions;

  public ManagerApiGrpc(
      DatasetMetadataStore datasetMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      ReplicaRestoreService replicaRestoreService,
      FieldRedactionMetadataStore fieldRedactionMetadataStore) {
    this(
        datasetMetadataStore,
        null,
        snapshotMetadataStore,
        replicaRestoreService,
        fieldRedactionMetadataStore,
        2);
  }

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
    this.partitionMetadataStore = partitionMetadataStore;
    this.minNumberOfPartitions = minNumberOfPartitions <= 0 ? 2 : minNumberOfPartitions;
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

  private PartitionMetadataStore requirePartitionMetadataStore() {
    if (partitionMetadataStore == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Partition metadata store is not configured")
          .asRuntimeException();
    }
    return partitionMetadataStore;
  }

  private PartitionMetadataFromDatasetConfigs createPartitionMetadataFromDatasetConfigs() {
    return new PartitionMetadataFromDatasetConfigs(
        datasetMetadataStore.listSync(),
        requirePartitionMetadataStore().listSync(),
        minNumberOfPartitions);
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
      Preconditions.checkArgument(
          request.getPartitionIdsList().stream().noneMatch(String::isBlank),
          "PartitionIds list must not contain blank strings");
      Preconditions.checkArgument(!request.getName().isBlank(), "Dataset name must not be blank");

      DatasetMetadata datasetMetadata;
      try {
        datasetMetadata = datasetMetadataStore.getSync(request.getName());
      } catch (Exception e) {
        String msg = "No dataset named, '%s'. Please create it first.".formatted(request.getName());
        LOG.error(msg, e);
        responseObserver.onError(Status.NOT_FOUND.withDescription(msg).asException());
        return;
      }

      long updatedThroughputBytes =
          request.getThroughputBytes() < 0
              ? datasetMetadata.getThroughputBytes()
              : request.getThroughputBytes();
      boolean requireDedicatedPartition =
          request.hasRequireDedicatedPartition()
              ? request.getRequireDedicatedPartition()
              : datasetMetadata.isUsingDedicatedPartitions();

      List<String> partitionIdList;
      if (request.getPartitionIdsList().isEmpty()) {
        partitionIdList =
            autoAssignPartition(
                datasetMetadata,
                updatedThroughputBytes,
                requireDedicatedPartition,
                createPartitionMetadataFromDatasetConfigs());
        LOG.info("Auto-assigning partitions for {} to : {}", request.getName(), partitionIdList);
        if (partitionIdList.isEmpty()) {
          String msg = "Error updating partition assignment, could not find partitions to assign";
          LOG.error(msg);
          responseObserver.onError(Status.UNKNOWN.withDescription(msg).asException());
          return;
        }
      } else {
        partitionIdList = request.getPartitionIdsList();
        LOG.info(
            "Manually assigning partitions for {} to : {}", request.getName(), partitionIdList);
        if (partitionMetadataStore != null) {
          PartitionMetadataFromDatasetConfigs partitionData =
              createPartitionMetadataFromDatasetConfigs();
          List<String> nonExistentRequestedPartitionIds =
              partitionIdList.stream()
                  .filter(id -> !partitionData.getPartitionIds().contains(id))
                  .sorted()
                  .toList();
          Preconditions.checkArgument(
              nonExistentRequestedPartitionIds.isEmpty(),
              "Requested partition IDs do not exist: %s"
                  .formatted(nonExistentRequestedPartitionIds));
        }
      }
      partitionIdList = partitionIdList.stream().sorted().toList();

      ImmutableList<DatasetPartitionMetadata> updatedDatasetPartitionMetadata =
          addNewPartition(datasetMetadata, partitionIdList);

      DatasetMetadata updatedDatasetMetadata =
          new DatasetMetadata(
              datasetMetadata.getName(),
              datasetMetadata.getOwner(),
              updatedThroughputBytes,
              updatedDatasetPartitionMetadata,
              datasetMetadata.getServiceNamePattern(),
              requireDedicatedPartition);
      datasetMetadataStore.updateSync(updatedDatasetMetadata);

      responseObserver.onNext(
          ManagerApi.UpdatePartitionAssignmentResponse.newBuilder()
              .addAllAssignedPartitionIds(partitionIdList)
              .build());
      responseObserver.onCompleted();
      LOG.info(
          "Updated partition assignment for dataset: {}, throughput: {} -> {} partitions: {} -> {}",
          request.getName(),
          datasetMetadata.getThroughputBytes(),
          updatedThroughputBytes,
          datasetMetadata.getLatestPartitionMetadata(),
          partitionIdList);
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

  /**
   * Returns a new list of dataset partition metadata, with the provided partition IDs as the
   * current active assignment. This finds the current active assignment (end time of max long),
   * sets it to the current time, and then appends a new dataset partition assignment starting from
   * current time + 1 to max long.
   */
  private static ImmutableList<DatasetPartitionMetadata> addNewPartition(
      DatasetMetadata datasetMetadata, List<String> newPartitionIdsList) {
    ImmutableList<DatasetPartitionMetadata> existingPartitions =
        datasetMetadata.getPartitionConfigs();
    if (newPartitionIdsList.isEmpty()) {
      return ImmutableList.copyOf(existingPartitions);
    }

    Optional<DatasetPartitionMetadata> previousActiveDatasetPartition =
        datasetMetadata.getLatestPartitionMetadata();
    List<DatasetPartitionMetadata> remainingDatasetPartitions =
        datasetMetadata.getAllButLatestDatasetPartitions();

    if (previousActiveDatasetPartition.isPresent()
        && previousActiveDatasetPartition.get().getPartitions().equals(newPartitionIdsList)) {
      return ImmutableList.copyOf(existingPartitions);
    }

    long partitionCutoverTime = Instant.now().toEpochMilli();

    ImmutableList.Builder<DatasetPartitionMetadata> builder =
        ImmutableList.<DatasetPartitionMetadata>builder().addAll(remainingDatasetPartitions);

    if (previousActiveDatasetPartition.isPresent()) {
      DatasetPartitionMetadata updatedPreviousActivePartition =
          new DatasetPartitionMetadata(
              previousActiveDatasetPartition.get().getStartTimeEpochMs(),
              partitionCutoverTime,
              previousActiveDatasetPartition.get().getPartitions());
      builder.add(updatedPreviousActivePartition);
    }

    DatasetPartitionMetadata newPartitionMetadata =
        new DatasetPartitionMetadata(partitionCutoverTime + 1, MAX_TIME, newPartitionIdsList);
    return builder.add(newPartitionMetadata).build();
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

      PartitionMetadataStore store = requirePartitionMetadataStore();
      if (store.hasSync(request.getPartitionId())) {
        String msg = "Partition with id '%s' already exists".formatted(request.getPartitionId());
        LOG.error(msg);
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription(msg).asException());
        return;
      }

      PartitionMetadata newPartitionMetadata =
          new PartitionMetadata(request.getPartitionId(), request.getMaxCapacity());
      store.createSync(newPartitionMetadata);
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
    } catch (StatusRuntimeException e) {
      LOG.error("Error creating new partition", e);
      responseObserver.onError(e);
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
      PartitionMetadataStore store = requirePartitionMetadataStore();
      if (!store.hasSync(request.getPartitionId())) {
        String msg = "Partition with id '%s' does not exist".formatted(request.getPartitionId());
        responseObserver.onError(Status.NOT_FOUND.withDescription(msg).asException());
        return;
      }
      boolean partitionIsReferenced =
          datasetMetadataStore.listSync().stream()
              .map(dataset -> datasetMetadataStore.getSync(dataset.getName()))
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

      store.deleteSync(request.getPartitionId());
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
                  createPartitionMetadataFromDatasetConfigs().getLivePMDs().stream()
                      .map(ManagerApiGrpc::toCalculatedPartitionMetadataProto)
                      .toList())
              .build());
      responseObserver.onCompleted();
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

  /**
   * Automatically finds partition IDs for the requested dataset shape while minimizing partition
   * churn where possible.
   */
  private static ImmutableList<String> autoAssignPartition(
      DatasetMetadata datasetMetadata,
      long throughputBytes,
      boolean requireDedicatedPartition,
      PartitionMetadataFromDatasetConfigs partitionMetadataFromDatasetConfigs) {
    if (partitionMetadataFromDatasetConfigs.hasNoPartitionsDeclared()) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed %d partitions with enough capacity, found 0"
                  .formatted(partitionMetadataFromDatasetConfigs.minNumberOfPartitions))
          .asRuntimeException();
    }

    List<String> currentPartitions =
        datasetMetadata
            .getLatestPartitionMetadata()
            .map(DatasetPartitionMetadata::getPartitions)
            .orElseGet(ImmutableList::of);

    PartitionMetadataFromDatasetConfigs partitionMetadataAfterDroppingDatasetBeingModified =
        partitionMetadataFromDatasetConfigs.minusDataset(datasetMetadata);
    if (requireDedicatedPartition) {
      List<LivePartitionState> reusablePartitions =
          partitionMetadataAfterDroppingDatasetBeingModified.existingDedicatedPartitions(
              datasetMetadata);
      Comparator<LivePartitionState> compareByAvailableCapacityThenId =
          Comparator.comparing(LivePartitionState::getAvailableCapacity)
              .thenComparing(LivePartitionState::getPartitionID);
      List<LivePartitionState> emptyPartitions =
          partitionMetadataFromDatasetConfigs.currentEmptyPartitions();
      List<LivePartitionState> sortedPartitions =
          Stream.concat(
                  reusablePartitions.stream().sorted(compareByAvailableCapacityThenId),
                  emptyPartitions.stream().sorted(compareByAvailableCapacityThenId))
              .toList();
      LOG.debug(
          "current empty partitions: {}, list to pull from {}",
          partitionMetadataAfterDroppingDatasetBeingModified.currentEmptyPartitions().stream()
              .map(LivePartitionState::getPartitionID)
              .toList(),
          sortedPartitions.stream().map(LivePartitionState::getPartitionID).toList());

      ImmutableList<String> lastProposal = ImmutableList.of();
      for (long proposedPartitionCt = partitionMetadataFromDatasetConfigs.minNumberOfPartitions;
          proposedPartitionCt <= sortedPartitions.size();
          proposedPartitionCt++) {
        long nextPerPartitionThroughput = Math.ceilDiv(throughputBytes, proposedPartitionCt);
        lastProposal =
            sortedPartitions.stream()
                .filter(p -> p.getAvailableCapacity() >= nextPerPartitionThroughput)
                .limit(proposedPartitionCt)
                .map(LivePartitionState::getPartitionID)
                .collect(ImmutableList.toImmutableList());
        LOG.debug(
            "dedicated proposal for partition count: {}, per partition throughput: {}, proposal: {}",
            proposedPartitionCt,
            nextPerPartitionThroughput,
            lastProposal);
        if (lastProposal.size() == proposedPartitionCt) {
          return lastProposal;
        }
      }

      throw Status.FAILED_PRECONDITION
          .withDescription(
              "Needed %d partitions with enough capacity, found %d: %s"
                  .formatted(
                      partitionMetadataFromDatasetConfigs.minNumberOfPartitions,
                      lastProposal.size(),
                      lastProposal))
          .asRuntimeException();
    }

    List<LivePartitionState> partitionsSorted =
        partitionMetadataAfterDroppingDatasetBeingModified.getLivePMDs().stream()
            .filter(partition -> partition.canUseForSharedAssignment(datasetMetadata.getName()))
            .sorted(
                Comparator.comparing(
                        (LivePartitionState p) ->
                            currentPartitions.contains(p.getPartitionID()))
                    .reversed()
                    .thenComparing(LivePartitionState::getAvailableCapacity)
                    .thenComparing(LivePartitionState::getPartitionID))
            .toList();
    LOG.debug(
        "partitions sorted: {}",
        partitionsSorted.stream().map(LivePartitionState::getPartitionID).toList());

    ImmutableList<String> lastProposal = ImmutableList.of();
    for (long proposedPartitionCt = 1;
        proposedPartitionCt <= partitionsSorted.size();
        proposedPartitionCt++) {
      long nextPerPartitionThroughput = Math.ceilDiv(throughputBytes, proposedPartitionCt);
      lastProposal =
          partitionsSorted.stream()
              .filter(p -> p.getAvailableCapacity() >= nextPerPartitionThroughput)
              .limit(proposedPartitionCt)
              .map(LivePartitionState::getPartitionID)
              .collect(ImmutableList.toImmutableList());
      LOG.debug(
          "proposal for partition count: {}, per partition throughput: {}, proposal: {}",
          proposedPartitionCt,
          nextPerPartitionThroughput,
          lastProposal);
      if (lastProposal.size() == proposedPartitionCt
          && proposedPartitionCt >= partitionMetadataFromDatasetConfigs.minNumberOfPartitions) {
        return lastProposal;
      }
    }
    throw Status.FAILED_PRECONDITION
        .withDescription(
            "Needed %d partitions with enough capacity, found %d: %s"
                .formatted(
                    partitionMetadataFromDatasetConfigs.minNumberOfPartitions,
                    lastProposal.size(),
                    lastProposal))
        .asRuntimeException();
  }

  /** Holds the calculated live partition state used for assignment and partition listing. */
  private static class PartitionMetadataFromDatasetConfigs {
    private final long minNumberOfPartitions;
    private final List<LivePartitionState> livePMDs;

    PartitionMetadataFromDatasetConfigs(
        List<DatasetMetadata> datasetMetadataList,
        List<PartitionMetadata> partitionMetadataList,
        long minNumberOfPartitions) {
      this(
          calculatePartitionMetadataFromConfig(datasetMetadataList, partitionMetadataList),
          minNumberOfPartitions);
    }

    PartitionMetadataFromDatasetConfigs(
        List<LivePartitionState> livePMDs, long minNumberOfPartitions) {
      this.livePMDs = livePMDs;
      this.minNumberOfPartitions = minNumberOfPartitions;
    }

    PartitionMetadataFromDatasetConfigs minusDataset(DatasetMetadata datasetMetadata) {
      long currentPerPartitionThroughput = datasetMetadata.getLatestPerPartitionThroughput();
      ImmutableList<String> currentIds =
          datasetMetadata
              .getLatestPartitionMetadata()
              .map(DatasetPartitionMetadata::getPartitions)
              .orElse(ImmutableList.of());

      List<LivePartitionState> newList =
          livePMDs.stream()
              .map(
                  p -> {
                    if (currentIds.contains(p.getPartitionID())) {
                      return new LivePartitionState(
                          p.getPartitionID(),
                          Math.max(0, p.getProvisionedCapacity() - currentPerPartitionThroughput),
                          p.getMaxCapacity(),
                          p.getOccupancy());
                    }
                    return p;
                  })
              .toList();
      return new PartitionMetadataFromDatasetConfigs(newList, minNumberOfPartitions);
    }

    boolean hasNoPartitionsDeclared() {
      return livePMDs.isEmpty();
    }

    List<LivePartitionState> currentEmptyPartitions() {
      return livePMDs.stream().filter(LivePartitionState::isEmpty).toList();
    }

    List<LivePartitionState> getLivePMDs() {
      return livePMDs;
    }

    List<String> getPartitionIds() {
      return livePMDs.stream().map(LivePartitionState::getPartitionID).toList();
    }

    List<LivePartitionState> existingDedicatedPartitions(DatasetMetadata datasetMetadata) {
      return livePMDs.stream().filter(p -> p.isDedicatedOnlyTo(datasetMetadata.getName())).toList();
    }
  }

  private static List<LivePartitionState> calculatePartitionMetadataFromConfig(
      List<DatasetMetadata> datasetMetadataList, List<PartitionMetadata> partitionMetadataList) {
    final Map<String, List<String>> partitionDatasets = new HashMap<>();
    final Map<String, Long> partitionProvisioning = new HashMap<>();
    final Map<String, String> partitionDedicatedOwner = new HashMap<>();
    for (PartitionMetadata partitionMetadata : partitionMetadataList) {
      partitionProvisioning.put(partitionMetadata.getPartitionID(), 0L);
      partitionDatasets.put(partitionMetadata.getPartitionID(), new ArrayList<>());
    }

    for (DatasetMetadata datasetMetadata : datasetMetadataList) {
      Optional<DatasetPartitionMetadata> latest = datasetMetadata.getLatestPartitionMetadata();
      long perPartitionValue = datasetMetadata.getLatestPerPartitionThroughput();
      boolean useDedicatedPartition = datasetMetadata.isUsingDedicatedPartitions();
      for (String partitionId :
          latest.map(DatasetPartitionMetadata::getPartitions).orElse(ImmutableList.of())) {
        if (!partitionProvisioning.containsKey(partitionId)) {
          LOG.warn(
              "Dataset {} references partition {} that is not in the partition catalog",
              datasetMetadata.getName(),
              partitionId);
          continue;
        }
        partitionProvisioning.put(
            partitionId, perPartitionValue + partitionProvisioning.getOrDefault(partitionId, 0L));
        partitionDatasets.get(partitionId).add(datasetMetadata.getName());
        if (useDedicatedPartition) {
          String existingDedicatedOwner =
              partitionDedicatedOwner.putIfAbsent(partitionId, datasetMetadata.getName());
          Preconditions.checkArgument(
              existingDedicatedOwner == null
                  || existingDedicatedOwner.equals(datasetMetadata.getName()),
              "partition %s cannot be dedicated to multiple datasets".formatted(partitionId));
        }
      }
    }

    return partitionMetadataList.stream()
        .sorted(Comparator.comparing(PartitionMetadata::getPartitionID))
        .map(
            p -> {
              List<String> datasets = partitionDatasets.get(p.getPartitionID());
              String dedicatedOwner = partitionDedicatedOwner.get(p.getPartitionID());

              PartitionOccupancy occupancy;
              if (datasets.isEmpty()) {
                occupancy = new PartitionOccupancy.Empty();
              } else if (dedicatedOwner == null) {
                occupancy = new PartitionOccupancy.Shared(datasets);
              } else {
                Preconditions.checkArgument(
                    datasets.size() == 1 && datasets.get(0).equals(dedicatedOwner),
                    "partition occupancy must be empty, shared, or dedicated to exactly one dataset");
                occupancy = new PartitionOccupancy.Dedicated(dedicatedOwner);
              }

              return new LivePartitionState(
                  p.getPartitionID(),
                  partitionProvisioning.get(p.getPartitionID()),
                  p.getMaxCapacity(),
                  occupancy);
            })
        .toList();
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
