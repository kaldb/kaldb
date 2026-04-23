package com.slack.astra.server;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.astra.server.ManagerApiGrpc.MAX_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import brave.Tracing;
import com.slack.astra.clusterManager.ReplicaRestoreService;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.dataset.DatasetPartitionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadata;
import com.slack.astra.metadata.fieldredaction.FieldRedactionMetadataStore;
import com.slack.astra.metadata.partition.PartitionMetadata;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.manager_api.ManagerApi;
import com.slack.astra.proto.manager_api.ManagerApiServiceGrpc;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.MetricsUtil;
import com.slack.astra.util.GrpcCleanupExtension;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ManagerApiGrpcTest {

  @RegisterExtension public final GrpcCleanupExtension grpcCleanup = new GrpcCleanupExtension();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private DatasetMetadataStore datasetMetadataStore;
  private PartitionMetadataStore partitionMetadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private ReplicaRestoreService replicaRestoreService;
  private FieldRedactionMetadataStore fieldRedactionMetadataStore;
  private ManagerApiServiceGrpc.ManagerApiServiceBlockingStub managerApiStub;

  @BeforeEach
  public void setUp() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ManagerApiGrpcTest")
                    .setZkSessionTimeoutMs(30000)
                    .setZkConnectionTimeoutMs(30000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    datasetMetadataStore =
        spy(new DatasetMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true));
    partitionMetadataStore =
        spy(new PartitionMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry, true));
    snapshotMetadataStore =
        spy(new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    replicaMetadataStore =
        spy(new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    fieldRedactionMetadataStore =
        spy(
            new FieldRedactionMetadataStore(
                curatorFramework, metadataStoreConfig, meterRegistry, true));

    AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig replicaRecreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaRestoreServiceConfig.newBuilder()
            .addAllReplicaSets(List.of("rep1"))
            .setMaxReplicasPerRequest(200)
            .setReplicaLifespanMins(60)
            .setSchedulePeriodMins(30)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaRestoreServiceConfig(replicaRecreationServiceConfig)
            .build();

    replicaRestoreService =
        new ReplicaRestoreService(replicaMetadataStore, meterRegistry, managerConfig);

    managerApiStub = createManagerApiStub(nextServerName("default"), 2);
  }

  @AfterEach
  public void tearDown() throws Exception {
    replicaRestoreService.stopAsync();
    replicaRestoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    replicaMetadataStore.close();
    snapshotMetadataStore.close();
    datasetMetadataStore.close();
    partitionMetadataStore.close();
    fieldRedactionMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  private Metadata.PartitionMetadata createPartition(String partitionId, long maxCapacity) {
    Metadata.PartitionMetadata createdPartition =
        managerApiStub.createPartition(
            ManagerApi.CreatePartitionRequest.newBuilder()
                .setPartitionId(partitionId)
                .setMaxCapacity(maxCapacity)
                .build());
    await()
        .untilAsserted(
            () ->
                assertThat(partitionMetadataStore.listSync())
                    .extracting(PartitionMetadata::getPartitionId)
                    .contains(partitionId));
    return createdPartition;
  }

  private void createPartitions(List<String> partitionIds) {
    partitionIds.forEach(partitionId -> createPartition(partitionId, 100));
  }

  private String nextServerName(String suffix) {
    return getClass().getName() + "-" + suffix + "-" + System.nanoTime();
  }

  private ManagerApiServiceGrpc.ManagerApiServiceBlockingStub createManagerApiStub(
      String serverName, int minNumberOfPartitions) throws Exception {
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(
                new ManagerApiGrpc(
                    datasetMetadataStore,
                    partitionMetadataStore,
                    snapshotMetadataStore,
                    replicaRestoreService,
                    fieldRedactionMetadataStore,
                    minNumberOfPartitions))
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());

    return ManagerApiServiceGrpc.newBlockingStub(channel);
  }

  @Test
  public void shouldCreateAndGetNewDataset() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .build());

    Metadata.DatasetMetadata getDatasetMetadataResponse =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());
    assertThat(getDatasetMetadataResponse.getName()).isEqualTo(datasetName);
    assertThat(getDatasetMetadataResponse.getOwner()).isEqualTo(datasetOwner);
    assertThat(getDatasetMetadataResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(getDatasetMetadataResponse.getPartitionConfigsList().size()).isEqualTo(0);

    DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingDuplicateDatasetName() {
    String datasetName = "testDataset";
    String datasetOwner1 = "testOwner1";
    String datasetOwner2 = "testOwner2";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner1)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner2)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    DatasetMetadata datasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(datasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.getOwner()).isEqualTo(datasetOwner1);
    assertThat(datasetMetadata.getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorCreatingWithInvalidDatasetNames() {
    String datasetOwner = "testOwner";

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName("")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName("/")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(".")
                            .setOwner(datasetOwner)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorWithEmptyOwnerInformation() {
    String datasetName = "testDataset";

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner("")
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("owner must not be null or blank");

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdateExistingDataset() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    String serviceNamePattern = "serviceNamePattern";
    String updatedServiceNamePattern = "updatedServiceNamePattern";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner(datasetOwner)
            .setServiceNamePattern(serviceNamePattern)
            .build());

    String updatedDatasetOwner = "testOwnerUpdated";
    Metadata.DatasetMetadata updatedDatasetResponse =
        managerApiStub.updateDatasetMetadata(
            ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(updatedDatasetOwner)
                .setServiceNamePattern(serviceNamePattern)
                .build());

    assertThat(updatedDatasetResponse.getName()).isEqualTo(datasetName);
    assertThat(updatedDatasetResponse.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(updatedDatasetResponse.getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(updatedDatasetResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedDatasetResponse.getPartitionConfigsList().size()).isEqualTo(0);

    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getOwner().equals(updatedDatasetOwner);
            });

    assertThat(datasetMetadata.get().getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.get().getServiceNamePattern()).isEqualTo(serviceNamePattern);
    assertThat(datasetMetadata.get().getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.get().getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.get().getPartitionConfigs().size()).isEqualTo(0);

    Metadata.DatasetMetadata updatedServiceNamePatternResponse =
        managerApiStub.updateDatasetMetadata(
            ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(updatedDatasetOwner)
                .setServiceNamePattern(updatedServiceNamePattern)
                .build());

    assertThat(updatedServiceNamePatternResponse.getName()).isEqualTo(datasetName);
    assertThat(updatedServiceNamePatternResponse.getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(updatedServiceNamePatternResponse.getServiceNamePattern())
        .isEqualTo(updatedServiceNamePattern);
    assertThat(updatedServiceNamePatternResponse.getThroughputBytes()).isEqualTo(0);
    assertThat(updatedServiceNamePatternResponse.getPartitionConfigsList().size()).isEqualTo(0);

    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return Objects.equals(
                  datasetMetadata.get().getServiceNamePattern(), updatedServiceNamePattern);
            });

    datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
    assertThat(datasetMetadata.get().getName()).isEqualTo(datasetName);
    assertThat(datasetMetadata.get().getServiceNamePattern()).isEqualTo(updatedServiceNamePattern);
    assertThat(datasetMetadata.get().getOwner()).isEqualTo(updatedDatasetOwner);
    assertThat(datasetMetadata.get().getThroughputBytes()).isEqualTo(0);
    assertThat(datasetMetadata.get().getPartitionConfigs().size()).isEqualTo(0);
  }

  @Test
  public void shouldErrorGettingNonexistentDataset() {
    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.getDatasetMetadata(
                        ManagerApi.GetDatasetMetadataRequest.newBuilder().setName("foo").build()));
    Status status = throwable.getStatus();
    assertThat(status.getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldUpdatePartitionAssignments() {
    String datasetName = "testDataset";
    String datasetOwner = "testOwner";

    Metadata.DatasetMetadata initialDatasetRequest =
        managerApiStub.createDatasetMetadata(
            ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                .setName(datasetName)
                .setOwner(datasetOwner)
                .build());
    assertThat(initialDatasetRequest.getPartitionConfigsList().size()).isEqualTo(0);

    createPartitions(List.of("1", "2", "3", "4", "5"));

    long nowMs = Instant.now().toEpochMilli();
    long throughputBytes = 10;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(throughputBytes)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().size() == 1
                    && datasetMetadataStore.listSync().get(0).getThroughputBytes()
                        == throughputBytes);

    Metadata.DatasetMetadata firstAssignment =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(datasetName).build());

    assertThat(firstAssignment.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstAssignment.getPartitionConfigsList().size()).isEqualTo(1);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(firstAssignment.getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata firstDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(firstDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(firstDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(firstDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(firstDatasetMetadata.getPartitionConfigs().size()).isEqualTo(1);

    // only update the partition assignment, leaving throughput
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(-1)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());

    AtomicReference<Metadata.DatasetMetadata> secondAssignment = new AtomicReference<>();
    await()
        .until(
            () -> {
              secondAssignment.set(
                  managerApiStub.getDatasetMetadata(
                      ManagerApi.GetDatasetMetadataRequest.newBuilder()
                          .setName(datasetName)
                          .build()));
              return secondAssignment.get().getThroughputBytes() == throughputBytes;
            });

    assertThat(secondAssignment.get().getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondAssignment.get().getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(secondAssignment.get().getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(secondAssignment.get().getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(secondAssignment.get().getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata secondDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(secondDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(secondDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(secondDatasetMetadata.getThroughputBytes()).isEqualTo(throughputBytes);
    assertThat(secondDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);

    // only update the throughput while explicitly preserving the partition assignment
    long updatedThroughputBytes = 12;
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(updatedThroughputBytes)
            .addAllPartitionIds(List.of("3", "4", "5"))
            .build());

    AtomicReference<Metadata.DatasetMetadata> thirdAssignment = new AtomicReference<>();
    await()
        .until(
            () -> {
              thirdAssignment.set(
                  managerApiStub.getDatasetMetadata(
                      ManagerApi.GetDatasetMetadataRequest.newBuilder()
                          .setName(datasetName)
                          .build()));
              return thirdAssignment.get().getThroughputBytes() == updatedThroughputBytes;
            });

    assertThat(thirdAssignment.get().getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdAssignment.get().getPartitionConfigsList().size()).isEqualTo(2);
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(0).getPartitionsList())
        .isEqualTo(List.of("1", "2"));
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(0).getEndTimeEpochMs())
        .isNotEqualTo(MAX_TIME);

    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getPartitionsList())
        .isEqualTo(List.of("3", "4", "5"));
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getStartTimeEpochMs())
        .isGreaterThanOrEqualTo(nowMs);
    assertThat(thirdAssignment.get().getPartitionConfigsList().get(1).getEndTimeEpochMs())
        .isEqualTo(MAX_TIME);

    DatasetMetadata thirdDatasetMetadata = datasetMetadataStore.getSync(datasetName);
    assertThat(thirdDatasetMetadata.getName()).isEqualTo(datasetName);
    assertThat(thirdDatasetMetadata.getOwner()).isEqualTo(datasetOwner);
    assertThat(thirdDatasetMetadata.getThroughputBytes()).isEqualTo(updatedThroughputBytes);
    assertThat(thirdDatasetMetadata.getPartitionConfigs().size()).isEqualTo(2);
  }

  @Test
  public void shouldCreateListAndDeletePartitions() {
    Metadata.PartitionMetadata createdPartition = createPartition("1", 250);
    assertThat(createdPartition.getPartitionId()).isEqualTo("1");
    assertThat(createdPartition.getMaxCapacity()).isEqualTo(250);

    ManagerApi.ListPartitionMetadataResponse listPartitionResponse =
        managerApiStub.listPartitionMetadata(
            ManagerApi.ListPartitionRequest.newBuilder().build());
    assertThat(listPartitionResponse.getPartitionMetadataList()).hasSize(1);
    assertThat(listPartitionResponse.getPartitionMetadata(0).getPartitionId()).isEqualTo("1");
    assertThat(listPartitionResponse.getPartitionMetadata(0).getMaxCapacity()).isEqualTo(250);
    assertThat(listPartitionResponse.getPartitionMetadata(0).getProvisionedCapacity()).isZero();
    assertThat(listPartitionResponse.getPartitionMetadata(0).hasEmpty()).isTrue();

    ManagerApi.DeletePartitionResponse deletePartitionResponse =
        managerApiStub.deletePartition(
            ManagerApi.DeletePartitionRequest.newBuilder().setPartitionId("1").build());
    assertThat(deletePartitionResponse.getStatus()).isEqualTo("Deleted partition 1 successfully");

    await()
        .untilAsserted(
            () -> {
              ManagerApi.ListPartitionMetadataResponse listAfterDeleteResponse =
                  managerApiStub.listPartitionMetadata(
                      ManagerApi.ListPartitionRequest.newBuilder().build());
              assertThat(listAfterDeleteResponse.getPartitionMetadataList()).isEmpty();
            });
  }

  @Test
  public void shouldRejectCreatingPartitionWithNonNumericId() {
    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createPartition(
                        ManagerApi.CreatePartitionRequest.newBuilder()
                            .setPartitionId("partition-a")
                            .setMaxCapacity(250)
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Partition ID must be numeric: partition-a");
  }

  @Test
  public void shouldRejectCreatingDuplicatePartitionWhenStoreCreateRaces() {
    doThrow(
            new InternalMetadataStoreException(
                "duplicate partition",
                new org.apache.zookeeper.KeeperException.NodeExistsException()))
        .when(partitionMetadataStore)
        .createSync(eq(new PartitionMetadata("1", 250)));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createPartition(
                        ManagerApi.CreatePartitionRequest.newBuilder()
                            .setPartitionId("1")
                            .setMaxCapacity(250)
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.ALREADY_EXISTS.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Partition with id '1' already exists");
  }

  @Test
  public void shouldReturnInternalWhenPartitionCreateStoreFailsUnexpectedly() {
    doThrow(new InternalMetadataStoreException("store failure", new RuntimeException("boom")))
        .when(partitionMetadataStore)
        .createSync(eq(new PartitionMetadata("1", 250)));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createPartition(
                        ManagerApi.CreatePartitionRequest.newBuilder()
                            .setPartitionId("1")
                            .setMaxCapacity(250)
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INTERNAL.getCode());
    assertThat(throwable.getStatus().getDescription()).contains("store failure");
  }

  @Test
  public void shouldRejectManualAssignmentWithUnknownPartitionIds() {
    String datasetName = "manualInvalidPartitionDataset";
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    createPartition("1", 100);

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(50)
                            .addAllPartitionIds(List.of("1", "999"))
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Requested partition IDs do not exist: [999]");
  }

  @Test
  public void shouldRejectManualAssignmentWithDuplicatePartitionIds() {
    String datasetName = "manualDuplicatePartitionDataset";
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    createPartition("1", 100);

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(50)
                            .addAllPartitionIds(List.of("1", "1"))
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Requested partition IDs must be unique: [1]");
  }

  @Test
  public void shouldRejectManualAssignmentWhenBelowMinimumPartitionCount() {
    String datasetName = "manualBelowMinimumDataset";
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    createPartitions(List.of("1", "2"));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(20)
                            .addPartitionIds("1")
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Needed at least 2 partitions, found 1: [1]");
  }

  @Test
  public void shouldRejectManualSharedAssignmentWhenSelectedPartitionLacksCapacity() {
    String existingDatasetName = "existingSharedDataset";
    String datasetName = "manualSharedOverCapacityDataset";
    createPartitions(List.of("1", "2", "3"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(existingDatasetName)
            .setOwner("owner")
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(existingDatasetName)
            .setThroughputBytes(100)
            .addAllPartitionIds(List.of("1", "3"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().stream()
                    .filter(dataset -> dataset.getName().equals(existingDatasetName))
                    .findFirst()
                    .flatMap(DatasetMetadata::getActivePartitionMetadata)
                    .isPresent());

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(150)
                            .addAllPartitionIds(List.of("1", "2"))
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Shared assignment requires each selected partition to be eligible")
        .contains("[1]");
  }

  @Test
  public void shouldRejectManualDedicatedAssignmentWhenSelectedPartitionIsNotDedicatedEligible() {
    String existingDatasetName = "existingDedicatedConflictDataset";
    String datasetName = "manualDedicatedConflictDataset";
    createPartitions(List.of("1", "2", "3"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(existingDatasetName)
            .setOwner("owner")
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(existingDatasetName)
            .setThroughputBytes(100)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().stream()
                    .filter(dataset -> dataset.getName().equals(existingDatasetName))
                    .findFirst()
                    .flatMap(DatasetMetadata::getActivePartitionMetadata)
                    .isPresent());

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(20)
                            .addAllPartitionIds(List.of("1", "3"))
                            .setRequireDedicatedPartition(true)
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Dedicated assignment requires each selected partition to be eligible")
        .contains("[1]");
  }

  @Test
  public void shouldAllowManualDedicatedUpgradeOnSelfOwnedSharedPartitions() {
    String datasetName = "manualDedicatedUpgradeDataset";
    createPartitions(List.of("1", "2", "3"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(100)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore.listSync().stream()
                    .filter(dataset -> dataset.getName().equals(datasetName))
                    .findFirst()
                    .flatMap(DatasetMetadata::getActivePartitionMetadata)
                    .isPresent());

    ManagerApi.UpdatePartitionAssignmentResponse response =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(100)
                .addAllPartitionIds(List.of("1", "2"))
                .setRequireDedicatedPartition(true)
                .build());

    assertThat(response.getAssignedPartitionIdsList()).containsExactly("1", "2");
    await()
        .until(
            () ->
                datasetMetadataStore.getSync(datasetName).isUsingDedicatedPartitions()
                    && datasetMetadataStore
                        .getSync(datasetName)
                        .getActivePartitionMetadata()
                        .map(DatasetPartitionMetadata::getPartitions)
                        .orElseGet(com.google.common.collect.ImmutableList::of)
                        .equals(List.of("1", "2")));
  }

  @Test
  public void shouldAutoAssignSharedPartitions() {
    String datasetName = "sharedAutoDataset";
    createPartitions(List.of("1", "2", "3"));
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    ManagerApi.UpdatePartitionAssignmentResponse response =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(100)
                .build());

    assertThat(response.getAssignedPartitionIdsList()).containsExactly("1", "2");
    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getThroughputBytes() == 100
                  && datasetMetadata.get().getActivePartitionMetadata().isPresent();
            });
    assertThat(datasetMetadata.get().isUsingDedicatedPartitions()).isFalse();
    assertThat(datasetMetadata.get().getActivePerPartitionThroughput()).isEqualTo(50);

    ManagerApi.ListPartitionMetadataResponse listPartitionResponse =
        managerApiStub.listPartitionMetadata(
            ManagerApi.ListPartitionRequest.newBuilder().build());
    Map<String, ManagerApi.LivePartitionState> partitionsById =
        livePartitionStateById(listPartitionResponse);
    assertThat(partitionsById.keySet()).containsExactlyInAnyOrder("1", "2", "3");
    assertThat(partitionsById.get("1").getProvisionedCapacity()).isEqualTo(50);
    assertThat(partitionsById.get("1").getShared().getDatasetsList()).containsExactly(datasetName);
    assertThat(partitionsById.get("2").getProvisionedCapacity()).isEqualTo(50);
    assertThat(partitionsById.get("2").getShared().getDatasetsList()).containsExactly(datasetName);
    assertThat(partitionsById.get("3").getProvisionedCapacity()).isZero();
  }

  @Test
  public void shouldHonorConfiguredMinimumPartitionCountDuringAutoAssignment() throws Exception {
    ManagerApiServiceGrpc.ManagerApiServiceBlockingStub managerApiStubWithMinThree =
        createManagerApiStub(nextServerName("min-three"), 3);
    String datasetName = "minPartitionCountDataset";
    createPartitions(List.of("1", "2", "3"));

    managerApiStubWithMinThree.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    ManagerApi.UpdatePartitionAssignmentResponse response =
        managerApiStubWithMinThree.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(20)
                .build());

    assertThat(response.getAssignedPartitionIdsList()).containsExactly("1", "2", "3");
    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getActivePartitionMetadata().isPresent();
            });
    assertThat(datasetMetadata.get().getActivePerPartitionThroughput()).isEqualTo(7);
  }

  @Test
  public void shouldPreserveAutoAssignmentOrderingWhenPersistingAndReturningPartitions() {
    String datasetName = "numericOrderingDataset";
    createPartitions(List.of("10", "2"));
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    ManagerApi.UpdatePartitionAssignmentResponse response =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(100)
                .build());

    assertThat(response.getAssignedPartitionIdsList()).containsExactly("2", "10");
    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getActivePartitionMetadata().isPresent();
            });
    assertThat(datasetMetadata.get().getActivePartitionMetadata().orElseThrow().getPartitions())
        .containsExactly("2", "10");
  }

  @Test
  public void shouldAutoAssignDedicatedPartitions() {
    String sharedDatasetName = "sharedDataset";
    String dedicatedDatasetName = "dedicatedDataset";
    createPartitions(List.of("1", "2", "3", "4"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(sharedDatasetName)
            .setOwner("owner")
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(sharedDatasetName)
            .setThroughputBytes(100)
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore
                    .getSync(sharedDatasetName)
                    .getActivePartitionMetadata()
                    .isPresent());

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(dedicatedDatasetName)
            .setOwner("owner")
            .build());
    ManagerApi.UpdatePartitionAssignmentResponse dedicatedResponse =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(dedicatedDatasetName)
                .setThroughputBytes(150)
                .setRequireDedicatedPartition(true)
                .build());

    assertThat(dedicatedResponse.getAssignedPartitionIdsList()).containsExactly("3", "4");
    AtomicReference<DatasetMetadata> dedicatedDatasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              dedicatedDatasetMetadata.set(datasetMetadataStore.getSync(dedicatedDatasetName));
              return dedicatedDatasetMetadata.get().isUsingDedicatedPartitions()
                  && dedicatedDatasetMetadata.get().getActivePartitionMetadata().isPresent();
            });
    assertThat(dedicatedDatasetMetadata.get().getActivePerPartitionThroughput()).isEqualTo(75);

    ManagerApi.ListPartitionMetadataResponse listPartitionResponse =
        managerApiStub.listPartitionMetadata(
            ManagerApi.ListPartitionRequest.newBuilder().build());
    Map<String, ManagerApi.LivePartitionState> partitionsById =
        livePartitionStateById(listPartitionResponse);
    assertThat(partitionsById.get("3").getDedicated().getDataset()).isEqualTo(dedicatedDatasetName);
    assertThat(partitionsById.get("4").getDedicated().getDataset()).isEqualTo(dedicatedDatasetName);
  }

  @Test
  public void shouldRejectDedicatedAutoAssignmentWhenEqualShareWouldOverloadPartition() {
    String datasetName = "dedicatedPooledCapacityDataset";
    createPartition("1", 1);
    createPartition("2", 100);
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(101)
                            .setRequireDedicatedPartition(true)
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Needed 2 partitions with enough capacity")
        .contains("found 1: [2]");
  }

  @Test
  public void shouldUseMoreDedicatedPartitionsWhenEqualShareNeedsCapacity() {
    String datasetName = "dedicatedEqualShareDataset";
    createPartition("1", 50);
    createPartition("2", 55);
    createPartition("3", 70);
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    ManagerApi.UpdatePartitionAssignmentResponse response =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(120)
                .setRequireDedicatedPartition(true)
                .build());

    assertThat(response.getAssignedPartitionIdsList()).containsExactly("1", "2", "3");
    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().isUsingDedicatedPartitions()
                  && datasetMetadata.get().getActivePartitionMetadata().isPresent();
            });
    assertThat(datasetMetadata.get().getActivePerPartitionThroughput()).isEqualTo(40);
  }

  @Test
  public void shouldReturnFailedPreconditionWhenDedicatedOwnershipConflicts() {
    createPartition("1", 100);
    datasetMetadataStore.createSync(
        new DatasetMetadata(
            "dataset-a",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, MAX_TIME, List.of("1"))),
            "",
            true));
    datasetMetadataStore.createSync(
        new DatasetMetadata(
            "dataset-b",
            "owner",
            100,
            List.of(new DatasetPartitionMetadata(1, MAX_TIME, List.of("1"))),
            "",
            true));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.listPartitionMetadata(
                        ManagerApi.ListPartitionRequest.newBuilder().build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("partition 1 cannot be dedicated to multiple datasets");
  }

  @Test
  public void shouldSerializeConcurrentAutoPartitionAssignments() throws Exception {
    String firstDatasetName = "serializedAutoDatasetA";
    String secondDatasetName = "serializedAutoDatasetB";
    createPartitions(List.of("1", "2", "3", "4"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(firstDatasetName)
            .setOwner("owner")
            .build());
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(secondDatasetName)
            .setOwner("owner")
            .build());

    CountDownLatch firstDatasetUpdateStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstDatasetUpdate = new CountDownLatch(1);
    CountDownLatch secondDatasetUpdateStarted = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              DatasetMetadata updatedDatasetMetadata = invocation.getArgument(0);
              if (updatedDatasetMetadata.getName().equals(firstDatasetName)) {
                firstDatasetUpdateStarted.countDown();
                if (!releaseFirstDatasetUpdate.await(5, TimeUnit.SECONDS)) {
                  throw new AssertionError(
                      "Timed out waiting to release first partition assignment update");
                }
              }
              if (updatedDatasetMetadata.getName().equals(secondDatasetName)) {
                secondDatasetUpdateStarted.countDown();
              }
              return invocation.callRealMethod();
            })
        .when(datasetMetadataStore)
        .updateSync(any(DatasetMetadata.class));

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      Future<ManagerApi.UpdatePartitionAssignmentResponse> firstAssignment =
          executorService.submit(
              () ->
                  managerApiStub.updatePartitionAssignment(
                      ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                          .setName(firstDatasetName)
                          .setThroughputBytes(100)
                          .setRequireDedicatedPartition(true)
                          .build()));
      assertThat(firstDatasetUpdateStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<ManagerApi.UpdatePartitionAssignmentResponse> secondAssignment =
          executorService.submit(
              () ->
                  managerApiStub.updatePartitionAssignment(
                      ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                          .setName(secondDatasetName)
                          .setThroughputBytes(100)
                          .setRequireDedicatedPartition(true)
                          .build()));

      await()
          .untilAsserted(
              () -> {
                verify(datasetMetadataStore, times(1)).updateSync(any(DatasetMetadata.class));
                verify(datasetMetadataStore)
                    .updateSync(argThat(dataset -> dataset.getName().equals(firstDatasetName)));
                verify(datasetMetadataStore, times(0))
                    .updateSync(argThat(dataset -> dataset.getName().equals(secondDatasetName)));
              });
      releaseFirstDatasetUpdate.countDown();

      assertThat(firstAssignment.get(5, TimeUnit.SECONDS).getAssignedPartitionIdsList())
          .containsExactly("1", "2");
      assertThat(secondDatasetUpdateStarted.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(secondAssignment.get(5, TimeUnit.SECONDS).getAssignedPartitionIdsList())
          .hasSize(2)
          .allMatch(partitionId -> List.of("1", "2", "3", "4").contains(partitionId));
    } finally {
      releaseFirstDatasetUpdate.countDown();
      executorService.shutdownNow();
    }
  }

  @Test
  public void shouldNotSerializeConcurrentAutoAssignmentsAcrossManagerInstances() throws Exception {
    // Current behavior is intentionally asserted here: synchronized manager RPCs only serialize
    // updates within one ManagerApiGrpc instance. If cross-process coordination is added later,
    // this test should be updated to reflect the new behavior.
    ManagerApiServiceGrpc.ManagerApiServiceBlockingStub secondManagerApiStub =
        createManagerApiStub(nextServerName("secondary-manager"), 2);
    String firstDatasetName = "crossManagerDatasetA";
    String secondDatasetName = "crossManagerDatasetB";
    createPartitions(List.of("1", "2", "3", "4"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(firstDatasetName)
            .setOwner("owner")
            .build());
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(secondDatasetName)
            .setOwner("owner")
            .build());

    CountDownLatch firstDatasetUpdateStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstDatasetUpdate = new CountDownLatch(1);
    CountDownLatch secondDatasetUpdateStarted = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              DatasetMetadata updatedDatasetMetadata = invocation.getArgument(0);
              if (updatedDatasetMetadata.getName().equals(firstDatasetName)) {
                firstDatasetUpdateStarted.countDown();
                if (!releaseFirstDatasetUpdate.await(5, TimeUnit.SECONDS)) {
                  throw new AssertionError(
                      "Timed out waiting to release first cross-manager update");
                }
              }
              if (updatedDatasetMetadata.getName().equals(secondDatasetName)) {
                secondDatasetUpdateStarted.countDown();
              }
              return invocation.callRealMethod();
            })
        .when(datasetMetadataStore)
        .updateSync(any(DatasetMetadata.class));

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      Future<ManagerApi.UpdatePartitionAssignmentResponse> firstAssignment =
          executorService.submit(
              () ->
                  managerApiStub.updatePartitionAssignment(
                      ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                          .setName(firstDatasetName)
                          .setThroughputBytes(100)
                          .setRequireDedicatedPartition(true)
                          .build()));
      assertThat(firstDatasetUpdateStarted.await(5, TimeUnit.SECONDS)).isTrue();

      Future<ManagerApi.UpdatePartitionAssignmentResponse> secondAssignment =
          executorService.submit(
              () ->
                  secondManagerApiStub.updatePartitionAssignment(
                      ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                          .setName(secondDatasetName)
                          .setThroughputBytes(100)
                          .setRequireDedicatedPartition(true)
                          .build()));

      assertThat(secondDatasetUpdateStarted.await(5, TimeUnit.SECONDS)).isTrue();
      releaseFirstDatasetUpdate.countDown();

      assertThat(firstAssignment.get(5, TimeUnit.SECONDS).getAssignedPartitionIdsList())
          .containsExactly("1", "2");
      assertThat(secondAssignment.get(5, TimeUnit.SECONDS).getAssignedPartitionIdsList())
          .containsExactly("1", "2");
    } finally {
      releaseFirstDatasetUpdate.countDown();
      executorService.shutdownNow();
    }
  }

  @Test
  public void shouldPreserveAndExplicitlyClearDedicatedPartitionRequirement() {
    String datasetName = "preserveDedicatedRequirementDataset";
    createPartitions(List.of("1", "2", "3"));
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    ManagerApi.UpdatePartitionAssignmentResponse initialResponse =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(100)
                .setRequireDedicatedPartition(true)
                .build());
    assertThat(initialResponse.getAssignedPartitionIdsList()).containsExactly("1", "2");

    AtomicReference<DatasetMetadata> datasetMetadata = new AtomicReference<>();
    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().isUsingDedicatedPartitions()
                  && datasetMetadata.get().getActivePartitionMetadata().isPresent();
            });

    ManagerApi.UpdatePartitionAssignmentResponse preservedResponse =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(80)
                .build());
    assertThat(preservedResponse.getAssignedPartitionIdsList()).containsExactly("1", "2");

    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return datasetMetadata.get().getThroughputBytes() == 80
                  && datasetMetadata.get().isUsingDedicatedPartitions();
            });
    assertThat(datasetMetadata.get().getActivePartitionMetadata().orElseThrow().getPartitions())
        .containsExactly("1", "2");

    ManagerApi.UpdatePartitionAssignmentResponse clearedResponse =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(datasetName)
                .setThroughputBytes(80)
                .setRequireDedicatedPartition(false)
                .build());
    assertThat(clearedResponse.getAssignedPartitionIdsList()).containsExactly("1", "2");

    await()
        .until(
            () -> {
              datasetMetadata.set(datasetMetadataStore.getSync(datasetName));
              return !datasetMetadata.get().isUsingDedicatedPartitions();
            });
  }

  @Test
  public void shouldNotAutoAssignSharedDatasetToOtherDatasetDedicatedPartitions() {
    String dedicatedDatasetName = "payments";
    String sharedDatasetName = "search";
    createPartitions(List.of("1", "2", "3", "4"));

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(dedicatedDatasetName)
            .setOwner("payments-team")
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(dedicatedDatasetName)
            .setThroughputBytes(150)
            .setRequireDedicatedPartition(true)
            .build());
    await()
        .until(
            () -> datasetMetadataStore.getSync(dedicatedDatasetName).isUsingDedicatedPartitions());

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(sharedDatasetName)
            .setOwner("search-team")
            .build());
    ManagerApi.UpdatePartitionAssignmentResponse sharedResponse =
        managerApiStub.updatePartitionAssignment(
            ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                .setName(sharedDatasetName)
                .setThroughputBytes(100)
                .build());

    assertThat(sharedResponse.getAssignedPartitionIdsList()).containsExactly("3", "4");
  }

  @Test
  public void shouldReturnFailedPreconditionWhenAutoAssignmentLacksPartitions() {
    String datasetName = "notEnoughPartitionsDataset";
    createPartition("1", 100);
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(50)
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("proposal not enough candidate partitions: needed 2, available 1: [1]");
  }

  @Test
  public void shouldRejectDeletingReferencedPartition() {
    String datasetName = "referencedPartitionDataset";
    createPartition("7", 100);
    createPartition("8", 100);
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetName)
            .setThroughputBytes(50)
            .addAllPartitionIds(List.of("7", "8"))
            .build());
    await()
        .until(
            () ->
                datasetMetadataStore
                    .getSync(datasetName)
                    .getActivePartitionMetadata()
                    .map(partitionMetadata -> partitionMetadata.getPartitions().contains("7"))
                    .orElse(false));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deletePartition(
                        ManagerApi.DeletePartitionRequest.newBuilder()
                            .setPartitionId("7")
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Partition with id '7' is still referenced");
  }

  @Test
  public void shouldRejectDeletingPartitionWithNonNumericId() {
    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deletePartition(
                        ManagerApi.DeletePartitionRequest.newBuilder()
                            .setPartitionId("partition-a")
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Partition ID must be numeric: partition-a");
  }

  @Test
  public void shouldErrorUpdatingPartitionAssignmentNonexistentDataset() {
    String datasetName = "testDataset";
    List<String> partitionList = List.of("1", "2");

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(-1)
                            .addAllPartitionIds(partitionList)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldListExistingDatasets() {
    String datasetName1 = "testDataset1";
    String datasetOwner1 = "testOwner1";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName1)
            .setOwner(datasetOwner1)
            .build());

    String datasetName2 = "testDataset2";
    String datasetOwner2 = "testOwner2";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName2)
            .setOwner(datasetOwner2)
            .build());

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());

    assertThat(

  @Test
  public void shouldRejectManualAssignmentWithNonNumericPartitionIds() {
    String datasetName = "manualNonNumericPartitionDataset";
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    createPartition("1", 100);

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(50)
                            .addAllPartitionIds(List.of("1", "partition-a"))
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("Requested partition IDs must be numeric: [partition-a]");
  }
        listDatasetMetadataResponse

  @Test
  public void shouldRejectNegativeThroughputValuesOtherThanPreserveSentinel() {
    String datasetName = "negativeThroughputDataset";
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetName)
            .setOwner("owner")
            .build());
    createPartitions(List.of("1", "2"));

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updatePartitionAssignment(
                        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
                            .setName(datasetName)
                            .setThroughputBytes(-2)
                            .addAllPartitionIds(List.of("1", "2"))
                            .build()));

    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.INVALID_ARGUMENT.getCode());
    assertThat(throwable.getStatus().getDescription())
        .contains("throughputBytes must be non-negative or the preserve sentinel (-1), got -2");
  }
            .getDatasetMetadataList()
            .containsAll(
                List.of(
                    Metadata.DatasetMetadata.newBuilder()
                        .setName(datasetName1)
                        .setOwner(datasetOwner1)
                        .setThroughputBytes(0)
                        .build(),
                    Metadata.DatasetMetadata.newBuilder()
                        .setName(datasetName2)
                        .setOwner(datasetOwner2)
                        .setThroughputBytes(0)
                        .build())));

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(2);
    assertThat(
        AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore)
            .containsAll(
                List.of(
                    new DatasetMetadata(
                        datasetName1, datasetOwner1, 0, Collections.emptyList(), datasetName1),
                    new DatasetMetadata(
                        datasetName2, datasetOwner2, 0, Collections.emptyList(), datasetName2))));
  }

  @Test
  public void shouldDeleteDatasetWithMultipleDatasetsAndUnrelatedSnapshots() {
    String datasetNameToDelete = "datasetToDelete";
    String datasetNameWithSnapshots = "datasetWithSnapshots";
    String datasetNameWithoutPartitions = "datasetWithoutPartitions";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetNameToDelete)
            .setOwner("ownerDelete")
            .build());
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetNameWithSnapshots)
            .setOwner("ownerSnapshots")
            .build());
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetNameWithoutPartitions)
            .setOwner("ownerNoPartitions")
            .build());

    createPartitions(List.of("1", "2", "3", "4"));

    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetNameToDelete)
            .setThroughputBytes(100)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetNameWithSnapshots)
            .setThroughputBytes(200)
            .addAllPartitionIds(List.of("3", "4"))
            .build());

    long nowMs = Instant.now().toEpochMilli();
    snapshotMetadataStore.createSync(
        new SnapshotMetadata("snapshot-a", nowMs, nowMs + 1, 0, "3", 123));
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    Metadata.DatasetMetadata deletedDataset =
        managerApiStub.deleteDatasetMetadata(
            ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                .setName(datasetNameToDelete)
                .build());

    assertThat(deletedDataset.getName()).isEqualTo(datasetNameToDelete);

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());
    assertThat(listDatasetMetadataResponse.getDatasetMetadataList())
        .extracting(Metadata.DatasetMetadata::getName)
        .containsExactlyInAnyOrder(datasetNameWithSnapshots, datasetNameWithoutPartitions);
    assertThat(snapshotMetadataStore.listSync()).hasSize(1);

    StatusRuntimeException deletedDatasetLookupError =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.getDatasetMetadata(
                        ManagerApi.GetDatasetMetadataRequest.newBuilder()
                            .setName(datasetNameToDelete)
                            .build()));
    assertThat(deletedDatasetLookupError.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
  }

  @Test
  public void shouldReturnNotFoundDeletingNonexistentDataset() {
    String missingDatasetName = "missingDataset";
    String existingDatasetName = "existingDataset";
    String existingOwner = "existingOwner";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(existingDatasetName)
            .setOwner(existingOwner)
            .build());

    StatusRuntimeException deleteError =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deleteDatasetMetadata(
                        ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                            .setName(missingDatasetName)
                            .build()));

    assertThat(deleteError.getStatus().getCode()).isEqualTo(Status.NOT_FOUND.getCode());
    assertThat(deleteError.getStatus().getDescription())
        .isEqualTo("Dataset not found: " + missingDatasetName);

    Metadata.DatasetMetadata existingDataset =
        managerApiStub.getDatasetMetadata(
            ManagerApi.GetDatasetMetadataRequest.newBuilder().setName(existingDatasetName).build());
    assertThat(existingDataset.getOwner()).isEqualTo(existingOwner);

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());
    assertThat(listDatasetMetadataResponse.getDatasetMetadataList())
        .extracting(Metadata.DatasetMetadata::getName)
        .containsExactly(existingDatasetName);

    Metadata.DatasetMetadata deletedExistingDataset =
        managerApiStub.deleteDatasetMetadata(
            ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                .setName(existingDatasetName)
                .build());
    assertThat(deletedExistingDataset.getName()).isEqualTo(existingDatasetName);

    ManagerApi.ListDatasetMetadataResponse listAfterDeleteResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());
    assertThat(listAfterDeleteResponse.getDatasetMetadataList()).isEmpty();
  }

  @Test
  public void shouldReturnUnknownWhenDeleteExistenceCheckFails() {
    String datasetName = "datasetWithDeleteStoreError";
    String errorString = "deleteHasSyncError";

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .hasSync(eq(datasetName));

    StatusRuntimeException deleteError =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deleteDatasetMetadata(
                        ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .build()));

    assertThat(deleteError.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(deleteError.getStatus().getDescription()).isEqualTo(errorString);

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore)).isEmpty();
  }

  @Test
  public void shouldRejectDeletingDatasetWhenSnapshotsReferenceItsPartitions() {
    String datasetNameToDelete = "datasetWithReferencedPartitions";
    String otherDatasetName = "otherDataset";

    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(datasetNameToDelete)
            .setOwner("ownerDelete")
            .build());
    managerApiStub.createDatasetMetadata(
        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
            .setName(otherDatasetName)
            .setOwner("ownerOther")
            .build());

    createPartitions(List.of("1", "2", "3", "4"));

    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(datasetNameToDelete)
            .setThroughputBytes(200)
            .addAllPartitionIds(List.of("1", "2"))
            .build());
    managerApiStub.updatePartitionAssignment(
        ManagerApi.UpdatePartitionAssignmentRequest.newBuilder()
            .setName(otherDatasetName)
            .setThroughputBytes(200)
            .addAllPartitionIds(List.of("3", "4"))
            .build());

    long nowMs = Instant.now().toEpochMilli();
    snapshotMetadataStore.createSync(
        new SnapshotMetadata("snapshot-1", nowMs, nowMs + 1, 0, "1", 111));
    snapshotMetadataStore.createSync(
        new SnapshotMetadata("snapshot-2", nowMs, nowMs + 1, 0, "2", 222));
    snapshotMetadataStore.createSync(
        new SnapshotMetadata("snapshot-3", nowMs, nowMs + 1, 0, "3", 333));
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    StatusRuntimeException deleteDatasetError =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deleteDatasetMetadata(
                        ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                            .setName(datasetNameToDelete)
                            .build()));

    assertThat(deleteDatasetError.getStatus().getCode())
        .isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(deleteDatasetError.getStatus().getDescription())
        .contains("Cannot delete dataset '" + datasetNameToDelete + "'")
        .contains("2 snapshot(s) still reference its partitions");

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());
    assertThat(listDatasetMetadataResponse.getDatasetMetadataList())
        .extracting(Metadata.DatasetMetadata::getName)
        .containsExactlyInAnyOrder(datasetNameToDelete, otherDatasetName);
  }

  @Test
  public void shouldDeleteDatasetWhenSnapshotsOnlyMatchReassignedPartitionInLaterWindow() {
    // NOTE: Current behavior is intentionally asserted to keep CI green until the FIXME in
    // ManagerApiGrpc.calculateRequiredSnapshots is addressed in issue #82. Once that fix lands,
    // this test should be updated to expect successful deletion.
    String datasetNameToDelete = "datasetWithReassignedPartition";
    String otherDatasetName = "datasetWithCurrentPartitionOwner";
    String reassignedPartitionId = "shared-partition";

    datasetMetadataStore.createSync(
        new DatasetMetadata(
            datasetNameToDelete,
            "ownerDelete",
            300,
            List.of(new DatasetPartitionMetadata(1000, 2000, List.of(reassignedPartitionId))),
            datasetNameToDelete));
    datasetMetadataStore.createSync(
        new DatasetMetadata(
            otherDatasetName,
            "ownerOther",
            400,
            List.of(new DatasetPartitionMetadata(3000, MAX_TIME, List.of(reassignedPartitionId))),
            otherDatasetName));

    snapshotMetadataStore.createSync(
        new SnapshotMetadata("snapshot-1", 3500, 3600, 0, reassignedPartitionId, 111));
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    StatusRuntimeException deleteDatasetError =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.deleteDatasetMetadata(
                        ManagerApi.DeleteDatasetMetadataRequest.newBuilder()
                            .setName(datasetNameToDelete)
                            .build()));
    assertThat(deleteDatasetError.getStatus().getCode())
        .isEqualTo(Status.FAILED_PRECONDITION.getCode());
    assertThat(deleteDatasetError.getStatus().getDescription())
        .contains("Cannot delete dataset '" + datasetNameToDelete + "'")
        .contains("1 snapshot(s) still reference its partitions");

    ManagerApi.ListDatasetMetadataResponse listDatasetMetadataResponse =
        managerApiStub.listDatasetMetadata(
            ManagerApi.ListDatasetMetadataRequest.newBuilder().build());
    assertThat(listDatasetMetadataResponse.getDatasetMetadataList())
        .extracting(Metadata.DatasetMetadata::getName)
        .containsExactlyInAnyOrder(datasetNameToDelete, otherDatasetName);
  }

  @Test
  public void shouldHandleZkErrorsGracefully() {
    String datasetName = "testZkErrorsDataset";
    String datasetOwner = "testZkErrorsOwner";
    String errorString = "zkError";

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .createSync(
            eq(
                new DatasetMetadata(
                    datasetName, datasetOwner, 0L, Collections.emptyList(), datasetName)));

    StatusRuntimeException throwableCreate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createDatasetMetadata(
                        ManagerApi.CreateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner)
                            .setServiceNamePattern(datasetName)
                            .build()));

    assertThat(throwableCreate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableCreate.getStatus().getDescription()).isEqualTo(errorString);

    doThrow(new InternalMetadataStoreException(errorString))
        .when(datasetMetadataStore)
        .updateSync(
            eq(
                new DatasetMetadata(
                    datasetName, datasetOwner, 0L, Collections.emptyList(), datasetName)));

    StatusRuntimeException throwableUpdate =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.updateDatasetMetadata(
                        ManagerApi.UpdateDatasetMetadataRequest.newBuilder()
                            .setName(datasetName)
                            .setOwner(datasetOwner)
                            .build()));

    assertThat(throwableUpdate.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwableUpdate.getStatus().getDescription()).contains(datasetName);

    assertThat(AstraMetadataTestUtils.listSyncUncached(datasetMetadataStore).size()).isEqualTo(0);
  }

  @Test
  public void shouldFetchSnapshotsWithinTimeframeAndPartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata overlapsStartTimeIncluded =
        new SnapshotMetadata("a", startTime, startTime + 6, 0, "a", 0);
    SnapshotMetadata overlapsStartTimeExcluded =
        new SnapshotMetadata("b", startTime, startTime + 6, 0, "b", 0);

    SnapshotMetadata fullyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata("c", startTime + 4, startTime + 11, 0, "a", 0);
    SnapshotMetadata fullyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata("d", startTime + 4, startTime + 11, 0, "b", 0);

    SnapshotMetadata partiallyOverlapsStartEndTimeIncluded =
        new SnapshotMetadata("e", startTime + 4, startTime + 5, 0, "a", 0);
    SnapshotMetadata partiallyOverlapsStartEndTimeExcluded =
        new SnapshotMetadata("f", startTime + 4, startTime + 5, 0, "b", 0);

    SnapshotMetadata overlapsEndTimeIncluded =
        new SnapshotMetadata("g", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata overlapsEndTimeExcluded =
        new SnapshotMetadata("h", startTime + 10, startTime + 15, 0, "b", 0);

    SnapshotMetadata notWithinStartEndTimeExcluded1 =
        new SnapshotMetadata("i", startTime, startTime + 4, 0, "a", 0);
    SnapshotMetadata notWithinStartEndTimeExcluded2 =
        new SnapshotMetadata("j", startTime + 11, startTime + 15, 0, "a", 0);

    DatasetMetadata datasetWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(datasetWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);

    List<SnapshotMetadata> snapshotsWithData =
        ManagerApiGrpc.calculateRequiredSnapshots(
            Arrays.asList(
                overlapsEndTimeIncluded,
                overlapsEndTimeExcluded,
                partiallyOverlapsStartEndTimeIncluded,
                partiallyOverlapsStartEndTimeExcluded,
                fullyOverlapsStartEndTimeIncluded,
                fullyOverlapsStartEndTimeExcluded,
                overlapsStartTimeIncluded,
                overlapsStartTimeExcluded,
                notWithinStartEndTimeExcluded1,
                notWithinStartEndTimeExcluded2),
            datasetMetadataStore,
            start,
            end,
            "foo");

    assertThat(snapshotsWithData.size()).isEqualTo(4);
    assertThat(
            snapshotsWithData.containsAll(
                Arrays.asList(
                    overlapsStartTimeIncluded,
                    fullyOverlapsStartEndTimeIncluded,
                    partiallyOverlapsStartEndTimeIncluded,
                    overlapsEndTimeIncluded)))
        .isTrue();
  }

  @Test
  public void shouldResolveHistoricalSnapshotsWithoutPartitionCatalogEntries() {
    long queryStartTime = 1000;
    long queryEndTime = 2000;
    String datasetName = "historicalDataset";
    String historicalPartitionId = "42";
    SnapshotMetadata historicalSnapshot =
        new SnapshotMetadata("historical-snapshot", 1200, 1300, 0, historicalPartitionId, 111);

    datasetMetadataStore.createSync(
        new DatasetMetadata(
            datasetName,
            "owner",
            100,
            List.of(
                new DatasetPartitionMetadata(
                    queryStartTime, MAX_TIME, List.of(historicalPartitionId))),
            datasetName));
    snapshotMetadataStore.createSync(historicalSnapshot);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);
    assertThat(partitionMetadataStore.listSync()).isEmpty();

    List<SnapshotMetadata> snapshotsWithData =
        ManagerApiGrpc.calculateRequiredSnapshots(
            snapshotMetadataStore.listSync(),
            datasetMetadataStore,
            queryStartTime,
            queryEndTime,
            datasetName);

    assertThat(snapshotsWithData).containsExactly(historicalSnapshot);
  }

  @Test
  public void shouldRestoreReplicaSinglePartition() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata snapshotIncluded =
        new SnapshotMetadata("g", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata("h", startTime + 10, startTime + 15, 0, "b", 0);

    snapshotMetadataStore.createSync(snapshotIncluded);
    snapshotMetadataStore.createSync(snapshotExcluded);

    DatasetMetadata serviceWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a"))),
            "fooService");

    datasetMetadataStore.createSync(serviceWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 2);

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await()
        .until(
            () -> MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry) == 1);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);
  }

  @Test
  public void shouldRestoreReplicasMultiplePartitions() {
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    SnapshotMetadata snapshotIncluded =
        new SnapshotMetadata("a", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotIncluded2 =
        new SnapshotMetadata("b", startTime + 10, startTime + 15, 0, "b", 0);
    SnapshotMetadata snapshotExcluded =
        new SnapshotMetadata("c", startTime + 10, startTime + 15, 0, "c", 0);

    snapshotMetadataStore.createSync(snapshotIncluded);
    snapshotMetadataStore.createSync(snapshotIncluded2);
    snapshotMetadataStore.createSync(snapshotExcluded);

    DatasetMetadata serviceWithDataInPartitionA =
        new DatasetMetadata(
            "foo",
            "a",
            1,
            List.of(new DatasetPartitionMetadata(startTime + 5, startTime + 6, List.of("a", "b"))),
            "fooService");

    datasetMetadataStore.createSync(serviceWithDataInPartitionA);

    await().until(() -> datasetMetadataStore.listSync().size() == 1);
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplica(
        ManagerApi.RestoreReplicaRequest.newBuilder()
            .setServiceName("foo")
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(2);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }

  @Test
  public void shouldRestoreGivenSnapshotIds() {
    long startTime = Instant.now().toEpochMilli();

    SnapshotMetadata snapshotFoo =
        new SnapshotMetadata("foo", startTime + 10, startTime + 15, 0, "a", 0);
    SnapshotMetadata snapshotBar =
        new SnapshotMetadata("bar", startTime + 10, startTime + 15, 0, "b", 0);
    SnapshotMetadata snapshotBaz =
        new SnapshotMetadata("baz", startTime + 10, startTime + 15, 0, "c", 0);

    snapshotMetadataStore.createSync(snapshotFoo);
    snapshotMetadataStore.createSync(snapshotBar);
    snapshotMetadataStore.createSync(snapshotBaz);
    await().until(() -> snapshotMetadataStore.listSync().size() == 3);

    replicaRestoreService.startAsync();
    replicaRestoreService.awaitRunning();

    managerApiStub.restoreReplicaIds(
        ManagerApi.RestoreReplicaIdsRequest.newBuilder()
            .addAllIdsToRestore(List.of("foo", "bar", "baz"))
            .build());

    await().until(() -> replicaMetadataStore.listSync().size() == 3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(3);
    assertThat(MetricsUtil.getCount(ReplicaRestoreService.REPLICAS_FAILED, meterRegistry))
        .isEqualTo(0);

    replicaRestoreService.stopAsync();
  }

  @Test
  public void shouldCreateAndGetNewFieldRedaction() {
    String redactionName = "testRedaction";
    String fieldName = "testfieldName";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName)
            .setFieldName(fieldName)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    Metadata.RedactedFieldMetadata getRedactedFieldResponse =
        managerApiStub.getFieldRedaction(
            ManagerApi.GetFieldRedactionRequest.newBuilder().setName(redactionName).build());
    assertThat(getRedactedFieldResponse.getName()).isEqualTo(redactionName);
    assertThat(getRedactedFieldResponse.getFieldName()).isEqualTo(fieldName);
    assertThat(getRedactedFieldResponse.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(getRedactedFieldResponse.getEndTimeEpochMs()).isEqualTo(end);

    FieldRedactionMetadata fieldRedactionMetadata =
        fieldRedactionMetadataStore.getSync(redactionName);
    assertThat(fieldRedactionMetadata.getName()).isEqualTo(redactionName);
    assertThat(fieldRedactionMetadata.getFieldName()).isEqualTo(fieldName);
    assertThat(fieldRedactionMetadata.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(fieldRedactionMetadata.getEndTimeEpochMs()).isEqualTo(end);
  }

  @Test
  public void shouldListExistingFieldRedactions() {
    String redactionName1 = "testFieldRedaction1";
    String field1 = "testField1";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName1)
            .setFieldName(field1)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    String redactionName2 = "testFieldRedaction2";
    String field2 = "testField2";

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName2)
            .setFieldName(field2)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    ManagerApi.ListFieldRedactionsResponse listFieldRedactionsResponse =
        managerApiStub.listFieldRedactions(
            ManagerApi.ListFieldRedactionsRequest.newBuilder().build());

    assertThat(
        listFieldRedactionsResponse
            .getRedactedFieldsList()
            .containsAll(
                List.of(
                    Metadata.RedactedFieldMetadata.newBuilder()
                        .setName(redactionName1)
                        .setFieldName(field1)
                        .setStartTimeEpochMs(start)
                        .setEndTimeEpochMs(end)
                        .build(),
                    Metadata.RedactedFieldMetadata.newBuilder()
                        .setName(redactionName2)
                        .setFieldName(field2)
                        .setStartTimeEpochMs(start)
                        .setEndTimeEpochMs(end)
                        .build())));

    assertThat(AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size())
        .isEqualTo(2);
    assertThat(
        AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore)
            .containsAll(
                List.of(
                    new FieldRedactionMetadata(redactionName1, field1, start, end),
                    new FieldRedactionMetadata(redactionName2, field2, start, end))));
  }

  @Test
  public void shouldDeleteExistingFieldRedaction() {
    String redactionName = "testRedaction";
    String fieldName = "testFieldName";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName)
            .setFieldName(fieldName)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    Metadata.RedactedFieldMetadata deleteRedactedFieldResponse =
        managerApiStub.deleteFieldRedaction(
            ManagerApi.DeleteFieldRedactionRequest.newBuilder().setName(redactionName).build());
    assertThat(deleteRedactedFieldResponse.getName()).isEqualTo(redactionName);
    assertThat(deleteRedactedFieldResponse.getFieldName()).isEqualTo(fieldName);
    assertThat(deleteRedactedFieldResponse.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(deleteRedactedFieldResponse.getEndTimeEpochMs()).isEqualTo(end);

    // TODO: Temp fix. Fails in CI. Possible race condition.
    await()
        .untilAsserted(
            () -> assertThat(fieldRedactionMetadataStore.hasSync(redactionName)).isFalse());
  }

  @Test
  public void shouldErrorCreatingDuplicateFieldRedactionName() {
    String redactionName = "testFieldRedaction";
    String fieldName1 = "fieldName1";
    String fieldName2 = "fieldName2";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    managerApiStub.createFieldRedaction(
        ManagerApi.CreateFieldRedactionRequest.newBuilder()
            .setName(redactionName)
            .setFieldName(fieldName1)
            .setStartTimeEpochMs(start)
            .setEndTimeEpochMs(end)
            .build());

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName(redactionName)
                            .setFieldName(fieldName2)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    FieldRedactionMetadata fieldRedactionMetadata =
        fieldRedactionMetadataStore.getSync(redactionName);
    assertThat(fieldRedactionMetadata.getName()).isEqualTo(redactionName);
    assertThat(fieldRedactionMetadata.getFieldName()).isEqualTo(fieldName1);
    assertThat(fieldRedactionMetadata.getStartTimeEpochMs()).isEqualTo(start);
    assertThat(fieldRedactionMetadata.getEndTimeEpochMs()).isEqualTo(end);
  }

  @Test
  public void shouldErrorCreatingWithInvalidRedactionNames() {
    String fieldName = "testFieldName";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    StatusRuntimeException throwable1 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName("")
                            .setFieldName(fieldName)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable1.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable1.getStatus().getDescription()).isEqualTo("name can't be null or empty.");

    StatusRuntimeException throwable2 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName("/")
                            .setFieldName(fieldName)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable2.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    StatusRuntimeException throwable3 =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName(".")
                            .setFieldName(fieldName)
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable3.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());

    assertThat(AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size())
        .isEqualTo(0);
  }

  @Test
  public void shouldErrorWithEmptyFieldName() {
    String redactionName = "testRedaction";
    long startTime = Instant.now().toEpochMilli();
    long start = startTime + 5;
    long end = startTime + 10;

    StatusRuntimeException throwable =
        (StatusRuntimeException)
            catchThrowable(
                () ->
                    managerApiStub.createFieldRedaction(
                        ManagerApi.CreateFieldRedactionRequest.newBuilder()
                            .setName(redactionName)
                            .setFieldName("")
                            .setStartTimeEpochMs(start)
                            .setEndTimeEpochMs(end)
                            .build()));
    assertThat(throwable.getStatus().getCode()).isEqualTo(Status.UNKNOWN.getCode());
    assertThat(throwable.getStatus().getDescription()).isEqualTo("field name cannot be null");

    assertThat(AstraMetadataTestUtils.listSyncUncached(fieldRedactionMetadataStore).size())
        .isEqualTo(0);
  }

  private static Map<String, ManagerApi.LivePartitionState> livePartitionStateById(
      ManagerApi.ListPartitionMetadataResponse response) {
    return response.getPartitionMetadataList().stream()
        .collect(
            Collectors.toMap(ManagerApi.LivePartitionState::getPartitionId, Function.identity()));
  }
}
