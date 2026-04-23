package com.slack.astra.server.partitionassignment;

import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.partition.PartitionMetadataStore;
import java.util.List;

/** Loads the current live partition state from persisted dataset and partition metadata. */
public class LivePartitionStateLoader {
  private final DatasetMetadataStore datasetMetadataStore;
  private final PartitionMetadataStore partitionMetadataStore;

  public LivePartitionStateLoader(
      DatasetMetadataStore datasetMetadataStore, PartitionMetadataStore partitionMetadataStore) {
    this.datasetMetadataStore = datasetMetadataStore;
    this.partitionMetadataStore = partitionMetadataStore;
  }

  public List<LivePartitionState> loadAll() {
    // TODO(shard-autoassignment): listSync() is cache-backed in the ZooKeeper path. Rapid
    // back-to-back manager updates can therefore compute placement or render ListPartition from a
    // stale global view even though manager RPC entrypoints are synchronized. Revisit whether this
    // path needs a direct read or stronger coordination for correctness-sensitive callers.
    return LivePartitionState.fromMetadata(
        datasetMetadataStore.listSync(), partitionMetadataStore.listSync());
  }
}
