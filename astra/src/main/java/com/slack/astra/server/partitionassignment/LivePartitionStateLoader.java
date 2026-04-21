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
    return LivePartitionState.fromMetadata(
        datasetMetadataStore.listFreshSync(), partitionMetadataStore.listFreshSync());
  }
}
