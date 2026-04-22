package com.slack.astra.metadata.partition;

import com.google.common.collect.ImmutableSet;
import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.ZookeeperMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Set;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;

public class PartitionMetadataStore extends AstraMetadataStore<PartitionMetadata> {
  public static final String PARTITION_MAP_METADATA_STORE_ZK_PATH = "/partition_map";

  public PartitionMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.MetadataStoreConfig metadataStoreConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) {
    super(
        new ZookeeperMetadataStore<>(
            curator,
            metadataStoreConfig.getZookeeperConfig(),
            CreateMode.PERSISTENT,
            shouldCache,
            new PartitionMetadataSerializer().toModelSerializer(),
            PARTITION_MAP_METADATA_STORE_ZK_PATH,
            meterRegistry),
        null,
        metadataStoreConfig.getMode(),
        meterRegistry);
  }

  public Set<String> listPartitionIdsSync() {
    return listSync().stream()
        .map(PartitionMetadata::getPartitionID)
        .collect(ImmutableSet.toImmutableSet());
  }
}
