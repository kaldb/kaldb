package com.slack.astra.logstore.opensearch;

import static org.opensearch.common.settings.IndexScopedSettings.BUILT_IN_INDEX_SETTINGS;

import com.slack.astra.logstore.LogMessage;
import java.util.HashSet;
import java.util.UUID;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;

final class AstraIndexSettings {
  // we can make this configurable when SchemaAwareLogDocumentBuilderImpl enforces a limit
  // set this to a high number for now
  private static final int TOTAL_FIELDS_LIMIT =
      Integer.parseInt(System.getProperty("astra.mapping.totalFieldsLimit", "2500"));
  private static final Settings INDEX_SETTINGS_TEMPLATE = buildIndexSettingsTemplate();
  private static final Settings NODE_SETTINGS =
      Settings.builder().put("indices.query.query_string.analyze_wildcard", true).build();
  private static final Settings SHARED_SERVICE_SETTINGS =
      Settings.builder().put(NODE_SETTINGS).put(INDEX_SETTINGS_TEMPLATE).build();

  /** Returns shared OpenSearch configuration without a synthetic index identity. */
  static Settings getSharedServiceSettings() {
    return SHARED_SERVICE_SETTINGS;
  }

  /** Creates settings with a distinct synthetic index identity for adapter cache ownership. */
  static IndexSettings create() {
    Settings settings =
        Settings.builder()
            .put(INDEX_SETTINGS_TEMPLATE)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUID.randomUUID().toString())
            .build();
    IndexScopedSettings indexScopedSettings =
        new IndexScopedSettings(settings, new HashSet<>(BUILT_IN_INDEX_SETTINGS));

    return new IndexSettings(
        IndexMetadata.builder("index").settings(settings).build(),
        NODE_SETTINGS,
        indexScopedSettings);
  }

  /** Builds the index-scoped configuration template for each synthetic adapter index. */
  private static Settings buildIndexSettingsTemplate() {
    return Settings.builder()
        .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_2_11_0)
        .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), TOTAL_FIELDS_LIMIT)

        // Astra time sorts the indexes while building it
        // {LuceneIndexStoreImpl#buildIndexWriterConfig}
        // When we were using the lucene query parser the sort info was leveraged by lucene
        // automatically ( as the sort info persists in the segment info ) at query time.
        // However the OpenSearch query parser has a custom implementation which relies on the
        // index sort info to be present as a setting here.
        .put("index.sort.field", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
        .put("index.sort.order", "desc")
        .put("index.query.default_field", LogMessage.SystemField.ALL.fieldName)
        .put("index.query_string.lenient", false)
        .build();
  }

  private AstraIndexSettings() {}
}
