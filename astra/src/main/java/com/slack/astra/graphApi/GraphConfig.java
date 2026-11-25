package com.slack.astra.graphApi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GraphConfig {
  private static final Logger LOG = LoggerFactory.getLogger(GraphConfig.class);

  public static final GraphConfig DEFAULT = new GraphConfig(Map.of(), Map.of());

  public enum EntityType {
    NODE,
    EDGE
  }

  /**
   * Represents how a single logical field on a node should be mapped to span tags.
   *
   * <p>Each field has: - default key (can be a list) to look up in tags (combined with delimiter if
   * multiple) - a default fallback value if the keys aren't found - an optional delimiter for
   * combining multiple key values - an optional list of rules that can override the default key
   */
  public static final class TagConfig {
    private final List<String> defaultKey;
    private final String defaultValue;
    private final String keyDelimiter;
    private final List<RuleConfig> rules;

    @JsonCreator
    public TagConfig(
        @JsonProperty("default_key") List<String> defaultKey,
        @JsonProperty("default_value") String defaultValue,
        @JsonProperty("key_delimiter") String keyDelimiter,
        @JsonProperty("rules") List<RuleConfig> rules) {
      this.defaultKey = (defaultKey == null) ? Collections.emptyList() : List.copyOf(defaultKey);
      this.defaultValue = defaultValue;
      // Set default keyDelimiter to "." if null or empty
      this.keyDelimiter = (keyDelimiter == null || keyDelimiter.isEmpty()) ? "." : keyDelimiter;
      this.rules = (rules == null) ? Collections.emptyList() : List.copyOf(rules);
    }

    public List<String> getDefaultKey() {
      return defaultKey;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    public String getKeyDelimiter() {
      return keyDelimiter;
    }

    public List<RuleConfig> getRules() {
      return rules;
    }
  }

  /**
   * Represents a conditional rule for overriding which tag key to use. Note: This logic does not
   * currently support multiple field matches under a single rule.
   */
  public static class RuleConfig {
    private final String field;
    private final String value;
    private final List<String> overrideKey;

    @JsonCreator
    public RuleConfig(
        @JsonProperty("field") String field,
        @JsonProperty("value") String value,
        @JsonProperty("override_key") List<String> overrideKey) {
      this.field = field;
      this.value = value;
      this.overrideKey = (overrideKey == null) ? Collections.emptyList() : List.copyOf(overrideKey);
    }

    public String getField() {
      return field;
    }

    public String getValue() {
      return value;
    }

    public List<String> getOverrideKey() {
      return overrideKey;
    }
  }

  // Holds the entire mapping for logical field names to their configuration of defaults and rules
  // for nodes.
  private final Map<String, TagConfig> nodeMetadataTagMapping;
  // Holds the entire mapping for logical field names to their configuration of defaults and rules
  // for nodes.
  private final Map<String, TagConfig> edgeMetadataTagMapping;

  @JsonCreator
  public GraphConfig(
      @JsonProperty("node_metadata_tag_mapping") Map<String, TagConfig> nodeMetadataTagMapping,
      @JsonProperty("edge_metadata_tag_mapping") Map<String, TagConfig> edgeMetadataTagMapping) {
    this.nodeMetadataTagMapping =
        (nodeMetadataTagMapping == null)
            ? Collections.emptyMap()
            : Map.copyOf(nodeMetadataTagMapping);
    this.edgeMetadataTagMapping =
        (edgeMetadataTagMapping == null)
            ? Collections.emptyMap()
            : Map.copyOf(edgeMetadataTagMapping);
  }

  public Map<String, TagConfig> getNodeMetadataTagMapping() {
    return nodeMetadataTagMapping;
  }

  public Map<String, TagConfig> getEdgeMetadataTagMapping() {
    return edgeMetadataTagMapping;
  }

  /**
   * Loads a GraphConfig from a YAML file on disk. On failure, log and return a null config.
   *
   * @param filePath Path of the config to load
   * @return GraphConfig containing mappings of node metadata fields -> corresponding span tags.
   */
  public static GraphConfig load(Path filePath) throws IOException {
    try {
      String yaml = Files.readString(filePath);
      return load(yaml);
    } catch (Exception e) {
      LOG.warn(
          "Failed to read dependency graph config from file path. Returning default config", e);
    }

    return DEFAULT;
  }

  /**
   * Loads a GraphConfig from a YAML string. On failure, log and return a null config.
   *
   * @param configYAML YAML string of config contents.
   * @return GraphConfig containing mappings of node metadata fields -> corresponding span tags.
   */
  public static GraphConfig load(String configYAML) throws IOException {
    if (!configYAML.isEmpty()) {
      try {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        return mapper.readValue(configYAML, GraphConfig.class);
      } catch (Exception e) {
        LOG.warn(
            "Failed to parse dependency graph config file contents. Returning default config", e);
      }
    }

    return DEFAULT;
  }

  /**
   * Creates metadata for a graph entity (node or edge) from a span, using either default behavior
   * or configured mapping.
   *
   * @param span ZipkinSpanResponse containing the span data
   * @return SortedMap containing the metadata for the requested entity
   */
  public SortedMap<String, String> createMetadataFromSpan(
      ZipkinSpanResponse span, EntityType entityType) {
    SortedMap<String, String> metadata = new TreeMap<>();

    if (this == DEFAULT) {
      // We only care about default behavior for a node, edge metadata is optional.
      if (entityType == EntityType.NODE) {
        // use service name from remote endpoint
        metadata.put("service", span.getRemoteEndpoint().getServiceName());
      }
    } else {
      Map<String, String> tags = span.getTags();

      // Use configured tag mapping
      Set<String> keys =
          switch (entityType) {
            case EDGE -> this.edgeMetadataTagMapping.keySet();
            case NODE -> this.nodeMetadataTagMapping.keySet();
          };

      for (String key : keys) {
        metadata.put(key, resolve(tags, key, entityType));
      }
    }

    return metadata;
  }

  /**
   * Resolves the actual tag value for a given logical field, using the provided span tags.
   *
   * <p>Steps: 1. Look up the TagConfig for this logical field (e.g. "resource"). 2. Check if any
   * rules match: - Iterate through each rule in reverse order. - If a rule's field/value condition
   * matches, try to resolve using the overrideKey. - If the resolved value is non-empty, use it. -
   * Otherwise, continue to the next matching rule. 3. If no rule produces a non-empty value, use
   * the defaultKey: - Look up each keyPart from defaultKey list in the tags map. - Combine the
   * values with the delimiter if multiple parts are present. - If any part is missing or empty,
   * fall back to defaultValue.
   *
   * <p>Note: This logic does not currently support multiple field matches for a single rule.
   *
   * @param tags Map of tags from the span.
   * @param logicalField the node metadata field to resolve.
   * @return String the value of the logical metadata field after applying all GraphConfig rules.
   */
  public String resolve(Map<String, String> tags, String logicalField, EntityType entityType) {
    TagConfig baseCfg =
        switch (entityType) {
          case EDGE -> this.edgeMetadataTagMapping.get(logicalField);
          case NODE -> this.nodeMetadataTagMapping.get(logicalField);
        };

    // If the config doesn't define this logical field, just return from raw tags or fallback.
    if (baseCfg == null) {
      return tags.getOrDefault(logicalField, "unknown_" + logicalField);
    }

    // Later rules override earlier ones, so start from the back of the list.
    // Try each matching rule until one produces a non-empty value.
    for (RuleConfig rule : baseCfg.getRules().reversed()) {
      if (rule.getValue()
          .equals(tags.getOrDefault(rule.getField(), "unknown_" + rule.getField()))) {
        String resolved = resolveKeys(tags, rule.getOverrideKey(), baseCfg.getKeyDelimiter());
        if (resolved != null && !resolved.isEmpty()) {
          return resolved;
        }
        // Continue to next matching rule if value is empty
      }
    }

    // No rules or rules produced a non-empty value, try the default key
    String defaultResolved = resolveKeys(tags, baseCfg.getDefaultKey(), baseCfg.getKeyDelimiter());
    if (defaultResolved != null && !defaultResolved.isEmpty()) {
      return defaultResolved;
    }

    // No rules produced a non-empty value, Fall back to default value
    return baseCfg.getDefaultValue();
  }

  /**
   * Helper method to resolve a list of keys from the tags map.
   *
   * @param tags Map of tags from the span.
   * @param keys List of keys to look up.
   * @param delimiter Delimiter to use when combining multiple key values.
   * @return The resolved value, or null if any key is missing.
   */
  private String resolveKeys(Map<String, String> tags, List<String> keys, String delimiter) {
    if (keys == null || keys.isEmpty()) {
      return null;
    }

    // Collect values for all parts in a key
    List<String> values = new java.util.ArrayList<>();
    for (String keyPart : keys) {
      String value = tags.get(keyPart);
      if (value == null) {
        // If any key is missing, return null
        return null;
      }
      values.add(value);
    }

    // Combine values with delimiter if present and multiple keys exist
    if (values.size() > 1 && delimiter != null) {
      return String.join(delimiter, values);
    }
    return values.get(0);
  }

  @Override
  public String toString() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (Exception e) {
      return "GraphConfig{error serializing to string}";
    }
  }
}
