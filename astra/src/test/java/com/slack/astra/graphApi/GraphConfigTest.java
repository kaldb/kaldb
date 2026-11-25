package com.slack.astra.graphApi;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.zipkinApi.ZipkinSpanResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class GraphConfigTest {
  @Test
  public void testLoadValidYamlConfig(@TempDir Path tempDir) throws IOException {
    String yamlContent =
        """
              node_metadata_tag_mapping:
                service:
                  default_key:
                    - service.name
                  default_value: unknown_service
                  rules:
                    - field: cluster.name
                      value: prod
                      override_key:
                        - prod.service.name
                    - field: cluster.name
                      value: staging
                      override_key:
                        - test.service.name
                cluster:
                  default_key:
                    - cluster.name
                  default_value: unknown_cluster
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: cluster.name
                      value: prod
                      override_key:
                        - operation.prod
              """;

    Path configFile = tempDir.resolve("test-config.yaml");
    Files.writeString(configFile, yamlContent);

    GraphConfig config = GraphConfig.load(configFile);

    assertThat(config).isNotNull();
    assertThat(config.getNodeMetadataTagMapping()).hasSize(2);
    assertThat(config.getEdgeMetadataTagMapping()).hasSize(1);

    GraphConfig.TagConfig serviceConfig = config.getNodeMetadataTagMapping().get("service");
    assertThat(serviceConfig.getDefaultKey()).containsExactly("service.name");
    assertThat(serviceConfig.getDefaultValue()).isEqualTo("unknown_service");
    assertThat(serviceConfig.getRules()).hasSize(2);

    GraphConfig.RuleConfig rule1 = serviceConfig.getRules().getFirst();
    assertThat(rule1.getOverrideKey()).containsExactly("prod.service.name");
    assertThat(rule1.getField()).isEqualTo("cluster.name");
    assertThat(rule1.getValue()).isEqualTo("prod");

    GraphConfig.RuleConfig rule2 = serviceConfig.getRules().get(1);
    assertThat(rule2.getOverrideKey()).containsExactly("test.service.name");
    assertThat(rule2.getField()).isEqualTo("cluster.name");
    assertThat(rule2.getValue()).isEqualTo("staging");

    GraphConfig.TagConfig clusterConfig = config.getNodeMetadataTagMapping().get("cluster");
    assertThat(clusterConfig.getDefaultKey()).containsExactly("cluster.name");
    assertThat(clusterConfig.getDefaultValue()).isEqualTo("unknown_cluster");
    assertThat(clusterConfig.getRules()).hasSize(0);

    GraphConfig.TagConfig connectionTypeConfig =
        config.getEdgeMetadataTagMapping().get("operation");
    assertThat(connectionTypeConfig.getDefaultKey()).containsExactly("operation_name");
    assertThat(connectionTypeConfig.getDefaultValue()).isEqualTo("unknown_operation");
    assertThat(connectionTypeConfig.getRules()).hasSize(1);
  }

  @Test
  public void testLoadNonExistentFile() throws IOException {
    GraphConfig config = GraphConfig.load(Path.of("/non/existent/file.yaml"));
    assertThat(config).isEqualTo(GraphConfig.DEFAULT);
  }

  @Test
  public void testLoadEmptyConfigFile() throws IOException {
    GraphConfig config = GraphConfig.load(Path.of(""));
    assertThat(config).isEqualTo(GraphConfig.DEFAULT);
  }

  @Test
  public void testLoadInvalidYaml(@TempDir Path tempDir) throws IOException {
    String invalidYaml =
        """
        invalid: yaml: content:
          - broken
        """;

    Path configFile = tempDir.resolve("invalid-config.yaml");
    Files.writeString(configFile, invalidYaml);

    GraphConfig config = GraphConfig.load(configFile);
    assertThat(config).isEqualTo(GraphConfig.DEFAULT);
  }

  @Test
  public void testResolveWithDefaultValue() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
             node_metadata_tag_mapping:
               app:
                 default_key:
                   - app.name
                 default_value: unknown_app
               namespace:
                 default_key:
                   - namespace.name
                 default_value: unknown_namespace
             edge_metadata_tag_mapping:
               operation:
                 default_key:
                   - operation_name
                 default_value: unknown_operation
             """);
    Map<String, String> tags = new HashMap<>();

    // Node
    String result = config.resolve(tags, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("unknown_app");

    // Edge
    result = config.resolve(tags, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("unknown_operation");
  }

  @Test
  public void testResolveWithUnknownField() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
               operation:
                 default_key:
                   - operation_name
                 default_value: unknown_operation
              """);
    Map<String, String> tags = Map.of("some.tag", "some-value");

    // Node
    String result = config.resolve(tags, "some_field", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("unknown_some_field");

    // Edge
    result = config.resolve(tags, "some_field", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("unknown_some_field");
  }

  @Test
  public void testResolveWithMatchingRule() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
              """);
    Map<String, String> tags =
        Map.of(
            "app.name",
            "my-app",
            "namespace.name",
            "prod-ns",
            "prod.app.name",
            "my-app-in-prod",
            "operation.prod",
            "prod_operation");

    // Node
    String result = config.resolve(tags, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app-in-prod");

    // Edge
    result = config.resolve(tags, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("prod_operation");
  }

  @Test
  public void testResolveWithMatchingRuleButMissingOverrideKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app", "namespace.name", "prod-ns", "operation_name", "some_operation");

    // Node - rule matches but override key is missing, should return default key's value
    String result = config.resolve(tags, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");

    // Edge - rule matches but override key is missing, should return default key's value
    result = config.resolve(tags, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("some_operation");
  }

  @Test
  public void testResolveWithNonMatchingRule() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
              """);
    Map<String, String> tags =
        Map.of(
            "app.name", "my-app", "namespace.name", "dev-ns", "operation_name", "some_operation");

    // Node
    String result = config.resolve(tags, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");

    // Edge
    result = config.resolve(tags, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("some_operation");
  }

  @Test
  public void testResolveWithMultipleRules() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
                    - field: cluster.name
                      value: east
                      override_key:
                        - operation.east
              """);
    Map<String, String> tags =
        Map.of(
            "app.name",
            "my-app",
            "namespace.name",
            "prod-ns",
            "cluster.name",
            "east",
            "prod.app.name",
            "my-app-in-prod",
            "east.app.name",
            "my-app-east",
            "operation_name",
            "some_operation",
            "operation.prod",
            "operation_prod",
            "operation.east",
            "operation_east");

    // Node
    String result = config.resolve(tags, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app-east");

    // Edge
    result = config.resolve(tags, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("operation_east");
  }

  @Test
  public void testResolveWithMultipleRulesNoMatch() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
              node_metadata_tag_mapping:
                app:
                  default_key:
                    - app.name
                  default_value: unknown_app
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - prod.app.name
                    - field: cluster.name
                      value: east
                      override_key:
                        - east.app.name
                namespace:
                  default_key:
                    - namespace.name
                  default_value: unknown_namespace
              edge_metadata_tag_mapping:
                operation:
                  default_key:
                    - operation_name
                  default_value: unknown_operation
                  rules:
                    - field: namespace.name
                      value: prod-ns
                      override_key:
                        - operation.prod
                    - field: cluster.name
                      value: east
                      override_key:
                        - operation_east
              """);
    Map<String, String> tags =
        Map.of(
            "app.name",
            "my-app",
            "namespace.name",
            "dev-ns",
            "cluster.name",
            "west",
            "operation_name",
            "some_operation",
            "operation.prod",
            "operation_prod");

    // Node
    String result = config.resolve(tags, "app", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");

    // Edge
    result = config.resolve(tags, "operation", GraphConfig.EntityType.EDGE);
    assertThat(result).isEqualTo("some_operation");
  }

  @Test
  public void testResolveWithMultipleDefaultKeys() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                          """);
    Map<String, String> tags = Map.of("kube.app", "my-app", "kube.namespace", "prod");

    String result = config.resolve(tags, "service", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app.prod");
  }

  @Test
  public void testResolveWithMultipleDefaultKeys_missingKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                          """);
    Map<String, String> tags = Map.of("kube.app", "my-app");

    String result = config.resolve(tags, "service", GraphConfig.EntityType.NODE);
    // Should return default value when any key is missing
    assertThat(result).isEqualTo("unknown_service");
  }

  @Test
  public void testResolveWithSingleDefaultKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                              default_value: unknown_service
                          """);
    Map<String, String> tags = Map.of("kube.app", "my-app");

    String result = config.resolve(tags, "service", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("my-app");
  }

  @Test
  public void testResolveWithMultipleOverrideKeys() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                              rules:
                                - field: operation_name
                                  value: http.request
                                  override_key:
                                    - tag.http.target.host
                                    - tag.http.method
                          """);
    Map<String, String> tags =
        Map.of(
            "kube.app",
            "my-app",
            "kube.namespace",
            "prod",
            "operation_name",
            "http.request",
            "tag.http.target.host",
            "api.example.com",
            "tag.http.method",
            "GET");

    String result = config.resolve(tags, "service", GraphConfig.EntityType.NODE);
    assertThat(result).isEqualTo("api.example.com.GET");
  }

  @Test
  public void testResolveWithMultipleOverrideKeys_missingKey() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                          node_metadata_tag_mapping:
                            service:
                              default_key:
                                - kube.app
                                - kube.namespace
                              default_value: unknown_service
                              key_delimiter: .
                              rules:
                                - field: operation_name
                                  value: http.request
                                  override_key:
                                    - tag.http.target.host
                                    - tag.http.method
                          """);
    Map<String, String> tags =
        Map.of(
            "kube.app",
            "my-app",
            "kube.namespace",
            "prod",
            "operation_name",
            "http.request",
            "tag.http.target.host",
            "api.example.com");

    String result = config.resolve(tags, "service", GraphConfig.EntityType.NODE);
    // Should return default key's value since override key is missing tag.http.method
    assertThat(result).isEqualTo("my-app.prod");
  }

  @Test
  public void testCreateNodeMetadataFromSpan_defaultConfig_usesRemoteEndpointServiceName() {
    GraphConfig config = GraphConfig.DEFAULT;
    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags("span1", "trace1", null, Map.of("some.tag", "some-value"));

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE);
    assertThat(metadata).hasSize(1);
    assertThat(metadata.get("service")).isEqualTo("default-service");
  }

  @Test
  public void testCreatNodeMetadataFromSpan_customConfig_usesTagMapping() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
      node_metadata_tag_mapping:
        app:
          default_key:
            - app.name
          default_value: unknown_app
        namespace:
          default_key:
            - namespace.name
          default_value: unknown_namespace
        resource:
          default_key:
            - resource.name
          default_value: unknown_resource
      """);

    Map<String, String> tags =
        Map.of(
            "app.name", "my-app",
            "namespace.name", "my-namespace",
            "resource.name", "my-resource");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.NODE);

    assertThat(metadata).hasSize(3);
    assertThat(metadata.get("app")).isEqualTo("my-app");
    assertThat(metadata.get("namespace")).isEqualTo("my-namespace");
    assertThat(metadata.get("resource")).isEqualTo("my-resource");
  }

  @Test
  public void testResolveWithMultipleMatchingRules_emptyValue_fallbackToNextRule()
      throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                node_metadata_tag_mapping:
                  resource:
                    default_key:
                      - resource
                    default_value: unknown_resource
                    rules:
                      - field: operation_name
                        value: http.request
                        override_key:
                          - http.url
                      - field: operation_name
                        value: http.request
                        override_key:
                          - tag.http.target.canonical_path
                """);

    // Case 1: tag.http.target.canonical_path is empty, should fallback to http.url
    Map<String, String> tags1 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "",
            "http.url",
            "/api/v1/users",
            "resource",
            "default_resource");

    String result1 = config.resolve(tags1, "resource", GraphConfig.EntityType.NODE);
    assertThat(result1).isEqualTo("/api/v1/users");

    // Case 2: Both are empty, should fallback to default key
    Map<String, String> tags2 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "",
            "http.url",
            "",
            "resource",
            "default_resource");

    String result2 = config.resolve(tags2, "resource", GraphConfig.EntityType.NODE);
    assertThat(result2).isEqualTo("default_resource");

    // Case 3: All empty including default key, should use default value
    Map<String, String> tags3 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "",
            "http.url",
            "",
            "resource",
            "");

    String result3 = config.resolve(tags3, "resource", GraphConfig.EntityType.NODE);
    assertThat(result3).isEqualTo("unknown_resource");

    // Case 4: tag.http.target.canonical_path has value, should use it
    Map<String, String> tags4 =
        Map.of(
            "operation_name",
            "http.request",
            "tag.http.target.canonical_path",
            "/api/canonical",
            "http.url",
            "/api/v1/users",
            "resource",
            "default_resource");

    String result4 = config.resolve(tags4, "resource", GraphConfig.EntityType.NODE);
    assertThat(result4).isEqualTo("/api/canonical");

    // Case 5: tag.http.target.canonical_path is missing, should fallback to http.url
    Map<String, String> tags5 =
        Map.of(
            "operation_name",
            "http.request",
            "http.url",
            "/api/v1/users",
            "resource",
            "default_resource");

    String result5 = config.resolve(tags5, "resource", GraphConfig.EntityType.NODE);
    assertThat(result1).isEqualTo("/api/v1/users");

    // Case 6: Both override keys are missing, should fallback to default key
    Map<String, String> tags6 =
        Map.of("operation_name", "http.request", "resource", "default_resource");

    String result6 = config.resolve(tags6, "resource", GraphConfig.EntityType.NODE);
    assertThat(result2).isEqualTo("default_resource");
  }

  @Test
  public void testCreateEdgeMetadataFromSpan_defaultConfig_isEmpty() {
    GraphConfig config = GraphConfig.DEFAULT;
    ZipkinSpanResponse span =
        TestUtils.createSpanWithTags("span1", "trace1", null, Map.of("some.tag", "some-value"));

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.EDGE);
    assertThat(metadata).isEmpty();
  }

  @Test
  public void testCreatEdgeMetadataFromSpan_customConfig_usesTagMapping() throws IOException {
    GraphConfig config =
        GraphConfig.load(
            """
                  edge_metadata_tag_mapping:
                    operation:
                      default_key:
                        - operation_name
                      default_value: unknown_operation
                  """);

    Map<String, String> tags = Map.of("operation_name", "some_operation");
    ZipkinSpanResponse span = TestUtils.createSpanWithTags("span1", "trace1", null, tags);

    SortedMap<String, String> metadata =
        config.createMetadataFromSpan(span, GraphConfig.EntityType.EDGE);

    assertThat(metadata).hasSize(1);
    assertThat(metadata.get("operation")).isEqualTo("some_operation");
  }
}
