package com.slack.astra.contrib.dashboardsgateway;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Native, per-request index/FLS authorization for the bounded secured gateway. */
final class GatewayAuthorization {
  static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  record Dataset(Set<String> fields, JsonNode mapping, JsonNode capabilities) {
    Dataset {
      fields = Set.copyOf(fields);
    }
  }

  record Decision(String principal, Map<String, Dataset> datasets) {
    Decision {
      datasets = Map.copyOf(datasets);
    }
  }

  static final class Rejected extends RuntimeException {
    final HttpStatus status;

    Rejected(HttpStatus status, String message) {
      super(message);
      this.status = status;
    }
  }

  static Rejected unavailable() {
    return new Rejected(HttpStatus.SERVICE_UNAVAILABLE, "native policy unavailable or incomplete");
  }

  static JsonNode parse(byte[] bytes) {
    try {
      JsonNode value = JSON.readTree(bytes);
      if (value == null || !value.isObject()) throw unavailable();
      return value;
    } catch (Exception e) {
      throw unavailable();
    }
  }

  static Set<String> strings(JsonNode node) {
    if (node == null || !node.isArray()) throw unavailable();
    Set<String> values = new LinkedHashSet<>();
    for (JsonNode item : node) {
      if (!item.isTextual() || item.asText().isBlank() || !values.add(item.asText()))
        throw unavailable();
    }
    return Set.copyOf(values);
  }

  static void schemaLeaves(
      JsonNode properties, String prefix, Map<String, String> leaves, int depth) {
    if (!properties.isObject() || depth > 20) throw unavailable();
    properties
        .fields()
        .forEachRemaining(
            entry -> {
              String name = prefix + entry.getKey();
              JsonNode definition = entry.getValue();
              if (!entry.getKey().matches("[a-zA-Z0-9_@-]{1,128}") || !definition.isObject())
                throw unavailable();
              if (definition.has("fields") || definition.path("type").asText().equals("alias")) {
                throw new Rejected(
                    HttpStatus.FORBIDDEN, "multi-field and alias schemas are unsupported");
              }
              if (definition.has("properties")) {
                schemaLeaves(definition.get("properties"), name + ".", leaves, depth + 1);
              } else {
                String type = definition.path("type").asText();
                if (!Set.of(
                        "text", "keyword", "date", "long", "integer", "double", "float", "boolean")
                    .contains(type)) {
                  throw new Rejected(HttpStatus.FORBIDDEN, "unsupported schema type");
                }
                leaves.put(name, type);
              }
            });
  }

  static Set<String> checkRole(JsonNode role) {
    Set<String> exclusions = new LinkedHashSet<>();
    if (role == null || !role.isObject() || !role.path("index_permissions").isArray())
      throw unavailable();
    for (JsonNode permission : role.get("index_permissions")) {
      if (!permission.isObject()) throw unavailable();
      JsonNode dls = permission.get("dls");
      JsonNode masked = permission.get("masked_fields");
      if ((dls != null && (!dls.isTextual() || !dls.asText().isBlank()))
          || (masked != null && (!masked.isArray() || !masked.isEmpty()))) {
        throw new Rejected(HttpStatus.FORBIDDEN, "DLS and masking policies are unsupported");
      }
      JsonNode fls = permission.get("fls");
      if (fls != null) {
        for (String rule : strings(fls)) {
          if (!rule.matches("[~!][a-zA-Z0-9_@-]+(?:\\.[a-zA-Z0-9_@-]+)*")) {
            throw new Rejected(
                HttpStatus.FORBIDDEN, "only exact negative leaf FLS rules are supported");
          }
          exclusions.add(rule.substring(1));
        }
      }
    }
    return Set.copyOf(exclusions);
  }

  static JsonNode checked(AggregatedHttpResponse response, boolean userProbe) {
    int status = response.status().code();
    if (userProbe && status == 401)
      throw new Rejected(HttpStatus.UNAUTHORIZED, "authentication required");
    if (userProbe && (status == 403 || status == 404))
      throw new Rejected(HttpStatus.FORBIDDEN, "index access denied");
    if (status != 200) throw unavailable();
    JsonNode result = parse(response.content().array());
    if (result.has("error")) throw unavailable();
    return result;
  }

  static String userAuthorization(AggregatedHttpRequest request) {
    var credentials = request.headers().getAll("authorization");
    if (credentials.size() != 1
        || !credentials.getFirst().startsWith("Basic ")
        || credentials.getFirst().length() > 8192) {
      throw new Rejected(HttpStatus.UNAUTHORIZED, "basic authentication required");
    }
    return credentials.getFirst();
  }

  final WebClient nativeClient;
  private final GatewayConfig config;
  private final String policyAuthorization;

  GatewayAuthorization(GatewayConfig config) {
    this.config = config;
    try {
      if (Files.size(config.security().policyCredentialsFile()) > 4096)
        throw new IllegalArgumentException();
      JsonNode credentials = parse(Files.readAllBytes(config.security().policyCredentialsFile()));
      String user = credentials.path("username").asText();
      String password = credentials.path("password").asText();
      if (user.isBlank() || user.contains(":") || password.isBlank())
        throw new IllegalArgumentException();
      policyAuthorization =
          "Basic "
              + Base64.getEncoder()
                  .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
      var builder =
          WebClient.builder(config.openSearchUri().toString())
              .responseTimeout(config.requestTimeout())
              .maxResponseLength(config.maxRequestBytes());
      if (config.security().caFile() != null) {
        builder.factory(
            ClientFactory.builder()
                .tlsCustomizer(ssl -> ssl.trustManager(config.security().caFile().toFile()))
                .build());
      }
      nativeClient = builder.build();
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot initialize secured native policy client");
    }
  }

  /** Authenticate a real native principal and authorize all requested concrete indexes. */
  CompletableFuture<Decision> authorize(AggregatedHttpRequest request, Set<String> datasets) {
    return authorize(request, datasets, false);
  }

  /** Permit native saved-object migration operations only for the authenticated server role. */
  CompletableFuture<Void> requireDashboardsService(AggregatedHttpRequest request) {
    return json(
            HttpMethod.GET, "/_plugins/_security/authinfo", null, userAuthorization(request), true)
        .thenApply(
            identity -> {
              if (!strings(identity.get("roles")).contains("kibana_server"))
                throw new Rejected(
                    HttpStatus.FORBIDDEN, "native migration requires the Dashboards server role");
              return null;
            });
  }

  /** Discover only native-authorized datasets; policy outages still fail the whole request. */
  CompletableFuture<Decision> discover(AggregatedHttpRequest request, Set<String> datasets) {
    return authorize(request, datasets, true);
  }

  private CompletableFuture<Decision> authorize(
      AggregatedHttpRequest request, Set<String> datasets, boolean partial) {
    String authorization = userAuthorization(request);
    return json(HttpMethod.GET, "/_plugins/_security/authinfo", null, authorization, true)
        .thenCompose(
            identity -> {
              String principal = identity.path("user_name").asText();
              Set<String> effectiveRoles = strings(identity.get("roles"));
              if (principal.isBlank() || effectiveRoles.isEmpty()) throw unavailable();
              return json(
                      HttpMethod.GET,
                      "/_plugins/_security/api/roles",
                      null,
                      policyAuthorization,
                      false)
                  .thenCompose(
                      roles -> {
                        Set<String> exclusions = new LinkedHashSet<>();
                        for (String name : effectiveRoles)
                          exclusions.addAll(checkRole(roles.get(name)));
                        CompletableFuture<Map<String, Dataset>> permissions =
                            CompletableFuture.completedFuture(new LinkedHashMap<>());
                        for (String dataset : datasets) {
                          if (!config.security().datasets().contains(dataset))
                            throw new Rejected(HttpStatus.FORBIDDEN, "unregistered index");
                          permissions =
                              permissions.thenCompose(
                                  decisions ->
                                      authorizeDataset(dataset, authorization, exclusions)
                                          .handle(
                                              (fields, error) -> {
                                                if (error == null) decisions.put(dataset, fields);
                                                else {
                                                  Throwable cause = error;
                                                  while (cause.getCause() != null)
                                                    cause = cause.getCause();
                                                  if (!partial
                                                      || !(cause instanceof Rejected rejection)
                                                      || rejection.status != HttpStatus.FORBIDDEN)
                                                    throw new java.util.concurrent
                                                        .CompletionException(cause);
                                                }
                                                return decisions;
                                              }));
                        }
                        // A completed policy change during the probes invalidates this request's
                        // decision.
                        return permissions.thenCompose(
                            decisions ->
                                json(
                                        HttpMethod.GET,
                                        "/_plugins/_security/api/roles",
                                        null,
                                        policyAuthorization,
                                        false)
                                    .thenCompose(
                                        after -> {
                                          for (String role : effectiveRoles)
                                            if (!roles.get(role).equals(after.get(role)))
                                              throw unavailable();
                                          return json(
                                                  HttpMethod.GET,
                                                  "/_plugins/_security/authinfo",
                                                  null,
                                                  authorization,
                                                  true)
                                              .thenApply(
                                                  current -> {
                                                    if (!principal.equals(
                                                            current.path("user_name").asText())
                                                        || !effectiveRoles.equals(
                                                            strings(current.get("roles"))))
                                                      throw unavailable();
                                                    return new Decision(principal, decisions);
                                                  });
                                        }));
                      });
            });
  }

  private CompletableFuture<Dataset> authorizeDataset(
      String dataset, String authorization, Set<String> exclusions) {
    return json(
            HttpMethod.POST,
            "/" + dataset + "/_search",
            "{\"size\":0,\"query\":{\"match_none\":{}}}",
            authorization,
            true)
        .thenCompose(
            probe -> {
              JsonNode shards = probe.path("_shards");
              if (!shards.path("failed").isInt()
                  || shards.path("failed").intValue() != 0
                  || shards.path("successful").asInt() < 1
                  || shards.path("total").asInt() != shards.path("successful").asInt()
                  || !probe.path("timed_out").isBoolean()
                  || probe.path("timed_out").asBoolean()
                  || probe.path("hits").path("total").path("value").asLong(-1) != 0)
                throw unavailable();
              return json(
                  HttpMethod.GET, "/" + dataset + "/_mapping", null, policyAuthorization, false);
            })
        .thenCompose(
            mapping -> {
              if (mapping.size() != 1 || !mapping.has(dataset)) throw unavailable();
              Map<String, String> leaves = new LinkedHashMap<>();
              schemaLeaves(mapping.get(dataset).path("mappings").path("properties"), "", leaves, 0);
              Set<String> parents = new LinkedHashSet<>();
              for (String field : leaves.keySet()) {
                for (int dot = field.indexOf('.'); dot >= 0; dot = field.indexOf('.', dot + 1)) {
                  parents.add(field.substring(0, dot));
                }
              }
              if (exclusions.stream().anyMatch(parents::contains)) {
                throw new Rejected(
                    HttpStatus.FORBIDDEN, "parent FLS exclusions are unsupported on 2.11");
              }
              return json(
                      HttpMethod.GET,
                      "/" + dataset + "/_field_caps?fields=*",
                      null,
                      authorization,
                      true)
                  .thenCompose(
                      caps -> {
                        if (!strings(caps.get("indices")).equals(Set.of(dataset))
                            || !caps.path("fields").isObject()) throw unavailable();
                        Set<String> visible = new LinkedHashSet<>();
                        caps.get("fields")
                            .fields()
                            .forEachRemaining(
                                entry -> {
                                  if (entry.getKey().startsWith("_")) return;
                                  JsonNode types = entry.getValue();
                                  if (types.has("object") || types.has("nested")) return;
                                  String type = leaves.get(entry.getKey());
                                  if (type == null
                                      || types.size() != 1
                                      || !types.path(type).path("type").asText().equals(type))
                                    throw unavailable();
                                  visible.add(entry.getKey());
                                });
                        if (visible.isEmpty())
                          throw new Rejected(HttpStatus.FORBIDDEN, "no visible fields");
                        return json(
                                HttpMethod.GET,
                                "/" + dataset + "/_mapping",
                                null,
                                policyAuthorization,
                                false)
                            .thenApply(
                                current -> {
                                  if (!mapping.equals(current)) throw unavailable();
                                  return new Dataset(visible, mapping.get(dataset), caps);
                                });
                      });
            });
  }

  private CompletableFuture<JsonNode> json(
      HttpMethod method, String path, String body, String authorization, boolean userProbe) {
    var headers =
        RequestHeaders.builder(method, path)
            .set("authorization", authorization)
            .contentType(MediaType.JSON_UTF_8)
            .build();
    var request =
        body == null ? HttpRequest.of(headers) : HttpRequest.of(headers, HttpData.ofUtf8(body));
    return nativeClient
        .execute(request)
        .aggregate()
        .thenApply(response -> checked(response, userProbe));
  }
}
