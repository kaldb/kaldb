package com.slack.astra.contrib.dashboardsgateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Explicit request grammar and defensive response projection for secured reads. */
final class AuthorizedSearch {
  enum Kind {
    NATIVE,
    SEARCH,
    MSEARCH,
    MAPPING,
    FIELD_CAPS,
    RESOLVE,
    ALIAS
  }

  record Item(String index, ObjectNode body) {}

  record Plan(Kind kind, List<Item> items, Set<String> datasets, boolean discovery) {}

  static GatewayAuthorization.Rejected bad() {
    return new GatewayAuthorization.Rejected(
        HttpStatus.BAD_REQUEST, "unsupported or malformed secured request");
  }

  static GatewayAuthorization.Rejected forbidden() {
    return new GatewayAuthorization.Rejected(HttpStatus.FORBIDDEN, "index or field access denied");
  }

  static ObjectNode object(String json) {
    try {
      JsonNode node = GatewayAuthorization.JSON.readTree(json);
      if (node == null || !node.isObject()) throw bad();
      return (ObjectNode) node;
    } catch (Exception e) {
      throw bad();
    }
  }

  static void keys(JsonNode node, Set<String> allowed) {
    if (!node.isObject()) throw bad();
    node.fieldNames()
        .forEachRemaining(
            key -> {
              if (!allowed.contains(key)) throw bad();
            });
  }

  static boolean systemIndex(String index) {
    return index.matches("\\.(kibana|opensearch_dashboards)[a-zA-Z0-9_.*-]*");
  }

  static Set<String> resolve(String selector, Set<String> registered) {
    Set<String> result = new LinkedHashSet<>();
    for (String pattern : selector.split(",", -1)) {
      if (!pattern.matches("[a-z0-9*_-]+")) throw bad();
      if (pattern.contains("*")) {
        Pattern matcher = Pattern.compile(Pattern.quote(pattern).replace("*", "\\E.*\\Q"));
        registered.stream().filter(name -> matcher.matcher(name).matches()).forEach(result::add);
      } else result.add(pattern);
    }
    return Set.copyOf(result);
  }

  static Plan inspect(AggregatedHttpRequest request, Set<String> registered) {
    String raw = request.path().split("\\?", 2)[0];
    if (raw.contains("%") || raw.contains("//") || raw.contains("..")) throw bad();
    String[] parts = raw.substring(1).split("/", -1);
    if (raw.equals("/")
        || raw.startsWith("/_plugins/_security/")
        || raw.startsWith("/_nodes")
        || raw.startsWith("/_cluster/health")
        || raw.startsWith("/_cluster/state")
        || raw.equals("/_bulk")
        || (parts.length > 0 && systemIndex(parts[0]))
        || (parts.length == 2
            && Set.of("_alias", "_template", "_index_template", "_component_template")
                .contains(parts[0])
            && (systemIndex(parts[1])
                || parts[1].matches("opensearch_dashboards_index_template[a-zA-Z0-9_*.-]*")))
        || (parts.length == 3
            && parts[0].equals("_cat")
            && parts[1].equals("templates")
            && parts[2].matches("opensearch_dashboards_index_template[a-zA-Z0-9_*.-]*"))) {
      return new Plan(Kind.NATIVE, List.of(), Set.of(), false);
    }
    Kind kind;
    String selector;
    if (raw.startsWith("/_resolve/index/")) {
      kind = Kind.RESOLVE;
      selector = raw.substring("/_resolve/index/".length());
    } else {
      String operation = parts[parts.length - 1];
      kind =
          switch (operation) {
            case "_search" -> Kind.SEARCH;
            case "_msearch" -> Kind.MSEARCH;
            case "_mapping" -> Kind.MAPPING;
            case "_field_caps" -> Kind.FIELD_CAPS;
            case "_alias" -> Kind.ALIAS;
            default -> throw bad();
          };
      if (parts.length > 2) throw bad();
      selector = parts.length == 2 ? parts[0] : "*";
    }
    if (request.method() != HttpMethod.GET && request.method() != HttpMethod.POST) throw bad();
    validateParameters(request.path());
    if (kind == Kind.MSEARCH) {
      String content = request.contentUtf8();
      if (!content.endsWith("\n")) throw bad();
      String[] lines = content.split("\n", -1);
      if (lines.length < 3 || lines.length % 2 != 1 || lines.length > 129) throw bad();
      List<Item> items = new ArrayList<>();
      Set<String> indexes = new LinkedHashSet<>();
      boolean nativeItems = false, dataItems = false;
      for (int i = 0; i < lines.length - 1; i += 2) {
        ObjectNode header = object(lines[i]);
        keys(header, Set.of("index", "ignore_unavailable", "preference", "search_type"));
        JsonNode indexNode = header.get("index");
        String index = indexNode == null ? selector : indexNode.asText("");
        if (indexNode != null && !indexNode.isTextual()) throw bad();
        if (!selector.equals("*") && !selector.equals(index)) throw forbidden();
        if (systemIndex(index)) nativeItems = true;
        else {
          if (!index.matches("[a-z0-9][a-z0-9_-]{0,127}")) throw bad();
          dataItems = true;
        }
        items.add(new Item(index, object(lines[i + 1])));
        indexes.add(index);
      }
      if (nativeItems && dataItems) throw bad();
      return new Plan(
          nativeItems ? Kind.NATIVE : kind, List.copyOf(items), Set.copyOf(indexes), false);
    }
    if (kind == Kind.SEARCH) {
      if (!selector.matches("[a-z0-9][a-z0-9_-]{0,127}")) throw bad();
      return new Plan(
          kind,
          List.of(
              new Item(
                  selector, object(request.content().isEmpty() ? "{}" : request.contentUtf8()))),
          Set.of(selector),
          false);
    }
    return new Plan(kind, List.of(), resolve(selector, registered), selector.contains("*"));
  }

  static void validateParameters(String path) {
    if (!path.contains("?")) return;
    Set<String> seen = new LinkedHashSet<>();
    for (String pair : path.split("\\?", 2)[1].split("&")) {
      String[] parts = pair.split("=", 2);
      String name = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
      if (!Set.of(
                  "preference",
                  "rest_total_hits_as_int",
                  "ignore_unavailable",
                  "ignore_throttled",
                  "allow_no_indices",
                  "expand_wildcards",
                  "max_concurrent_shard_requests",
                  "typed_keys",
                  "fields",
                  "include_unmapped",
                  "startTimeEpochMs",
                  "endTimeEpochMs")
              .contains(name)
          || !seen.add(name)) throw bad();
    }
  }

  static Set<String> select(JsonNode selector, GatewayAuthorization.Dataset dataset) {
    Set<String> result = new LinkedHashSet<>();
    if (selector == null) return dataset.fields();
    List<JsonNode> selectors = new ArrayList<>();
    if (selector.isArray()) selector.forEach(selectors::add);
    else selectors.add(selector);
    Map<String, String> all = new LinkedHashMap<>();
    GatewayAuthorization.schemaLeaves(
        dataset.mapping().path("mappings").path("properties"), "", all, 0);
    for (JsonNode item : selectors) {
      String field = item.isTextual() ? item.asText() : item.path("field").asText("");
      if (item.isObject()) keys(item, Set.of("field", "format"));
      if (field.equals("*")) {
        result.addAll(dataset.fields());
        continue;
      }
      if (!field.matches("[a-zA-Z0-9_@.*-]+")) throw forbidden();
      Pattern matcher = Pattern.compile(Pattern.quote(field).replace("*", "\\E.*\\Q"));
      Set<String> matched = new LinkedHashSet<>();
      all.keySet().stream().filter(name -> matcher.matcher(name).matches()).forEach(matched::add);
      if (matched.isEmpty() || !dataset.fields().containsAll(matched)) throw forbidden();
      result.addAll(matched);
    }
    return Set.copyOf(result);
  }

  static void field(String name, GatewayAuthorization.Dataset dataset) {
    if (!dataset.fields().contains(name)) throw forbidden();
  }

  static ObjectNode query(JsonNode node, GatewayAuthorization.Dataset dataset, int depth) {
    if (depth > 20 || !node.isObject() || node.size() != 1) throw bad();
    String kind = node.fieldNames().next();
    JsonNode value = node.get(kind);
    ObjectNode result = ((ObjectNode) node).deepCopy();
    switch (kind) {
      case "match_all", "match_none" -> {
        if (!value.isObject() || !value.isEmpty()) throw bad();
      }
      case "exists" -> {
        keys(value, Set.of("field"));
        field(value.path("field").asText(), dataset);
      }
      case "term", "terms", "match", "match_phrase", "range" -> {
        if (!value.isObject() || value.size() != 1) throw bad();
        String name = value.fieldNames().next();
        field(name, dataset);
        JsonNode argument = value.get(name);
        if (kind.equals("range"))
          keys(argument, Set.of("gt", "gte", "lt", "lte", "format", "time_zone", "boost"));
        else if (kind.equals("terms")) {
          if (!argument.isArray() || argument.size() > 1000) throw bad();
          argument.forEach(
              v -> {
                if (!v.isValueNode()) throw bad();
              });
        } else if (argument.isObject()) {
          keys(
              argument,
              kind.equals("term")
                  ? Set.of("value", "boost")
                  : Set.of("query", "operator", "zero_terms_query", "boost"));
          argument.forEach(
              v -> {
                if (!v.isValueNode()) throw bad();
              });
        } else if (!argument.isValueNode()) throw bad();
      }
      case "bool" -> {
        keys(
            value, Set.of("must", "filter", "must_not", "should", "minimum_should_match", "boost"));
        ObjectNode rewritten = (ObjectNode) result.get(kind);
        for (String clause : List.of("must", "filter", "must_not", "should"))
          if (value.has(clause)) {
            JsonNode clauses = value.get(clause);
            if (clauses.isArray()) {
              if (clauses.size() > 100) throw bad();
              ArrayNode children = GatewayAuthorization.JSON.createArrayNode();
              clauses.forEach(child -> children.add(query(child, dataset, depth + 1)));
              rewritten.set(clause, children);
            } else rewritten.set(clause, query(clauses, dataset, depth + 1));
          }
      }
      case "query_string", "simple_query_string" -> {
        keys(
            value,
            Set.of(
                "query",
                "fields",
                "default_field",
                "default_operator",
                "analyze_wildcard",
                "time_zone",
                "lenient"));
        if (!value.path("query").isTextual()) throw bad();
        String text = value.get("query").asText().trim();
        Set<String> fields = select(value.get("fields"), dataset);
        if (value.has("default_field")) fields = select(value.get("default_field"), dataset);
        int colon = text.indexOf(':');
        if (colon >= 0) {
          String explicit = text.substring(0, colon);
          field(explicit, dataset);
          fields = Set.of(explicit);
          text = text.substring(colon + 1);
        }
        if (text.equals("*") || text.isEmpty()) {
          if (colon < 0) return object("{\"match_all\":{}}");
          ObjectNode exists = GatewayAuthorization.JSON.createObjectNode();
          exists.putObject("exists").put("field", fields.iterator().next());
          return exists;
        }
        if (text.length() > 512) throw bad();
        for (int i = 0; i < text.length(); i++) {
          char c = text.charAt(i);
          if (!Character.isLetterOrDigit(c) && !Character.isWhitespace(c) && "._-@".indexOf(c) < 0)
            throw bad();
        }
        ArrayNode alternatives = GatewayAuthorization.JSON.createArrayNode();
        for (String name : fields) {
          JsonNode types = dataset.capabilities().path("fields").path(name);
          if (types.has("text") || types.has("keyword")) {
            ObjectNode match = GatewayAuthorization.JSON.createObjectNode();
            match.putObject("match").put(name, text);
            alternatives.add(match);
          }
        }
        if (alternatives.isEmpty()) return object("{\"match_none\":{}}");
        ObjectNode rewritten = GatewayAuthorization.JSON.createObjectNode();
        ObjectNode bool = rewritten.putObject("bool");
        bool.set("should", alternatives);
        bool.put("minimum_should_match", 1);
        return rewritten;
      }
      default -> throw bad();
    }
    return result;
  }

  static void aggregations(JsonNode node, GatewayAuthorization.Dataset dataset, int depth) {
    if (!node.isObject() || node.size() > 50 || depth > 10) throw bad();
    node.forEach(
        aggregation -> {
          keys(
              aggregation,
              Set.of(
                  "date_histogram",
                  "terms",
                  "value_count",
                  "min",
                  "max",
                  "sum",
                  "avg",
                  "aggs",
                  "aggregations",
                  "meta"));
          int count = 0;
          for (String type :
              List.of("date_histogram", "terms", "value_count", "min", "max", "sum", "avg"))
            if (aggregation.has(type)) {
              count++;
              JsonNode options = aggregation.get(type);
              Set<String> allowed =
                  type.equals("date_histogram")
                      ? Set.of(
                          "field",
                          "fixed_interval",
                          "calendar_interval",
                          "interval",
                          "time_zone",
                          "min_doc_count",
                          "extended_bounds",
                          "format",
                          "offset",
                          "order")
                      : type.equals("terms")
                          ? Set.of("field", "size", "order", "min_doc_count", "missing")
                          : Set.of("field", "missing");
              keys(options, allowed);
              field(options.path("field").asText(), dataset);
              if (options.has("order")) keys(options.get("order"), Set.of("_count", "_key"));
              if (options.has("extended_bounds"))
                keys(options.get("extended_bounds"), Set.of("min", "max"));
            }
          if (count != 1) throw bad();
          for (String child : List.of("aggs", "aggregations"))
            if (aggregation.has(child)) aggregations(aggregation.get(child), dataset, depth + 1);
        });
  }

  static ObjectNode search(ObjectNode input, GatewayAuthorization.Dataset dataset) {
    keys(
        input,
        Set.of(
            "query",
            "post_filter",
            "from",
            "size",
            "sort",
            "_source",
            "fields",
            "docvalue_fields",
            "stored_fields",
            "aggs",
            "aggregations",
            "track_total_hits",
            "track_scores",
            "version",
            "seq_no_primary_term",
            "timeout",
            "highlight"));
    ObjectNode body = input.deepCopy();
    for (String number : List.of("from", "size"))
      if (body.has(number)
          && (!body.get(number).canConvertToInt()
              || body.get(number).asInt() < 0
              || body.get(number).asInt() > 10000)) throw bad();
    for (String clause : List.of("query", "post_filter"))
      if (body.has(clause)) body.set(clause, query(body.get(clause), dataset, 0));
    if (!body.has("query")) body.set("query", object("{\"match_all\":{}}"));
    if (body.has("sort")) {
      JsonNode sorts = body.get("sort");
      List<JsonNode> list = new ArrayList<>();
      if (sorts.isArray()) sorts.forEach(list::add);
      else list.add(sorts);
      if (list.size() > 10) throw bad();
      for (JsonNode sort : list) {
        String name =
            sort.isTextual() ? sort.asText() : sort.size() == 1 ? sort.fieldNames().next() : "";
        if (!Set.of("_doc", "_score").contains(name)) field(name, dataset);
        if (sort.isObject() && sort.get(name).isObject())
          keys(sort.get(name), Set.of("order", "unmapped_type", "missing", "mode", "format"));
      }
    }
    JsonNode source = body.get("_source");
    Set<String> includes = dataset.fields();
    if (source != null && source.isObject()) {
      keys(source, Set.of("includes", "excludes", "include", "exclude"));
      JsonNode selected = source.has("includes") ? source.get("includes") : source.get("include");
      includes = new LinkedHashSet<>(select(selected, dataset));
      JsonNode excluded = source.has("excludes") ? source.get("excludes") : source.get("exclude");
      if (excluded != null) includes.removeAll(select(excluded, dataset));
    } else if (source != null && !source.isBoolean()) includes = select(source, dataset);
    if (source == null || !source.isBoolean() || source.asBoolean())
      body.set(
          "_source", GatewayAuthorization.JSON.valueToTree(includes.stream().sorted().toList()));
    for (String selector : List.of("fields", "docvalue_fields", "stored_fields"))
      if (body.has(selector)) {
        Set<String> selected = select(body.get(selector), dataset);
        body.set(
            selector, GatewayAuthorization.JSON.valueToTree(selected.stream().sorted().toList()));
      }
    for (String aggregation : List.of("aggs", "aggregations"))
      if (body.has(aggregation)) aggregations(body.get(aggregation), dataset, 0);
    if (body.has("highlight")) {
      JsonNode highlight = body.get("highlight");
      keys(
          highlight,
          Set.of(
              "fields",
              "pre_tags",
              "post_tags",
              "fragment_size",
              "number_of_fragments",
              "require_field_match"));
      if (highlight.has("fields"))
        highlight
            .get("fields")
            .fieldNames()
            .forEachRemaining(
                name -> select(GatewayAuthorization.JSON.getNodeFactory().textNode(name), dataset));
      body.remove("highlight"); // Styling is deliberately outside this demo's backend grammar.
    }
    return body;
  }

  static AggregatedHttpRequest prepare(
      AggregatedHttpRequest request, Plan plan, GatewayAuthorization.Decision decision) {
    String body;
    if (plan.kind() == Kind.SEARCH) {
      Item item = plan.items().getFirst();
      body = search(item.body(), decision.datasets().get(item.index())).toString();
    } else {
      StringBuilder ndjson = new StringBuilder();
      for (Item item : plan.items()) {
        ObjectNode normalized = search(item.body(), decision.datasets().get(item.index()));
        ndjson
            .append(GatewayAuthorization.JSON.createObjectNode().put("index", item.index()))
            .append('\n')
            .append(normalized)
            .append('\n');
      }
      body = ndjson.toString();
    }
    return AggregatedHttpRequest.of(
        request.headers().toBuilder()
            .contentType(
                plan.kind() == Kind.MSEARCH
                    ? MediaType.parse("application/x-ndjson")
                    : MediaType.JSON_UTF_8)
            .build(),
        HttpData.ofUtf8(body));
  }

  static JsonNode project(JsonNode node, String prefix, Set<String> fields, int depth) {
    if (depth > 30) throw GatewayAuthorization.unavailable();
    if (fields.contains(prefix)) {
      if (node.isObject()
          || (node.isArray()
              && java.util.stream.StreamSupport.stream(node.spliterator(), false)
                  .anyMatch(JsonNode::isContainerNode))) throw GatewayAuthorization.unavailable();
      return node.deepCopy();
    }
    if (node.isObject()) {
      ObjectNode result = GatewayAuthorization.JSON.createObjectNode();
      node.fields()
          .forEachRemaining(
              entry -> {
                JsonNode child =
                    project(
                        entry.getValue(),
                        prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey(),
                        fields,
                        depth + 1);
                if (child != null) result.set(entry.getKey(), child);
              });
      return result.isEmpty() ? null : result;
    }
    if (node.isArray()) {
      ArrayNode result = GatewayAuthorization.JSON.createArrayNode();
      node.forEach(
          item -> {
            JsonNode child = project(item, prefix, fields, depth + 1);
            if (child != null) result.add(child);
          });
      return result.isEmpty() ? null : result;
    }
    return null;
  }

  static ObjectNode filteredSearch(
      JsonNode input, String index, GatewayAuthorization.Dataset dataset) {
    if (!input.isObject() || input.has("error") || !input.path("hits").path("hits").isArray())
      throw GatewayAuthorization.unavailable();
    ObjectNode result = GatewayAuthorization.JSON.createObjectNode();
    for (String key : List.of("took", "timed_out", "_shards", "aggregations"))
      if (input.has(key)) result.set(key, input.get(key));
    ObjectNode hits = result.putObject("hits");
    for (String key : List.of("total", "max_score"))
      if (input.get("hits").has(key)) hits.set(key, input.get("hits").get(key));
    ArrayNode documents = hits.putArray("hits");
    for (JsonNode hit : input.get("hits").get("hits")) {
      if (!hit.path("_index").asText().equals(index)) throw GatewayAuthorization.unavailable();
      ObjectNode document = documents.addObject();
      for (String key : List.of("_index", "_id", "_score", "sort"))
        if (hit.has(key)) document.set(key, hit.get(key));
      if (hit.has("_source")) {
        JsonNode source = project(hit.get("_source"), "", dataset.fields(), 0);
        document.set(
            "_source", source == null ? GatewayAuthorization.JSON.createObjectNode() : source);
      }
      if (hit.has("fields")) {
        if (!hit.get("fields").isObject()) throw GatewayAuthorization.unavailable();
        ObjectNode visible = document.putObject("fields");
        hit.get("fields")
            .fields()
            .forEachRemaining(
                entry -> {
                  if (dataset.fields().contains(entry.getKey()))
                    visible.set(
                        entry.getKey(),
                        project(entry.getValue(), entry.getKey(), dataset.fields(), 0));
                });
      }
    }
    return result;
  }

  static JsonNode filterResponse(
      AggregatedHttpResponse response, Plan plan, GatewayAuthorization.Decision decision) {
    if (response.status().code() != 200) throw GatewayAuthorization.unavailable();
    JsonNode input = GatewayAuthorization.parse(response.content().array());
    if (plan.kind() == Kind.SEARCH) {
      Item item = plan.items().getFirst();
      return filteredSearch(input, item.index(), decision.datasets().get(item.index()));
    }
    if (!input.path("responses").isArray() || input.get("responses").size() != plan.items().size())
      throw GatewayAuthorization.unavailable();
    ObjectNode result = GatewayAuthorization.JSON.createObjectNode();
    ArrayNode responses = result.putArray("responses");
    for (int i = 0; i < plan.items().size(); i++) {
      Item item = plan.items().get(i);
      responses.add(
          filteredSearch(
              input.get("responses").get(i), item.index(), decision.datasets().get(item.index())));
    }
    return result;
  }

  static ObjectNode properties(JsonNode source, String prefix, Set<String> visible) {
    ObjectNode result = GatewayAuthorization.JSON.createObjectNode();
    source
        .fields()
        .forEachRemaining(
            entry -> {
              String field = prefix + entry.getKey();
              JsonNode definition = entry.getValue();
              if (definition.has("properties")) {
                ObjectNode children =
                    properties(definition.get("properties"), field + ".", visible);
                if (!children.isEmpty()) {
                  ObjectNode parent = result.putObject(entry.getKey());
                  parent.set("properties", children);
                  if (definition.path("type").asText().equals("nested"))
                    parent.put("type", "nested");
                }
              } else if (visible.contains(field))
                result.putObject(entry.getKey()).set("type", definition.get("type"));
            });
    return result;
  }

  static ObjectNode metadata(Plan plan, GatewayAuthorization.Decision decision) {
    ObjectNode result = GatewayAuthorization.JSON.createObjectNode();
    if (plan.kind() == Kind.RESOLVE) {
      ArrayNode indexes = result.putArray("indices");
      result.putArray("aliases");
      result.putArray("data_streams");
      decision.datasets().keySet().stream()
          .sorted()
          .forEach(
              name -> {
                ObjectNode item = indexes.addObject().put("name", name);
                item.putArray("aliases");
                item.putArray("attributes").add("open");
              });
    } else if (plan.kind() == Kind.FIELD_CAPS) {
      result.set(
          "indices",
          GatewayAuthorization.JSON.valueToTree(
              decision.datasets().keySet().stream().sorted().toList()));
      ObjectNode fields = result.putObject("fields");
      decision
          .datasets()
          .forEach(
              (index, dataset) ->
                  dataset
                      .fields()
                      .forEach(
                          name -> {
                            ObjectNode types =
                                fields.has(name)
                                    ? (ObjectNode) fields.get(name)
                                    : fields.putObject(name);
                            dataset
                                .capabilities()
                                .path("fields")
                                .path(name)
                                .fields()
                                .forEachRemaining(
                                    type -> {
                                      ObjectNode options =
                                          types.has(type.getKey())
                                              ? (ObjectNode) types.get(type.getKey())
                                              : (ObjectNode) type.getValue().deepCopy();
                                      if (!types.has(type.getKey())) {
                                        options.remove(
                                            List.of(
                                                "indices",
                                                "non_searchable_indices",
                                                "non_aggregatable_indices"));
                                        options.putArray("indices");
                                        types.set(type.getKey(), options);
                                      }
                                      ((ArrayNode) options.get("indices")).add(index);
                                    });
                          }));
    } else {
      decision
          .datasets()
          .forEach(
              (index, dataset) -> {
                if (plan.kind() == Kind.ALIAS) result.putObject(index).putObject("aliases");
                else
                  result
                      .putObject(index)
                      .putObject("mappings")
                      .set(
                          "properties",
                          properties(
                              dataset.mapping().path("mappings").path("properties"),
                              "",
                              dataset.fields()));
              });
    }
    return result;
  }
}
