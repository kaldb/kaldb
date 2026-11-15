package com.slack.astra.contrib.dashboardsgateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linecorp.armeria.common.QueryParams;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Translates the Dashboards 2.11 Trace Analytics contract to existing KalDB spans. */
final class TraceAnalyticsAdapter {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Pattern DATE_MATH =
      Pattern.compile("now(?:([+-])(\\d+)(ms|[smhdwMy]))?(?:/([smhdwMy]))?");
  private static final Map<String, String> FIELDS =
      Map.ofEntries(
          Map.entry("traceId", "trace_id"),
          Map.entry("spanId", "_id"),
          Map.entry("parentSpanId", "parent_id"),
          Map.entry("serviceName", "resource.service.name"),
          Map.entry("startTime", "_timesinceepoch"),
          Map.entry("endTime", "_timesinceepoch"),
          Map.entry("durationInNanos", "duration"),
          Map.entry("traceGroup", "name"),
          Map.entry("traceGroupFields.durationInNanos", "duration"),
          Map.entry("traceGroupFields.endTime", "_timesinceepoch"),
          Map.entry("traceGroupFields.statusCode", "status.code"));

  static String request(String body, String queryString) throws IOException {
    ObjectNode request = (ObjectNode) JSON.readTree(body);
    String size = QueryParams.fromQueryString(queryString == null ? "" : queryString).get("size");
    if (size != null) {
      int limit = Integer.parseInt(size);
      if (limit < 0) throw new IllegalArgumentException("Negative search size");
      request.put("size", limit);
    }
    // These are virtual fields. Fetch the stored span before applying the response projection.
    request.remove("_source");
    if (request.path("aggs").has("traces") || request.path("aggregations").has("traces")) {
      JsonNode original = request.remove("query");
      ObjectNode bool = request.putObject("query").putObject("bool");
      if (original != null) bool.putArray("must").add(original);
      bool.putArray("must_not").addObject().putObject("exists").put("field", "parent_id");
    }
    return JSON.writeValueAsString(translate(request, Instant.now(), ""));
  }

  private static JsonNode translate(JsonNode node, Instant now, String context) {
    if (node.isArray()) {
      ArrayNode result = JSON.createArrayNode();
      node.forEach(child -> result.add(translate(child, now, context)));
      return result;
    }
    if (!node.isObject()) return node;
    // Root spans have no parent field in KalDB, rather than an indexed empty parent ID.
    if (node.path("term").has("parentSpanId")) {
      JsonNode parent = node.path("term").path("parentSpanId");
      if (parent.path("value").asText(parent.asText()).isEmpty()) {
        ObjectNode result = JSON.createObjectNode();
        result
            .putObject("bool")
            .putArray("must_not")
            .addObject()
            .putObject("exists")
            .put("field", "parent_id");
        return result;
      }
    }
    ObjectNode result = JSON.createObjectNode();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String key = entry.getKey();
      JsonNode value = entry.getValue();
      if ((context.equals("term") || context.equals("terms"))
          && key.endsWith("durationInNanos")
          && FIELDS.containsKey(key)) {
        JsonNode scaled = value.deepCopy();
        if (scaled.isArray()) {
          for (int i = 0; i < scaled.size(); i++) {
            ((ArrayNode) scaled)
                .set(i, JSON.getNodeFactory().numberNode(scaled.get(i).asDouble() / 1000.0));
          }
        } else if (scaled.isObject() && scaled.has("value")) {
          ((ObjectNode) scaled).put("value", scaled.path("value").asDouble() / 1000.0);
        } else {
          scaled = JSON.getNodeFactory().numberNode(scaled.asDouble() / 1000.0);
        }
        result.set(FIELDS.get(key), scaled);
      } else if (key.equals("field") && value.isTextual()) {
        result.put(key, FIELDS.getOrDefault(value.asText(), value.asText()));
      } else if (key.equals("script") && (value.isTextual() || value.isObject())) {
        if (value.isTextual()) result.put(key, script(value.asText()));
        else {
          ObjectNode converted = value.deepCopy();
          if (converted.has("source"))
            converted.put("source", script(converted.get("source").asText()));
          result.set(key, converted);
        }
      } else if (key.equals("range") && value.isObject()) {
        ObjectNode ranges = result.putObject(key);
        value
            .fields()
            .forEachRemaining(
                range -> {
                  ObjectNode bounds = range.getValue().deepCopy();
                  if (range.getKey().equals("startTime") || range.getKey().equals("endTime")) {
                    for (String bound : new String[] {"gte", "gt", "lte", "lt"}) {
                      if (bounds.has(bound) && bounds.get(bound).isTextual()) {
                        bounds.put(
                            bound,
                            epochMillis(
                                bounds.get(bound).asText(),
                                now,
                                bound.equals("lte") || bound.equals("gt")));
                      }
                    }
                  } else if (range.getKey().endsWith("durationInNanos")) {
                    for (String bound : new String[] {"gte", "gt", "lte", "lt"}) {
                      if (bounds.has(bound))
                        bounds.put(bound, bounds.get(bound).asDouble() / 1000.0);
                    }
                  }
                  ranges.set(FIELDS.getOrDefault(range.getKey(), range.getKey()), bounds);
                });
      } else {
        result.set(FIELDS.getOrDefault(key, key), translate(value, now, key));
      }
    }
    if (node.path("filter").path("term").has("traceGroupFields.statusCode")) {
      String code = node.path("filter").path("term").path("traceGroupFields.statusCode").asText();
      if (!code.equals("2")) throw new IllegalArgumentException("Unsupported trace error filter");
      result.remove("filter");
      result
          .putObject("sum")
          .putObject("script")
          .put(
              "source",
              "doc['status.code'].size() != 0 && doc['status.code'].value == 'STATUS_CODE_ERROR' ? 1 : 0");
    }
    // Metrics on virtual duration fields must retain the plugin's nanosecond units.
    if (node.path("field").asText().equals("durationInNanos")) {
      result.remove("field");
      result
          .putObject("script")
          .put("source", "doc['duration'].size() == 0 ? 0 : doc['duration'].value * 1000.0");
    }
    if (node.path("field").asText().equals("traceGroupFields.endTime")) {
      result.remove("field");
      result
          .putObject("script")
          .put(
              "source",
              "doc['_timesinceepoch'].value + "
                  + "(doc['duration'].size() == 0 ? 0 : doc['duration'].value / 1000.0)");
    }
    if (node.has("traceGroupFields.statusCode") || node.has("status.code")) {
      JsonNode code =
          node.has("traceGroupFields.statusCode")
              ? node.get("traceGroupFields.statusCode")
              : node.get("status.code");
      String raw = code.path("value").asText(code.asText());
      String stored =
          switch (raw) {
            case "2" -> "STATUS_CODE_ERROR";
            case "1" -> "STATUS_CODE_OK";
            case "0" -> "STATUS_CODE_UNSET";
            default -> raw;
          };
      if (code.isObject()) ((ObjectNode) result.get("status.code")).put("value", stored);
      else result.put("status.code", stored);
    }
    return result;
  }

  private static long epochMillis(String value, Instant now, boolean roundUp) {
    if (value.matches("\\d+")) return Long.parseLong(value);
    Matcher math = DATE_MATH.matcher(value);
    if (!math.matches()) return Instant.parse(value).toEpochMilli();
    ZonedDateTime time = now.atZone(ZoneOffset.UTC);
    if (math.group(1) != null) {
      long amount = Long.parseLong(math.group(2)) * (math.group(1).equals("-") ? -1 : 1);
      ChronoUnit unit =
          switch (math.group(3)) {
            case "ms" -> ChronoUnit.MILLIS;
            case "s" -> ChronoUnit.SECONDS;
            case "m" -> ChronoUnit.MINUTES;
            case "h" -> ChronoUnit.HOURS;
            case "d" -> ChronoUnit.DAYS;
            case "w" -> ChronoUnit.WEEKS;
            case "M" -> ChronoUnit.MONTHS;
            case "y" -> ChronoUnit.YEARS;
            default -> throw new IllegalArgumentException("Invalid date math unit");
          };
      time = time.plus(amount, unit);
    }
    if (math.group(4) != null) {
      time =
          switch (math.group(4)) {
            case "s" -> time.truncatedTo(ChronoUnit.SECONDS);
            case "m" -> time.truncatedTo(ChronoUnit.MINUTES);
            case "h" -> time.truncatedTo(ChronoUnit.HOURS);
            case "d" -> time.truncatedTo(ChronoUnit.DAYS);
            case "w" ->
                time.minusDays(time.getDayOfWeek().getValue() - 1).truncatedTo(ChronoUnit.DAYS);
            case "M" -> time.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
            case "y" -> time.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
            default -> throw new IllegalArgumentException("Invalid date rounding unit");
          };
      if (roundUp) {
        time =
            switch (math.group(4)) {
              case "s" -> time.plusSeconds(1);
              case "m" -> time.plusMinutes(1);
              case "h" -> time.plusHours(1);
              case "d" -> time.plusDays(1);
              case "w" -> time.plusWeeks(1);
              case "M" -> time.plusMonths(1);
              case "y" -> time.plusYears(1);
              default -> throw new IllegalArgumentException("Invalid date rounding unit");
            };
        time = time.minusNanos(1_000_000);
      }
    }
    return time.toInstant().toEpochMilli();
  }

  private static String script(String source) {
    for (Map.Entry<String, String> entry : FIELDS.entrySet()) {
      String oldField = "doc['" + entry.getKey() + "']";
      String newField = "doc['" + entry.getValue() + "']";
      if (entry.getKey().endsWith("durationInNanos")) {
        source = source.replace(oldField + ".value", "(" + newField + ".value * 1000)");
      }
      source =
          source
              .replace(oldField, newField)
              .replace(
                  "containsKey('" + entry.getKey() + "')",
                  "containsKey('" + entry.getValue() + "')");
    }
    return source;
  }

  static String response(String body) throws IOException {
    JsonNode response = JSON.readTree(body);
    restoreErrorCounts(response.path("aggregations"));
    for (JsonNode hit : response.path("hits").path("hits")) {
      if (!(hit.get("_source") instanceof ObjectNode source)) continue;
      source.put("spanId", hit.path("_id").asText());
      source.put("traceId", source.path("trace_id").asText());
      source.put("parentSpanId", source.path("parent_id").asText(""));
      source.put("serviceName", source.path("resource.service.name").asText());
      Instant start = Instant.parse(source.path("_timesinceepoch").asText());
      source.put("startTime", start.toString());
      if (source.has("duration")) {
        long duration = Math.multiplyExact(source.path("duration").asLong(), 1000L);
        source.put("durationInNanos", duration);
        source.put("endTime", start.plusNanos(duration).toString());
      }
      String code = source.path("status.code").asText();
      source.put(
          "status.code",
          code.equals("STATUS_CODE_ERROR") ? 2 : code.equals("STATUS_CODE_OK") ? 1 : 0);
      if (hit.path("sort") instanceof ArrayNode sort && !sort.isEmpty()) {
        sort.set(
            0,
            JSON.getNodeFactory()
                .numberNode(
                    Math.addExact(
                        Math.multiplyExact(start.getEpochSecond(), 1_000_000_000L),
                        start.getNano())));
      }
    }
    return JSON.writeValueAsString(response);
  }

  private static void restoreErrorCounts(JsonNode node) {
    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              entry -> {
                if (entry.getKey().equals("error_count") && entry.getValue().has("value")) {
                  ObjectNode count = (ObjectNode) entry.getValue();
                  long value = count.path("value").asLong();
                  count.remove("value");
                  count.put("doc_count", value);
                } else restoreErrorCounts(entry.getValue());
              });
    } else if (node.isArray()) node.forEach(TraceAnalyticsAdapter::restoreErrorCounts);
  }
}
