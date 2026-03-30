package com.slack.astra.logstore.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.astra.logstore.LogMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

public final class SearchSortUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Set<String> LONG_SORT_FIELDS =
      Set.of(
          LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName,
          "@timestamp",
          "dropoff_datetime",
          "pickup_datetime");
  private static final SortSpec DEFAULT_SORT_SPEC =
      new SortSpec(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, SortValueType.LONG, true);

  private SearchSortUtils() {}

  public static Comparator<LogMessage> buildLogMessageComparator(String sortJson) {
    List<SortSpec> sortSpecs = parseSortSpecs(sortJson);
    return (left, right) -> compareLogMessages(left, right, sortSpecs);
  }

  public static Sort buildLuceneSort(String sortJson) {
    return new Sort(
        parseSortSpecs(sortJson).stream()
            .map(SearchSortUtils::toLuceneSortField)
            .toArray(SortField[]::new));
  }

  public static List<Object> getSortValues(LogMessage message, String sortJson) {
    return parseSortSpecs(sortJson).stream()
        .map(sortSpec -> getSortFieldValue(message, sortSpec.fieldName()))
        .map(SearchSortUtils::normalizeSortValue)
        .toList();
  }

  static List<SortSpec> parseSortSpecs(String sortJson) {
    if (sortJson == null || sortJson.isEmpty()) {
      return List.of(DEFAULT_SORT_SPEC);
    }
    try {
      JsonNode sortArray = OBJECT_MAPPER.readTree(sortJson);
      if (sortArray.isArray() && !sortArray.isEmpty()) {
        List<SortSpec> sortSpecs = new ArrayList<>();
        for (JsonNode sortNode : sortArray) {
          if (!sortNode.isObject() || sortNode.isEmpty()) {
            continue;
          }
          String fieldName = sortNode.fieldNames().next();
          boolean reverse = true;
          JsonNode fieldNode = sortNode.get(fieldName);
          if (fieldNode.isTextual()) {
            reverse = !"asc".equalsIgnoreCase(fieldNode.asText());
          } else if (fieldNode.isObject() && fieldNode.has("order")) {
            reverse = !"asc".equalsIgnoreCase(fieldNode.get("order").asText());
          }
          sortSpecs.add(
              new SortSpec(
                  fieldName,
                  LONG_SORT_FIELDS.contains(fieldName) ? SortValueType.LONG : SortValueType.DOUBLE,
                  reverse));
        }
        if (!sortSpecs.isEmpty()) {
          return sortSpecs;
        }
      }
    } catch (Exception e) {
      // Fall back to the default timestamp-desc sort if parsing fails.
    }
    return List.of(DEFAULT_SORT_SPEC);
  }

  private static int compareLogMessages(
      LogMessage left, LogMessage right, List<SortSpec> sortSpecs) {
    for (SortSpec sortSpec : sortSpecs) {
      int comparison =
          switch (sortSpec.valueType()) {
            case LONG ->
                compareNullableLongs(
                    coerceLong(getSortFieldValue(left, sortSpec.fieldName())),
                    coerceLong(getSortFieldValue(right, sortSpec.fieldName())));
            case DOUBLE ->
                compareNullableDoubles(
                    coerceDouble(getSortFieldValue(left, sortSpec.fieldName())),
                    coerceDouble(getSortFieldValue(right, sortSpec.fieldName())));
          };
      if (comparison != 0) {
        return sortSpec.reverse() ? -comparison : comparison;
      }
    }
    return 0;
  }

  private static SortField toLuceneSortField(SortSpec sortSpec) {
    SortField.Type luceneType =
        sortSpec.valueType() == SortValueType.LONG ? SortField.Type.LONG : SortField.Type.DOUBLE;
    return new SortField(sortSpec.fieldName(), luceneType, sortSpec.reverse());
  }

  private static Object getSortFieldValue(LogMessage message, String fieldName) {
    if (LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName.equals(fieldName)
        || "@timestamp".equals(fieldName)) {
      return message.getTimestamp();
    }
    return message.getSource().get(fieldName);
  }

  private static Object normalizeSortValue(Object value) {
    if (value instanceof Instant instant) {
      return instant.toEpochMilli();
    }
    if (value instanceof Number number) {
      if (number instanceof Float || number instanceof Double) {
        return number.doubleValue();
      }
      return number.longValue();
    }
    return value;
  }

  private static Long coerceLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof Instant instant) {
      return instant.toEpochMilli();
    }
    if (value instanceof String stringValue) {
      try {
        return Long.valueOf(stringValue);
      } catch (NumberFormatException ignored) {
        try {
          return Instant.parse(stringValue).toEpochMilli();
        } catch (Exception ignoredAgain) {
          return null;
        }
      }
    }
    return null;
  }

  private static Double coerceDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    if (value instanceof String stringValue) {
      try {
        return Double.valueOf(stringValue);
      } catch (NumberFormatException ignored) {
        return null;
      }
    }
    return null;
  }

  private static int compareNullableLongs(Long left, Long right) {
    if (left == null && right == null) {
      return 0;
    }
    if (left == null) {
      return 1;
    }
    if (right == null) {
      return -1;
    }
    return Long.compare(left, right);
  }

  private static int compareNullableDoubles(Double left, Double right) {
    if (left == null && right == null) {
      return 0;
    }
    if (left == null) {
      return 1;
    }
    if (right == null) {
      return -1;
    }
    return Double.compare(left, right);
  }

  enum SortValueType {
    LONG,
    DOUBLE
  }

  record SortSpec(String fieldName, SortValueType valueType, boolean reverse) {}
}
