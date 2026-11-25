package com.slack.astra.logstore.search;

import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.util.Objects;
import org.apache.lucene.document.InetAddressPoint;

/** A typed scalar value carried with each hit for distributed hit sorting and response cursors. */
public record HitSortValue(Kind kind, Object value) {
  public enum Kind {
    NULL,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    BOOLEAN,
    BYTES,
    IP_ADDRESS
  }

  public HitSortValue {
    Objects.requireNonNull(kind, "kind");
    validate(kind, value);
  }

  /** Wraps a Lucene sort value whose type already carries enough domain meaning. */
  public static HitSortValue of(Object value) {
    if (value == null) {
      return nullValue();
    }
    if (value instanceof HitSortValue sortValue) {
      return sortValue;
    }
    if (value instanceof Integer intValue) {
      return intValue(intValue);
    }
    if (value instanceof Long longValue) {
      return longValue(longValue);
    }
    if (value instanceof Float floatValue) {
      return floatValue(floatValue);
    }
    if (value instanceof Double doubleValue) {
      return doubleValue(doubleValue);
    }
    if (value instanceof String stringValue) {
      return stringValue(stringValue);
    }
    if (value instanceof Boolean boolValue) {
      return booleanValue(boolValue);
    }
    if (value instanceof ByteString bytesValue) {
      return bytes(bytesValue);
    }
    throw new IllegalArgumentException("Unsupported hit sort value type: " + value.getClass());
  }

  static HitSortValue nullValue() {
    return new HitSortValue(Kind.NULL, null);
  }

  static HitSortValue intValue(int value) {
    return new HitSortValue(Kind.INT, value);
  }

  static HitSortValue longValue(long value) {
    return new HitSortValue(Kind.LONG, value);
  }

  static HitSortValue floatValue(float value) {
    return new HitSortValue(Kind.FLOAT, value);
  }

  static HitSortValue doubleValue(double value) {
    return new HitSortValue(Kind.DOUBLE, value);
  }

  static HitSortValue stringValue(String value) {
    return new HitSortValue(Kind.STRING, value);
  }

  static HitSortValue booleanValue(boolean value) {
    return new HitSortValue(Kind.BOOLEAN, value);
  }

  /** Wraps raw bytes used for internal byte-ordered sort values such as the _id tie breaker. */
  public static HitSortValue bytes(ByteString value) {
    return new HitSortValue(Kind.BYTES, value);
  }

  /** Wraps Lucene-encoded IP address bytes so response rendering can return an IP string. */
  public static HitSortValue ipAddress(ByteString value) {
    return new HitSortValue(Kind.IP_ADDRESS, value);
  }

  /** Compares two sort values in ascending order, with nulls always last. */
  public static int compareAscending(HitSortValue left, HitSortValue right) {
    if (left.kind == Kind.NULL && right.kind == Kind.NULL) {
      return 0;
    }
    if (left.kind == Kind.NULL) {
      return 1;
    }
    if (right.kind == Kind.NULL) {
      return -1;
    }
    if (left.kind != right.kind) {
      if (isNumber(left) && isNumber(right)) {
        return compareNumbers((Number) left.value, (Number) right.value);
      }
      throw new IllegalArgumentException(
          "Cannot compare sort values of different types: " + left.kind + " and " + right.kind);
    }

    return switch (left.kind) {
      case INT -> Integer.compare((Integer) left.value, (Integer) right.value);
      case LONG -> Long.compare((Long) left.value, (Long) right.value);
      case FLOAT -> Float.compare((Float) left.value, (Float) right.value);
      case DOUBLE -> Double.compare((Double) left.value, (Double) right.value);
      case STRING -> compareStringsByUtf8ByteOrder((String) left.value, (String) right.value);
      case BOOLEAN -> Boolean.compare((Boolean) left.value, (Boolean) right.value);
      case BYTES, IP_ADDRESS -> compareBytes((ByteString) left.value, (ByteString) right.value);
      case NULL -> 0;
    };
  }

  /** Converts a user-visible sort value into the scalar shape expected by OpenSearch responses. */
  public Object responseValue() {
    return switch (kind) {
      case NULL -> null;
      case INT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN -> value;
      case IP_ADDRESS ->
          InetAddressPoint.decode(((ByteString) value).toByteArray()).getHostAddress();
      case BYTES ->
          throw new IllegalStateException(
              "Internal byte-backed hit sort values cannot be rendered in OpenSearch responses.");
    };
  }

  private static void validate(Kind kind, Object value) {
    switch (kind) {
      case NULL -> {
        if (value != null) {
          throw new IllegalArgumentException("Null sort values must not carry a value.");
        }
      }
      case INT -> requireValueType(kind, value, Integer.class);
      case LONG -> requireValueType(kind, value, Long.class);
      case FLOAT -> requireValueType(kind, value, Float.class);
      case DOUBLE -> requireValueType(kind, value, Double.class);
      case STRING -> requireValueType(kind, value, String.class);
      case BOOLEAN -> requireValueType(kind, value, Boolean.class);
      case BYTES, IP_ADDRESS -> requireValueType(kind, value, ByteString.class);
    }
  }

  private static void requireValueType(Kind kind, Object value, Class<?> expectedType) {
    if (!expectedType.isInstance(value)) {
      throw new IllegalArgumentException(
          "Sort value kind " + kind + " requires " + expectedType.getSimpleName() + ".");
    }
  }

  private static boolean isNumber(HitSortValue value) {
    return value.kind == Kind.INT
        || value.kind == Kind.LONG
        || value.kind == Kind.FLOAT
        || value.kind == Kind.DOUBLE;
  }

  private static int compareNumbers(Number left, Number right) {
    double leftDouble = left.doubleValue();
    double rightDouble = right.doubleValue();
    if (!Double.isFinite(leftDouble) || !Double.isFinite(rightDouble)) {
      return Double.compare(leftDouble, rightDouble);
    }
    return new BigDecimal(left.toString()).compareTo(new BigDecimal(right.toString()));
  }

  private static int compareStringsByUtf8ByteOrder(String left, String right) {
    int leftIndex = 0;
    int rightIndex = 0;
    while (leftIndex < left.length() && rightIndex < right.length()) {
      int leftCodePoint = left.codePointAt(leftIndex);
      int rightCodePoint = right.codePointAt(rightIndex);
      int comparison = Integer.compare(leftCodePoint, rightCodePoint);
      if (comparison != 0) {
        return comparison;
      }
      leftIndex += Character.charCount(leftCodePoint);
      rightIndex += Character.charCount(rightCodePoint);
    }
    if (leftIndex == left.length() && rightIndex == right.length()) {
      return 0;
    }
    return leftIndex == left.length() ? -1 : 1;
  }

  private static int compareBytes(ByteString left, ByteString right) {
    int sharedLength = Math.min(left.size(), right.size());
    for (int i = 0; i < sharedLength; i++) {
      int comparison = Integer.compare(left.byteAt(i) & 0xff, right.byteAt(i) & 0xff);
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(left.size(), right.size());
  }
}
