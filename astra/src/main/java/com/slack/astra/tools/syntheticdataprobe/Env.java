package com.slack.astra.tools.syntheticdataprobe;

final class Env {
  private Env() {}

  static String get(String key, String defaultValue) {
    String value = System.getenv(key);
    return (value == null || value.isBlank()) ? defaultValue : value;
  }

  static int getInt(String key, int defaultValue) {
    String value = get(key, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw invalidNumericValue(key, value, e);
    }
  }

  static long getLong(String key, long defaultValue) {
    String value = get(key, String.valueOf(defaultValue));
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw invalidNumericValue(key, value, e);
    }
  }

  static double getDouble(String key, double defaultValue) {
    String value = get(key, String.valueOf(defaultValue));
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw invalidNumericValue(key, value, e);
    }
  }

  private static NumberFormatException invalidNumericValue(
      String key, String value, NumberFormatException cause) {
    NumberFormatException exception =
        new NumberFormatException("Invalid numeric value for " + key + ": '" + value + "'");
    exception.initCause(cause);
    return exception;
  }
}
