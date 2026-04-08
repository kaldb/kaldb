package com.slack.astra.tools.monitor;

final class Env {
  private Env() {}

  static String get(String key, String defaultValue) {
    String value = System.getenv(key);
    return (value == null || value.isBlank()) ? defaultValue : value;
  }

  static int getInt(String key, int defaultValue) {
    return Integer.parseInt(get(key, String.valueOf(defaultValue)));
  }

  static long getLong(String key, long defaultValue) {
    return Long.parseLong(get(key, String.valueOf(defaultValue)));
  }

  static double getDouble(String key, double defaultValue) {
    return Double.parseDouble(get(key, String.valueOf(defaultValue)));
  }
}
