package com.slack.astra.tools.loadgen;

import java.util.Locale;

public class LoadGen {
  public static void main(String[] args) throws Exception {
    String mode = Env.get("LOADGEN_MODE", "bulk").toLowerCase(Locale.ROOT);
    switch (mode) {
      case "bulk" -> SimpleBulkLoadGenerator.runFromEnvironment();
      case "steady", "steady-state", "steady_state" -> SteadyStateMonitor.runFromEnvironment();
      default ->
          throw new IllegalArgumentException(
              "Unsupported LOADGEN_MODE: " + mode + ". Expected bulk or steady-state.");
    }
  }
}
