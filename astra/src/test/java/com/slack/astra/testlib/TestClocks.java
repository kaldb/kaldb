package com.slack.astra.testlib;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public final class TestClocks {
  private TestClocks() {}

  public static final class MutableClock extends Clock {
    private long epochMs;
    private final ZoneId zone;

    public MutableClock(Instant instant) {
      this(instant, ZoneOffset.UTC);
    }

    public MutableClock(Instant instant, ZoneId zone) {
      this.epochMs = instant.toEpochMilli();
      this.zone = zone;
    }

    public void advance(Duration duration) {
      epochMs += duration.toMillis();
    }

    @Override
    public ZoneId getZone() {
      return zone;
    }

    @Override
    public MutableClock withZone(ZoneId zone) {
      return new MutableClock(Instant.ofEpochMilli(epochMs), zone);
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(epochMs);
    }
  }
}
