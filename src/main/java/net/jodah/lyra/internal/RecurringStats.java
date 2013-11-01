package net.jodah.lyra.internal;

import net.jodah.lyra.util.Duration;

/**
 * Statistics to track the usage of a RecurringPolicy.
 * 
 * @author Jonathan Halterman
 */
public final class RecurringStats {
  // Retry stats
  private final int maxAttempts;
  private final long maxDuration;
  private final long interval;
  private long startTime;

  // Backoff stats
  private double intervalMultiplier = -1;
  private long maxInterval;

  // Mutable state
  private int attemptCount;
  private long waitTime;
  private long maxWaitTime;

  public RecurringStats(RecurringPolicy<?> retryPolicy) {
    maxAttempts = retryPolicy.getMaxAttempts();
    interval = retryPolicy.getInterval() == null ? 0 : retryPolicy.getInterval().toNanos();
    if (retryPolicy.getMaxDuration() == null) {
      maxDuration = -1;
      waitTime = interval;
      maxWaitTime = -1;
    } else {
      maxDuration = retryPolicy.getMaxDuration().toNanos();
      waitTime = Math.min(interval, maxDuration);
      maxWaitTime = maxDuration;
    }

    if (retryPolicy.getMaxInterval() != null) {
      intervalMultiplier = retryPolicy.getIntervalMultiplier();
      maxInterval = retryPolicy.getMaxInterval().toNanos();
    }
  }

  /**
   * Returns the max amount of time that an external caller should wait before the retry policy is
   * exceeded. The max wait time is calculated each time {@link #incrementAttempts()} or
   * {@link #incrementTime()} is called.
   */
  public Duration getMaxWaitTime() {
    return maxWaitTime == -1 ? Duration.inf() : Duration.nanos(maxWaitTime);
  }

  /**
   * Returns the amount of time that an external caller should wait, based on the retry policy,
   * before performing a retry. The wait time is calculated each time {@link #incrementAttempts()}
   * or {@link #incrementTime()} is called.
   */
  public Duration getWaitTime() {
    return Duration.nanos(waitTime);
  }

  /**
   * Increments the retry/recovery attempts and time.
   */
  public void incrementAttempts() {
    attemptCount++;
    incrementTime();
  }

  public void incrementTime() {
    long now = System.nanoTime();

    // First time
    if (startTime == 0)
      startTime = now;
    else if (intervalMultiplier != -1)
      waitTime = Math.min(maxInterval, (long) (waitTime * intervalMultiplier));

    if (maxDuration != -1) {
      long elapsedNanos = now - startTime;
      waitTime = Math.min(waitTime, maxDuration - elapsedNanos);
      maxWaitTime = maxDuration - elapsedNanos;
    }
  }

  /**
   * Returns true if the max attempts or max duration for the recurring policy have been exceeded
   * else false.
   */
  public boolean isPolicyExceeded() {
    boolean withinMaxRetries = maxAttempts == -1 || attemptCount < maxAttempts;
    boolean withinMaxDuration = maxDuration == -1 || System.nanoTime() - startTime < maxDuration;
    return !withinMaxRetries || !withinMaxDuration;
  }
}
