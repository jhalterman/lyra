package net.jodah.lyra.internal;

import net.jodah.lyra.retry.RetryPolicy;
import net.jodah.lyra.util.Duration;

/**
 * Tracks retry related statistics for a supervised instance.
 * 
 * @author Jonathan Halterman
 */
public final class RetryStats {
  // Retry stats
  private final int maxRetries;
  private final long maxDuration;
  private final long retryInterval;
  private long startTime;

  // Backoff stats
  private double retryIntervalMultiplier = -1;
  private long maxRetryInterval;

  // Mutable state
  private int retryCount;
  private long waitTime;
  private long maxWaitTime;

  public RetryStats(RetryPolicy retryPolicy) {
    maxRetries = retryPolicy.getMaxRetries();
    retryInterval = retryPolicy.getRetryInterval() == null ? 0 : retryPolicy.getRetryInterval()
        .toNanos();
    if (retryPolicy.getMaxDuration() == null) {
      maxDuration = -1;
      waitTime = retryInterval;
      maxWaitTime = -1;
    } else {
      maxDuration = retryPolicy.getMaxDuration().toNanos();
      waitTime = Math.min(retryInterval, maxDuration);
      maxWaitTime = maxDuration;
    }

    if (retryPolicy.getMaxRetryInterval() != null) {
      retryIntervalMultiplier = retryPolicy.getRetryIntervalMultiplier();
      maxRetryInterval = retryPolicy.getMaxRetryInterval().toNanos();
    }
  }

  /**
   * Returns the max amount of time that an external caller should wait before the retry policy is
   * exceeded. The max wait time is calculated each time {@link #incrementRetries()} or
   * {@link #incrementTime()} is called.
   */
  public Duration getMaxWaitTime() {
    return maxWaitTime == -1 ? Duration.inf() : Duration.nanos(maxWaitTime);
  }

  /**
   * Returns the amount of time that an external caller should wait, based on the retry policy,
   * before performing a retry. The wait time is calculated each time {@link #incrementRetries()} or
   * {@link #incrementTime()} is called.
   */
  public Duration getWaitTime() {
    return Duration.nanos(waitTime);
  }

  /**
   * Increments the retries and time.
   */
  public void incrementRetries() {
    retryCount++;
    incrementTime();
  }

  public void incrementTime() {
    long now = System.nanoTime();

    // First time
    if (startTime == 0)
      startTime = now;
    else if (retryIntervalMultiplier != -1)
      waitTime = Math.min(maxRetryInterval, (long) (waitTime * retryIntervalMultiplier));

    if (maxDuration != -1) {
      long elapsedNanos = now - startTime;
      waitTime = Math.min(waitTime, maxDuration - elapsedNanos);
      maxWaitTime = maxDuration - elapsedNanos;
    }
  }

  /**
   * Returns true if the max retries or max duration for the retry policy have been exceeded else
   * false.
   */
  public boolean isPolicyExceeded() {
    boolean withinMaxRetries = maxRetries == -1 || retryCount < maxRetries;
    boolean withinMaxDuration = maxDuration == -1 || System.nanoTime() - startTime < maxDuration;
    return !withinMaxRetries || !withinMaxDuration;
  }
}
