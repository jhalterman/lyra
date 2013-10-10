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
   * Updates the retry stats, incrementing the retry count and increasing the wait time if
   * exponential backoff is configured, and returns whether the caller can retry based on the
   * updated stats.
   */
  public boolean canRetryForUpdatedStats() {
    retryCount++;
    long now = System.nanoTime();

    // First time
    if (startTime == 0)
      startTime = now;
    else if (retryIntervalMultiplier != -1)
      waitTime = Math.min(maxRetryInterval, (long) (waitTime * retryIntervalMultiplier));

    long elapsedNanos = (now - startTime);
    if (maxDuration != -1) {
      waitTime = Math.min(waitTime, maxDuration - elapsedNanos);
      maxWaitTime = maxDuration - elapsedNanos;
    }

    boolean withinMaxRetries = maxRetries == -1 || retryCount <= maxRetries;
    boolean withinMaxDuration = maxDuration == -1 || elapsedNanos <= maxDuration;
    return withinMaxRetries && withinMaxDuration;
  }

  /**
   * Returns the amount of time that an external caller should wait, based on the retry policy,
   * before performing a retry.
   */
  public Duration getWaitTime() {
    return Duration.nanos(waitTime);
  }

  public Duration getMaxWaitTime() {
    return maxWaitTime == -1 ? Duration.inf() : Duration.nanos(maxWaitTime);
  }
}
