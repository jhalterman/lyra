package net.jodah.lyra.retry;

import net.jodah.lyra.internal.util.Assert;
import net.jodah.lyra.util.Duration;

/**
 * Policy that guides how retries and recovery are performed.
 * 
 * @author Jonathan Halterman
 */
public class RetryPolicy {
  private int maxRetries;
  private Duration maxDuration;
  private Duration retryInterval;
  private Duration maxRetryInterval;
  private int retryIntervalMultiplier;

  /**
   * Creates a RetryPolicy that always retries by default.
   */
  public RetryPolicy() {
    maxRetries = -1;
  }

  /**
   * Returns whether the policy allows any retries at all based on the configured maxRetries and
   * maxDuration.
   */
  public boolean allowsRetries() {
    return (maxRetries == -1 || maxRetries > 0) && (maxDuration == null || maxDuration.length > 0);
  }

  /**
   * Returns the max duration.
   * 
   * @see #withMaxDuration(Duration)
   */
  public Duration getMaxDuration() {
    return maxDuration;
  }

  /**
   * Returns the max retries.
   * 
   * @see #withMaxRetries(int)
   */
  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * Returns the max retry interval for backoff retries.
   * 
   * @see #withBackoff(Duration, Duration)
   */
  public Duration getMaxRetryInterval() {
    return maxRetryInterval;
  }

  /**
   * Returns the retry interval.
   * 
   * @see #withRetryInterval(Duration)
   * @see #withBackoff(Duration, Duration)
   * @see #withBackoff(Duration, Duration, int)
   */
  public Duration getRetryInterval() {
    return retryInterval;
  }

  /**
   * Returns the retry interval multiplier for backoff retries.
   * 
   * @see #withBackoff(Duration, Duration, int)
   */
  public int getRetryIntervalMultiplier() {
    return retryIntervalMultiplier;
  }

  /**
   * Sets the {@code retryInterval} to pause for, exponentially backing of to the
   * {@code maxRetryInterval} multiplying successive intervals by a factor of 2.
   * 
   * @throws NullPointerException if {@code retryInterval} or {@code maxRetryInterval} are null
   * @throws IllegalArgumentException if {@code retryInterval} is <= 0, {@code retryInterval} is >=
   *           {@code maxRetryInterval} or the {@code retryIntervalMultiplier} is <= 1
   */
  public RetryPolicy withBackoff(Duration retryInterval, Duration maxRetryInterval) {
    return withBackoff(retryInterval, maxRetryInterval, 2);
  }

  /**
   * Sets the {@code retryInterval} to pause for, exponentially backing of to the
   * {@code maxRetryInterval} multiplying successive intervals by the
   * {@code retryIntervalMultiplier}.
   * 
   * @throws NullPointerException if {@code retryInterval} or {@code maxRetryInterval} are null
   * @throws IllegalArgumentException if {@code retryInterval} is <= 0, {@code retryInterval} is >=
   *           {@code maxRetryInterval} or the {@code retryIntervalMultiplier} is <= 1
   */
  public RetryPolicy withBackoff(Duration retryInterval, Duration maxRetryInterval,
      int retryIntervalMultiplier) {
    Assert.notNull(retryInterval, "retryInterval");
    Assert.notNull(maxRetryInterval, "maxRetryInterval");
    Assert.isTrue(retryInterval.length > 0, "The retryInterval must be greater than 0");
    Assert.isTrue(retryInterval.length < maxRetryInterval.length,
        "The retryInterval must be less than the maxRetryInterval");
    Assert.isTrue(retryIntervalMultiplier > 1, "The retryIntervalMultiplier must be greater than 1");
    this.retryInterval = retryInterval;
    this.maxRetryInterval = maxRetryInterval;
    this.retryIntervalMultiplier = retryIntervalMultiplier;
    return this;
  }

  /**
   * Sets the max duration of the RetryPolicy. This is the max amount of time that retries will be
   * performed within, including the max amount of time to wait for resources (connections/channels)
   * to be recovered.
   * 
   * @throws NullPointerException if {@code maxDuration} is null
   */
  public RetryPolicy withMaxDuration(Duration maxDuration) {
    Assert.notNull(maxDuration, "maxDuration");
    this.maxDuration = maxDuration;
    return this;
  }

  /**
   * Sets the max retries to perform. -1 indicates to always retry. 0 indicates to never retry.
   */
  public RetryPolicy withMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  /**
   * Sets the {@code retryInterval} to pause for between retries.
   * 
   * @throws NullPointerException if {@code retryInterval} is null
   * @throws IllegalStateException if backoff intervals have already been set via
   *           {@link #withBackoff(Duration, Duration)} or
   *           {@link #withBackoff(Duration, Duration, int)}
   */
  public RetryPolicy withRetryInterval(Duration retryInterval) {
    Assert.notNull(retryInterval, "retryInterval");
    Assert.state(maxRetryInterval == null, "Backoff intervals have already been set");
    this.retryInterval = retryInterval;
    return this;
  }
}
