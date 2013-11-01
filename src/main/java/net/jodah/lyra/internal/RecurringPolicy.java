package net.jodah.lyra.internal;

import net.jodah.lyra.internal.util.Assert;
import net.jodah.lyra.util.Duration;

/**
 * Policy that defines recurring behavior.
 * 
 * @author Jonathan Halterman
 */
public abstract class RecurringPolicy<T extends RecurringPolicy<T>> {
  private int maxAttempts;
  private Duration maxDuration;
  private Duration interval;
  private Duration maxInterval;
  private int intervalMultiplier;

  /**
   * Creates a recovery policy that always attempts.
   */
  public RecurringPolicy() {
    maxAttempts = -1;
  }

  /**
   * Returns whether the policy allows any attempts based on the configured maxAttempts and
   * maxDuration.
   */
  public boolean allowsAttempts() {
    return (maxAttempts == -1 || maxAttempts > 0)
        && (maxDuration == null || maxDuration.length > 0);
  }

  /**
   * Returns the interval between attempts.
   * 
   * @see #withInterval(Duration)
   * @see #withBackoff(Duration, Duration)
   * @see #withBackoff(Duration, Duration, int)
   */
  public Duration getInterval() {
    return interval;
  }

  /**
   * Returns the interval multiplier for backoff attempts.
   * 
   * @see #withBackoff(Duration, Duration, int)
   */
  public int getIntervalMultiplier() {
    return intervalMultiplier;
  }

  /**
   * Returns the max attempts.
   * 
   * @see #withMaxAttempts(int)
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * Returns the max duration to perform attempts for.
   * 
   * @see #withMaxDuration(Duration)
   */
  public Duration getMaxDuration() {
    return maxDuration;
  }

  /**
   * Returns the max interval between backoff attempts.
   * 
   * @see #withBackoff(Duration, Duration)
   */
  public Duration getMaxInterval() {
    return maxInterval;
  }

  /**
   * Sets the {@code interval} to pause for between attempts, exponentially backing of to the
   * {@code maxInterval} multiplying successive intervals by a factor of 2.
   * 
   * @throws NullPointerException if {@code interval} or {@code maxInterval} are null
   * @throws IllegalArgumentException if {@code interval} is <= 0 or {@code interval} is >=
   *           {@code maxInterval}
   */
  public T withBackoff(Duration interval, Duration maxInterval) {
    return withBackoff(interval, maxInterval, 2);
  }

  /**
   * Sets the {@code interval} to pause for between attempts, exponentially backing of to the
   * {@code maxInterval} multiplying successive intervals by the {@code intervalMultiplier}.
   * 
   * @throws NullPointerException if {@code interval} or {@code maxInterval} are null
   * @throws IllegalArgumentException if {@code interval} is <= 0, {@code interval} is >=
   *           {@code maxInterval} or the {@code intervalMultiplier} is <= 1
   */
  @SuppressWarnings("unchecked")
  public T withBackoff(Duration interval, Duration maxInterval, int intervalMultiplier) {
    Assert.notNull(interval, "interval");
    Assert.notNull(maxInterval, "maxInterval");
    Assert.isTrue(interval.length > 0, "The interval must be greater than 0");
    Assert.isTrue(interval.length < maxInterval.length,
        "The interval must be less than the maxInterval");
    Assert.isTrue(intervalMultiplier > 1, "The intervalMultiplier must be greater than 1");
    this.interval = interval;
    this.maxInterval = maxInterval;
    this.intervalMultiplier = intervalMultiplier;
    return (T) this;
  }

  /**
   * Sets the {@code interval} to pause for between attempts.
   * 
   * @throws NullPointerException if {@code interval} is null
   * @throws IllegalStateException if backoff intervals have already been set via
   *           {@link #withBackoff(Duration, Duration)} or
   *           {@link #withBackoff(Duration, Duration, int)}
   */
  @SuppressWarnings("unchecked")
  public T withInterval(Duration interval) {
    Assert.notNull(interval, "interval");
    Assert.state(maxInterval == null, "Backoff intervals have already been set");
    this.interval = interval;
    return (T) this;
  }

  /**
   * Sets the max number of attempts to perform. -1 indicates to always attempt.
   */
  @SuppressWarnings("unchecked")
  public T withMaxAttempts(int maxAttempts) {
    this.maxAttempts = maxAttempts;
    return (T) this;
  }

  /**
   * Sets the max duration to perform attempts for.
   * 
   * @throws NullPointerException if {@code maxDuration} is null
   */
  @SuppressWarnings("unchecked")
  public T withMaxDuration(Duration maxDuration) {
    Assert.notNull(maxDuration, "maxDuration");
    this.maxDuration = maxDuration;
    return (T) this;
  }
}
