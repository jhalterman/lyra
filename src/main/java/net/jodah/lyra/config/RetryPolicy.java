package net.jodah.lyra.config;

import net.jodah.lyra.internal.RecurringPolicy;
import net.jodah.lyra.util.Duration;

/**
 * Policy that defines how retries should be performed.
 * 
 * @author Jonathan Halterman
 */
public class RetryPolicy extends RecurringPolicy<RetryPolicy> {
  /**
   * Creates a retry policy that retries forever.
   */
  public RetryPolicy() {
    super();
  }

  /**
   * Returns whether the policy allows any attempts based on the configured maxAttempts and
   * maxDuration.
   */
  @Override
  public boolean allowsAttempts() {
    return super.allowsAttempts();
  }

  /**
   * Returns the interval between attempts.
   * 
   * @see #withInterval(Duration)
   * @see #withBackoff(Duration, Duration)
   * @see #withBackoff(Duration, Duration, int)
   */
  @Override
  public Duration getInterval() {
    return super.getInterval();
  }

  /**
   * Returns the interval multiplier for backoff attempts.
   * 
   * @see #withBackoff(Duration, Duration, int)
   */
  @Override
  public int getIntervalMultiplier() {
    return super.getIntervalMultiplier();
  }

  /**
   * Returns the max attempts.
   * 
   * @see #withMaxAttempts(int)
   */
  @Override
  public int getMaxAttempts() {
    return super.getMaxAttempts();
  }

  /**
   * Returns the max duration to perform attempts for.
   * 
   * @see #withMaxDuration(Duration)
   */
  @Override
  public Duration getMaxDuration() {
    return super.getMaxDuration();
  }

  /**
   * Returns the max interval between backoff attempts.
   * 
   * @see #withBackoff(Duration, Duration)
   */
  @Override
  public Duration getMaxInterval() {
    return super.getMaxInterval();
  }

  /**
   * Sets the {@code interval} to pause for between attempts, exponentially backing of to the
   * {@code maxInterval} multiplying successive intervals by a factor of 2.
   * 
   * @throws NullPointerException if {@code interval} or {@code maxInterval} are null
   * @throws IllegalArgumentException if {@code interval} is <= 0 or {@code interval} is >=
   *           {@code maxInterval}
   */
  @Override
  public RetryPolicy withBackoff(Duration interval, Duration maxInterval) {
    return super.withBackoff(interval, maxInterval);
  }

  /**
   * Sets the {@code interval} to pause for between attempts, exponentially backing of to the
   * {@code maxInterval} multiplying successive intervals by the {@code intervalMultiplier}.
   * 
   * @throws NullPointerException if {@code interval} or {@code maxInterval} are null
   * @throws IllegalArgumentException if {@code interval} is <= 0, {@code interval} is >=
   *           {@code maxInterval} or the {@code intervalMultiplier} is <= 1
   */
  @Override
  public RetryPolicy withBackoff(Duration interval, Duration maxInterval, int intervalMultiplier) {
    return super.withBackoff(interval, maxInterval, intervalMultiplier);
  }

  /**
   * Sets the {@code interval} to pause for between attempts.
   * 
   * @throws NullPointerException if {@code interval} is null
   * @throws IllegalStateException if backoff intervals have already been set via
   *           {@link #withBackoff(Duration, Duration)} or
   *           {@link #withBackoff(Duration, Duration, int)}
   */
  @Override
  public RetryPolicy withInterval(Duration interval) {
    return super.withInterval(interval);
  }

  /**
   * Sets the max number of attempts to perform. -1 indicates to always attempt.
   */
  @Override
  public RetryPolicy withMaxAttempts(int maxAttempts) {
    return super.withMaxAttempts(maxAttempts);
  }

  /**
   * Sets the max duration to perform attempts for.
   * 
   * @throws NullPointerException if {@code maxDuration} is null
   */
  @Override
  public RetryPolicy withMaxDuration(Duration maxDuration) {
    return super.withMaxDuration(maxDuration);
  }
}
