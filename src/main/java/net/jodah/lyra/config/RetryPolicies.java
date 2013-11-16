package net.jodah.lyra.config;


/**
 * Factory methods for retry policies.
 * 
 * @author Jonathan Halterman
 */
public final class RetryPolicies {
  private RetryPolicies() {
  }

  /**
   * Returns a RetryPolicy that never retries.
   */
  public static RetryPolicy retryNever() {
    return new RetryPolicy().withMaxAttempts(0);
  }

  /**
   * Returns a RetryPolicy that always retries.
   */
  public static RetryPolicy retryAlways() {
    return new RetryPolicy().withMaxAttempts(-1);
  }
}
