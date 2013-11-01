package net.jodah.lyra.convention;

import net.jodah.lyra.config.RecoveryPolicy;

/**
 * Factory methods for recovery policies.
 * 
 * @author Jonathan Halterman
 */
public final class RecoveryPolicies {
  private RecoveryPolicies() {
  }

  /**
   * Returns a RecoveryPolicy that never recovers.
   */
  public static RecoveryPolicy recoverNever() {
    return new RecoveryPolicy().withMaxAttempts(0);
  }

  /**
   * Returns a RecoveryPolicy that always recovers.
   */
  public static RecoveryPolicy recoverAlways() {
    return new RecoveryPolicy().withMaxAttempts(-1);
  }
}
