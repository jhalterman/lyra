package net.jodah.lyra.config;

import java.util.Collection;

import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.retry.RetryPolicy;

import com.rabbitmq.client.Connection;

/**
 * {@link Connection} related configuration.
 * 
 * @author Jonathan Halterman
 */
public interface ConnectionConfig extends ChannelConfig {
  /**
   * Returns the connection's listeners.
   * 
   * @see #withConnectionListeners(ConnectionListener...)
   */
  Collection<ConnectionListener> getConnectionListeners();

  /**
   * Returns the connection's recovery policy.
   * 
   * @see #withConnectionRecoveryPolicy(RetryPolicy)
   */
  RetryPolicy getConnectionRecoveryPolicy();

  /**
   * Returns the connection's retry policy.
   * 
   * @see #withConnectionRetryPolicy(RetryPolicy)
   */
  RetryPolicy getConnectionRetryPolicy();

  /**
   * Sets the {@code connectionListeners} to call on connection related events.
   */
  ConnectionConfig withConnectionListeners(ConnectionListener... connectionListeners);

  /**
   * Sets the policy to use for the recovery of Connections after an unexpected Connection closure.
   */
  ConnectionConfig withConnectionRecoveryPolicy(RetryPolicy recoveryPolicy);

  /**
   * Sets the policy to use for handling {@link Connection} invocation errors.
   */
  ConnectionConfig withConnectionRetryPolicy(RetryPolicy retryPolicy);
}
