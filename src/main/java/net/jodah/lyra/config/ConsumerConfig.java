package net.jodah.lyra.config;

import java.util.Collection;

import net.jodah.lyra.event.ConsumerListener;

public interface ConsumerConfig {
  /**
   * Returns the consumer listeners.
   */
  Collection<ConsumerListener> getConsumerListeners();

  /**
   * Returns whether consumer recovery is enabled. Defaults to true when channel recovery is
   * configured.
   */
  boolean isConsumerRecoveryEnabled();

  /**
   * Sets the {@code consumerListeners} to call on consumer related events.
   */
  ConnectionConfig withConsumerListeners(ConsumerListener... consumerListeners);

  /**
   * Sets whether consumer recovery is enabled or not.
   */
  ConsumerConfig withConsumerRecovery(boolean enabled);
}
