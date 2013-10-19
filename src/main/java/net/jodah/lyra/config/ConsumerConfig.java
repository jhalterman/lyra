package net.jodah.lyra.config;

import java.util.Collection;

import net.jodah.lyra.event.ConsumerListener;

public interface ConsumerConfig {
  /**
   * Returns the consumer listeners.
   */
  Collection<ConsumerListener> getConsumerListeners();

  /**
   * Sets the {@code consumerListeners} to call on consumer related events.
   */
  ConnectionConfig withConsumerListeners(ConsumerListener... consumerListeners);
}
