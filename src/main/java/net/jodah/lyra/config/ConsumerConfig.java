package net.jodah.lyra.config;

import java.util.Collection;

import net.jodah.lyra.event.ConsumerListener;

import com.rabbitmq.client.Consumer;

/**
 * {@link Consumer} related configuration.
 * 
 * @author Jonathan Halterman
 */
public interface ConsumerConfig {
  /**
   * Returns the consumer listeners else empty list if none were configured.
   * 
   * @see #getConsumerListeners()
   */
  Collection<ConsumerListener> getConsumerListeners();

  /**
   * Returns whether consumer recovery is enabled. Defaults to true when channel recovery is
   * configured.
   * 
   * @see #withConsumerRecovery(boolean)
   */
  boolean isConsumerRecoveryEnabled();

  /**
   * Sets the {@code consumerListeners} to call on consumer related events.
   */
  ConsumerConfig withConsumerListeners(ConsumerListener... consumerListeners);

  /**
   * Sets whether consumer recovery is enabled or not.
   */
  ConsumerConfig withConsumerRecovery(boolean enabled);
}
