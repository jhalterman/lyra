package net.jodah.lyra.config;

import java.util.Collection;

import net.jodah.lyra.event.ChannelListener;

import com.rabbitmq.client.Channel;

/**
 * {@link Channel} related configuration.
 * 
 * @author Jonathan Halterman
 */
public interface ChannelConfig extends ConsumerConfig {
  /**
   * Returns the channel's listeners else empty list if none were configured.
   * 
   * @see #withChannelListeners(ChannelListener...)
   */
  Collection<ChannelListener> getChannelListeners();

  /**
   * Returns the channel's recovery policy.
   * 
   * @see #withChannelRecoveryPolicy(RecoveryPolicy)
   */
  RecoveryPolicy getChannelRecoveryPolicy();

  /**
   * Returns the channel's retry policy.
   * 
   * @see #withChannelRetryPolicy(RetryPolicy)
   */
  RetryPolicy getChannelRetryPolicy();

  /**
   * Sets the {@code channelListeners} to call on channel related events.
   */
  ChannelConfig withChannelListeners(ChannelListener... channelListeners);

  /**
   * Sets the {@code recoveryPolicy} to use for recovering the channel.
   */
  ChannelConfig withChannelRecoveryPolicy(RecoveryPolicy recoveryPolicy);

  /**
   * Sets the {@code retryPolicy} to use for retrying failed invocations on the channel.
   */
  ChannelConfig withChannelRetryPolicy(RetryPolicy retryPolicy);
}
