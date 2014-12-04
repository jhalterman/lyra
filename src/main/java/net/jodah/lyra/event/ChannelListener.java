package net.jodah.lyra.event;

import com.rabbitmq.client.Channel;

/**
 * Listens for {@link Channel} related events.
 * 
 * @author Jonathan Halterman
 */
public interface ChannelListener {
  /**
   * Called when the {@code channel} is successfully created.
   */
  void onCreate(Channel channel);

  /**
   * Called when channel creation fails.
   */
  void onCreateFailure(Throwable failure);

  /**
   * Called when recovery of the {@code channel} is started.
   */
  void onRecoveryStarted(Channel channel);

  /**
   * Called when the {@code channel} is successfully recovered from an unexpected closure but before
   * its consumers and their associated queues, exchanges, and bindings are recovered. This is
   * useful for performing any pre-consumer setup that is required such as declaring exchanges and
   * queues, and creating queue to exchange bindings.
   */
  void onRecovery(Channel channel);

  /**
   * Called when recovery of the {@code channel} and its consumers is completed. Note: The success
   * or failure of an individual consumer's recovery can be tracked with a {@link ConsumerListener}.
   */
  void onRecoveryCompleted(Channel channel);

  /**
   * Called when the {@code channel} fails to recover from an unexpected closure.
   */
  void onRecoveryFailure(Channel channel, Throwable failure);
}
