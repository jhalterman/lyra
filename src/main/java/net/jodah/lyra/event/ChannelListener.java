package net.jodah.lyra.event;

import com.rabbitmq.client.Channel;

/**
 * Listens for {@link Channel} related events.
 * 
 * @author Jonathan Halterman
 */
public interface ChannelListener {
  /**
   * Called after the {@code channel} and its consumers are recovered from an unexpected closure.
   */
  void onConsumerRecovery(Channel channel);

  /**
   * Called after the {@code channel} is successfully created.
   */
  void onCreate(Channel channel);

  /**
   * Called after channel creation fails.
   */
  void onCreateFailure(Throwable failure);

  /**
   * Called after the {@code channel} is recovered from an unexpected closure, but before its
   * consumers along with associated queues, exchanges, and bindings are recovered. This is useful
   * for performing any pre-consumer setup that is required such as declaring exchanges and queues,
   * and creating queue to exchange bindings.
   */
  void onRecovery(Channel channel);

  /**
   * Called after the {@code channel} fails to recover from an unexpected closure.
   */
  void onRecoveryFailure(Channel channel, Throwable failure);
}
