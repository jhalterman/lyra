package net.jodah.lyra.event;

import com.rabbitmq.client.Channel;

/**
 * Listens for {@link Channel} related events.
 * 
 * @author Jonathan Halterman
 */
public interface ChannelListener {
  /**
   * Called after the {@code channel} is successfully created.
   */
  void onCreate(Channel channel);

  /**
   * Called after channel creation fails.
   */
  void onCreateFailure(Throwable failure);

  /**
   * Called after the {@code channel}, along with its consumers, is recovered from an unexpected
   * closure.
   */
  void onRecovery(Channel channel);

  /**
   * Called after the {@code channel} fails to recover from an unexpected closure.
   */
  void onRecoveryFailure(Channel channel, Throwable failure);
}
