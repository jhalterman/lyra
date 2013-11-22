package net.jodah.lyra.event;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;

/**
 * Listens for {@link Consumer} related events.
 * 
 * @author Jonathan Halterman
 */
public interface ConsumerListener {
  /**
   * Called before the {@code consumer} is recovered from an unexpected closure on the
   * {@code channel}. This is useful for performing any pre-consumer setup that is required such as
   * declaring exchanges and queues, and creating queue to exchange bindings.
   */
  void onBeforeRecovery(Consumer consumer, Channel channel);

  /**
   * Called after the {@code consumer} is recovered from an unexpected closure on the
   * {@code channel}.
   */
  void onAfterRecovery(Consumer consumer, Channel channel);

  /**
   * Called after the {@code consumer} fails to recover from an unexpected closure on the
   * {@code channel}.
   */
  void onRecoveryFailure(Consumer consumer, Channel channel, Throwable failure);
}
