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
   * Called when recovery of the {@code consumer} on the {@code channel} is started. This is useful
   * for performing any pre-consumer setup that is required such as declaring exchanges and queues,
   * and creating queue to exchange bindings.
   */
  void onRecoveryStarted(Consumer consumer, Channel channel);

  /**
   * Called when recovery of the {@code consumer} on the {@code channel} is successfully completed.
   */
  void onRecoveryCompleted(Consumer consumer, Channel channel);

  /**
   * Called when the {@code consumer} fails to recover from an unexpected closure on the
   * {@code channel}.
   */
  void onRecoveryFailure(Consumer consumer, Channel channel, Throwable failure);
}
