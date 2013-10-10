package net.jodah.lyra.event;

import com.rabbitmq.client.Consumer;

/**
 * Listens for {@link Consumer} related events.
 * 
 * @author Jonathan Halterman
 */
public interface ConsumerListener {
  /**
   * Called after the {@code consumer} is recovered from an unexpected closure.
   */
  void onRecovery(Consumer consumer);

  /**
   * Called after the {@code consumer} fails to recover from an unexpected closure.
   */
  void onRecoveryFailure(Consumer consumer, Throwable failure);
}
