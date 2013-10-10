package net.jodah.lyra.event;

import com.rabbitmq.client.Consumer;

/**
 * No-op listener for sub-classing.
 * 
 * @author Jonathan Halterman
 */
public abstract class DefaultConsumerListener implements ConsumerListener {
  @Override
  public void onRecovery(Consumer consumer) {
  }

  @Override
  public void onRecoveryFailure(Consumer consumer, Throwable failure) {
  }
}
