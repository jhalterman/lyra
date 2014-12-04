package net.jodah.lyra.event;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;

/**
 * No-op consumer listener for sub-classing.
 * 
 * @author Jonathan Halterman
 */
public abstract class DefaultConsumerListener implements ConsumerListener {
  @Override
  public void onRecoveryCompleted(Consumer consumer, Channel channel) {
  }

  @Override
  public void onRecoveryFailure(Consumer consumer, Channel channel, Throwable failure) {
  }

  @Override
  public void onRecoveryStarted(Consumer consumer, Channel channel) {
  }
}
