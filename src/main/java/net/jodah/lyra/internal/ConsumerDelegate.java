package net.jodah.lyra.internal;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Delegates consumer method invocations while tracking delivery information.
 * 
 * @author Jonathan Halterman
 */
public class ConsumerDelegate implements Consumer {
  private final ChannelHandler channelHandler;
  final Consumer delegate;
  private volatile boolean closed;

  ConsumerDelegate(ChannelHandler channelHandler, Consumer delegate) {
    this.channelHandler = channelHandler;
    this.delegate = delegate;
  }

  @Override
  public boolean equals(Object other) {
    return delegate.equals(other);
  }

  @Override
  public void handleCancel(String consumerTag) throws IOException {
    delegate.handleCancel(consumerTag);
  }

  @Override
  public void handleCancelOk(String consumerTag) {
    delegate.handleCancelOk(consumerTag);
  }

  @Override
  public void handleConsumeOk(String consumerTag) {
    delegate.handleConsumeOk(consumerTag);
  }

  @Override
  public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
      byte[] body) throws IOException {
    if (closed)
      return;
    long deliveryTag = envelope.getDeliveryTag();
    channelHandler.maxDeliveryTag = deliveryTag = deliveryTag
        + channelHandler.previousMaxDeliveryTag;
    delegate.handleDelivery(
        consumerTag,
        new Envelope(deliveryTag, envelope.isRedeliver(), envelope.getExchange(),
            envelope.getRoutingKey()), properties, body);
  }

  @Override
  public void handleRecoverOk(String consumerTag) {
    delegate.handleRecoverOk(consumerTag);
  }

  @Override
  public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    delegate.handleShutdownSignal(consumerTag, sig);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  void close() {
    closed = true;
  }

  void open() {
    closed = false;
  }
}
