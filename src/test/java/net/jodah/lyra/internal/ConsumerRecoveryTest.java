package net.jodah.lyra.internal;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

@Test(groups = { "functional", "slow" })
public class ConsumerRecoveryTest extends AbstractFunctionalTest {
  /**
   * Sets up a publisher, consumer, and an acker that acks consumed messages on a delay. Asserts
   * that Lyra drops acks for stale delivery tags after a recovery occurs.
   */
  public void shouldDropAcksForStaleDeliveryTags() throws Throwable {
    mockConnection();
    final MockChannel mockChannel = mockChannel(1);
    doAnswer(failNTimes(1, nonRetryableChannelShutdownSignal(), null, mockChannel.channelHandler)).when(
        mockChannel.delegate)
        .basicCancel("foo-tag");

    final LinkedBlockingQueue<Long> tags = new LinkedBlockingQueue<Long>();
    final Consumer consumer = new DefaultConsumer(mockChannel.proxy) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
          AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
          System.out.println("Received " + envelope.getDeliveryTag());
          // Kill channel
          if (envelope.getDeliveryTag() % 25 == 0)
            mockChannel.proxy.basicCancel("foo-tag");

          tags.add(envelope.getDeliveryTag());
          Thread.sleep(5);
        } catch (Exception ignore) {
        }
      }
    };

    // Setup consumer
    when(
        mockChannel.delegate.basicConsume(eq("test-queue"), eq(false), eq("test-tag"),
            argThat(matcherFor(consumer)))).thenReturn("test-tag");
    mockChannel.proxy.basicConsume("test-queue", false, "test-tag", consumer);
    final AtomicInteger deliveryTag = new AtomicInteger(1);

    // Deliver messages
    final AtomicBoolean consume = new AtomicBoolean(true);
    Invocation invocation = mockChannel.channelHandler.consumerInvocations.get("test-tag");
    final ConsumerDelegate consumerDelegate = (ConsumerDelegate) invocation.args[invocation.args.length - 1];
    new Thread() {
      public void run() {
        try {
          for (; deliveryTag.get() <= 100000 && consume.get(); deliveryTag.incrementAndGet()) {
            consumerDelegate.handleDelivery("test-tag", new Envelope(deliveryTag.get(), false, "x",
                "#"), null, ("foo-" + deliveryTag).getBytes());
            Thread.sleep(5);
          }
        } catch (Exception e) {
        }
      }
    }.start();

    // Acker
    Thread.sleep(100);
    long tag = 0;
    while (tag < 100) {
      try {
        tag = tags.take();
        Thread.sleep(10);
        mockChannel.proxy.basicAck(tag, false);
      } catch (Exception e) {
      }
    }

    consume.set(false);
    verify(mockChannel.delegate, never()).basicAck(eq(25L), eq(false));
    verify(mockChannel.delegate).basicAck(eq(26L), eq(false));
  }
}
