package net.jodah.lyra.internal;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import static org.testng.Assert.*;
import org.testng.annotations.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

@Test(groups = "functional")
public class ConsumerRecoveryTest extends AbstractFunctionalTest {
  /**
   * Asserts that Lyra drops acks for stale delivery tags.
   */
  public void shouldDropAcksForStaleDeliveryTags() throws Throwable {
    mockConnection();
    final MockChannel mockChannel = mockChannel(1);
    final Consumer consumer = new DefaultConsumer(mockChannel.proxy) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
          AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
          System.out.println("Delivered " + envelope.getDeliveryTag());
          Thread.sleep(5);
          mockChannel.proxy.basicAck(envelope.getDeliveryTag(), false);
        } catch (Exception ignore) {
        }
      }
    };
    when(
        mockChannel.delegate.basicConsume(eq("test-queue"), eq(false), eq("test-tag"),
            argThat(matcherFor(consumer)))).thenReturn("test-tag");
    doAnswer(failNTimes(1, nonRetryableChannelShutdownSignal(), null, mockChannel.channelHandler)).when(
        mockChannel.delegate)
        .basicCancel("foo-tag");

    // Setup consumer
    mockChannel.proxy.basicConsume("test-queue", false, "test-tag", consumer);

    final AtomicInteger i = new AtomicInteger(1);
    final AtomicBoolean consume = new AtomicBoolean(true);
    new Thread() {
      public void run() {
        try {
          for (; i.get() <= 100000 && consume.get(); i.incrementAndGet()) {
            consumer.handleDelivery("test-tag", new Envelope(i.get(), false, "x", "#"), null,
                ("foo-" + i).getBytes());
            Thread.sleep(5);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();

    // Kill channel
    Thread.sleep(100);

    try {
      mockChannel.proxy.basicCancel("foo-tag");
      fail("Non-retryable failure should have thrown");
    } catch (Exception expected) {

    }

    // Stop delivering messages to consumer
    Thread.sleep(200);
    consume.set(false);

    // final Waiter waiter = new Waiter();
    // config.withConnectionListeners(new DefaultConnectionListener() {
    // @Override
    // public void onRecovery(Connection connection) {
    // waiter.resume();
    // }
    //
    // @Override
    // public void onRecoveryFailure(Connection connection, Throwable failure) {
    // waiter.resume();
    // }
    // });
    //
    // mockRecovery(e);
    // callShutdownListener(connectionHandler, retryableConnectionShutdownSignal());
    // waiter.await(1000, expectedShutdownListenerCalls);
    // Thread.sleep(100);
  }
}
