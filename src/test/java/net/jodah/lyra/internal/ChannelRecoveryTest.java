package net.jodah.lyra.internal;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicies;
import net.jodah.lyra.event.DefaultChannelListener;
import net.jodah.lyra.util.Duration;

import org.testng.annotations.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Tests channel recovery failures that occur. The general structure of these tests is to mock some
 * resources, trigger a recovery, then assert that the expected number of resource creations
 * occurred.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ChannelRecoveryTest extends AbstractRecoveryTest {
  /**
   * Asserts that the channel is recovered when closed. Failed consumers will not be recovered after
   * their failed recovery attempts.
   */
  public void shouldHandleChannelClosures() throws Throwable {
    performRecovery(mockChannel(2).channelHandler, mockChannel(2).channelHandler, 0, 2);
    verifyCxnCreations(1);
    verifyChannelCreations(1, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);

    verifyChannelCreations(2, 3);
    verifyConsumerCreations(2, 5, 2); // Failed consumer
    verifyConsumerCreations(2, 6, 3);
  }

  /**
   * Asserts that the channel and connection are recovered. Failed consumers will not be recovered
   * after their failed recovery attempts.
   */
  public void shouldHandleChannelThenConnectionClosures() throws Throwable {
    performRecovery(mockChannel(2).channelHandler, connectionHandler, 2, 5);
    verifyCxnCreations(3);
    verifyChannelCreations(1, 3);
    verifyConsumerCreations(1, 1, 3);
    verifyConsumerCreations(1, 2, 3);

    verifyChannelCreations(2, 4);
    verifyConsumerCreations(2, 5, 4); // Failed consumer
    verifyConsumerCreations(2, 6, 4);
  }

  /**
   * Asserts that a failure from a channel listener during recovery results in the channel being
   * recovered.
   */
  public void shouldHandleRecoveryFailureFromChannelListener() throws Throwable {
    final AtomicBoolean shutdownCalled = new AtomicBoolean();
    config = new Config().withRetryPolicy(
        RetryPolicies.retryAlways().withInterval(Duration.millis(10)))
        .withRecoveryPolicy(RecoveryPolicies.recoverAlways())
        .withChannelListeners(new DefaultChannelListener() {
          @Override
          public void onRecovery(Channel channel) {
            if (!shutdownCalled.get() && channel == mockChannel(2).proxy) {
              ShutdownSignalException e = nonRetryableChannelShutdownSignal();
              shutdownCalled.set(true);
              callShutdownListener(mockChannel(2).channelHandler, e);
              throw e;
            }
          }
        });

    performRecovery(mockChannel(2).channelHandler, mockChannel(2).channelHandler, 0, 0);
    verifyCxnCreations(1);
    verifyChannelCreations(1, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);

    verifyChannelCreations(2, 3);
    verifyConsumerCreations(2, 5, 2); // Only attempted once since first attempt fails
    verifyConsumerCreations(2, 6, 3);
  }

  @Override
  void mockRecovery(Exception e, RetryableResource retryableResource) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(mockChannel(2).delegate.queueDelete("test-queue")).thenAnswer(
        failNTimes(2, e, null, retryableResource));
    when(
        mockChannel(2).delegate.basicConsume(eq("test-queue"),
            argThat(matcherFor(mockConsumer(2, 5))))).thenAnswer(
        failNTimes(2, e, "test-tag", retryableResource));
  }
}
