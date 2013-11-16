package net.jodah.lyra.internal;

import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;

import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicies;
import net.jodah.lyra.event.DefaultChannelListener;
import net.jodah.lyra.util.Duration;

import org.testng.annotations.Test;

import com.rabbitmq.client.Channel;

/**
 * Tests channel recovery failures that occur via a connection's ShutdownListener. The general
 * structure of these tests is to mock some resources, trigger a recovery, then assert that the
 * expected number of resource creations occurred.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ChannelRecoveryTest extends AbstractRecoveryTest {
  /**
   * Asserts that a retryable channel closure via a ShutdownListener results in the channel being
   * recovered.
   */
  public void shouldHandleRetryableChannelClosure() throws Throwable {
    performRecovery(retryableChannelShutdownSignal(), 1);
    verifyChannelFailure();
  }

  /**
   * Asserts that a non-retryable channel closure via a ShutdownListener results in the channel
   * being recovered, but the failed consumer not being recovered.
   */
  public void shouldHandleNonRetryableChannelClosure() throws Throwable {
    performRecovery(nonRetryableChannelShutdownSignal(), 1);
    verifyChannelFailure();
  }

  /**
   * Asserts that a retryable connection closure via a ShutdownListener results in the connection
   * and channel being recovered.
   */
  public void shouldHandleRetryableConnectionClosure() throws Throwable {
    performRecovery(retryableConnectionShutdownSignal(), 4);
    verifyConnectionFailure();
  }

  /**
   * Asserts that an non-retryable connection closure via a ShutdownListener results in the failure
   * being re-thrown and logged.
   */
  public void shouldHandleNonRetryableConnectionClosure() throws Throwable {
    performRecovery(nonRetryableConnectionShutdownSignal(), 4);
    verifyConnectionFailure();
  }

  /**
   * Asserts that a failure from a channel listener during recovery results in the channel being
   * recovered.
   */
  public void shouldHandleRecoveryFailureFromChannelListener() throws Throwable {
    config = new Config().withRetryPolicy(
        RetryPolicies.retryAlways().withInterval(Duration.millis(10)))
        .withRecoveryPolicy(RecoveryPolicies.recoverAlways())
        .withChannelListeners(new DefaultChannelListener() {
          @Override
          public void onRecovery(Channel channel) {
            try {
              channel.queueDelete("test-queue");
            } catch (IOException e) {
            }
          }
        });

    performRecovery(nonRetryableChannelShutdownSignal(), 1);
    verifyCxnCreations(2);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 7);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 4);
    verifyConsumerCreations(2, 6, 7);
  }

  /**
   * All channel recoveries should result in the same number of invocations since we're not retrying
   * invocations.
   */
  private void verifyChannelFailure() throws Throwable {
    verifyCxnCreations(2);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 3);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2); // Failed consumer
    verifyConsumerCreations(2, 6, 3);
  }

  /**
   * All connection recoveries should result in the same number of invocations since we're not
   * retrying invocations.
   */
  private void verifyConnectionFailure() throws Throwable {
    verifyCxnCreations(5);
    verifyChannelCreations(1, 5);
    verifyChannelCreations(2, 5);
    verifyConsumerCreations(1, 1, 5);
    verifyConsumerCreations(1, 2, 5);
    verifyConsumerCreations(2, 5, 5);
    verifyConsumerCreations(2, 6, 5);
  }

  @Override
  void mockRecovery(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(mockChannel(2).delegate.queueDelete("test-queue")).thenAnswer(
        failNTimes(2, e, null, mockChannel(2).channelHandler));
    when(
        mockChannel(2).delegate.basicConsume(eq("test-queue"),
            argThat(matcherFor(mockConsumer(2, 5))))).thenAnswer(
        failNTimes(3, e, "test-tag", mockChannel(2).channelHandler));
  }
}
