package net.jodah.lyra.internal;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.testng.annotations.Test;

/**
 * Tests channel recovery failures that occur via a connection's ShutdownListener.
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
    verifyCxnCreations(2);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 3);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2); // Failed consumer
    verifyConsumerCreations(2, 6, 3);
  }

  /**
   * Asserts that a non-retryable channel closure via a ShutdownListener results in the failure
   * being re-thrown.
   */
  public void shouldHandleNonRetryableChannelClosure() throws Throwable {
    performRecovery(nonRetryableChannelShutdownSignal(), 1);
    verifyCxnCreations(2);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
  }

  /**
   * Asserts that a retryable connection closure via a ShutdownListener results in the connection
   * and channel being recovered.
   */
  public void shouldHandleRetryableConnectionClosure() throws Throwable {
    performRecovery(retryableConnectionShutdownSignal(), 4);
    verifyCxnCreations(5);
    verifyChannelCreations(1, 5);
    verifyChannelCreations(2, 5);
    verifyConsumerCreations(1, 1, 5);
    verifyConsumerCreations(1, 2, 5);
    verifyConsumerCreations(2, 5, 5);
    verifyConsumerCreations(2, 6, 5);
  }

  /**
   * Asserts that an non-retryable connection closure via a ShutdownListener results in the failure
   * being re-thrown.
   */
  public void shouldHandleNonRetryableConnectionClosure() throws Throwable {
    performRecovery(nonRetryableConnectionShutdownSignal(), 4);
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

    when(mockChannel(2).delegate.basicConsume(eq("test-queue"), eq(mockConsumer(2, 5)))).thenAnswer(
        failNTimes(3, e, "test-tag"));
  }
}
