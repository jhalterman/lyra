package net.jodah.lyra.internal;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.testng.annotations.Test;

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

    when(mockChannel(2).delegate.basicConsume(eq("test-queue"), eq(mockConsumer(2, 5)))).thenAnswer(
        failNTimes(3, e, "test-tag"));
  }
}
