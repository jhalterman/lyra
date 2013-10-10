package net.jodah.lyra.internal;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.testng.annotations.Test;

/**
 * Tests connection recovery failures that occur via a connection's ShutdownListener.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ConnectionRecoveryTest extends AbstractRecoveryTest {
  /**
   * Asserts that a retryable connection closure via a ShutdownListener results in the connection
   * and channel being recovered.
   */
  public void shouldHandleRetryableConnectionClosure() throws Throwable {
    performRecovery(retryableConnectionShutdownSignal(), 4);
    verifyCxnCreations(5);
    verifyChannelCreations(1, 5); // Failed channel
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
  }

  /**
   * Asserts that an non-retryable connection closure via a ShutdownListener results in the failure
   * being re-thrown.
   */
  public void shouldHandleNonRetryableConnectionClosure() throws Throwable {
    performRecovery(nonRetryableConnectionShutdownSignal(), 4);
    verifyCxnCreations(5);
    verifyChannelCreations(1, 5); // Failed channel
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
  }

  @Override
  void mockRecovery(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(connection.createChannel(eq(1))).thenAnswer(failNTimes(3, e, mockChannel(1).channel));
  }
}
