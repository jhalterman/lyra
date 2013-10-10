package net.jodah.lyra.internal;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.testng.annotations.Test;

/**
 * Tests failures that occur as the result of a connection invocation.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ConnectionInvocationTest extends AbstractInvocationTest {
  /**
   * Asserts that a retryable connection closure on a connection invocation results in the
   * connection and channel being recovered and the invocation being retried.
   */
  public void shouldHandleRetryableConnectionClosure() throws Throwable {
    performInvocation(retryableConnectionShutdownSignal());
    verifyCxnCreations(3);
    verifyChannelCreations(1, 3);
    verifyChannelCreations(2, 3);
    verifyConsumerCreations(1, 1, 3);
    verifyConsumerCreations(1, 2, 3);
    verifyConsumerCreations(2, 5, 3);
    verifyConsumerCreations(2, 6, 3);
    verifyInvocations(3);
  }

  /**
   * Asserts that an non-retryable connection closure on a connection invocation results in the
   * invocation failure being re-thrown and the invocation not being retried.
   */
  public void shouldHandleNonRetryableConnectionClosure() throws Throwable {
    performThrowableInvocation(nonRetryableConnectionShutdownSignal());
    verifyCxnCreations(2);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
    verifyInvocations(1);
  }

  @Override
  void mockInvocation(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(connection.createChannel()).thenAnswer(failNTimes(2, e, mockChannel(2).channel));
  }

  @Override
  void performInvocation() throws IOException {
    connectionProxy.createChannel();
  }

  void verifyInvocations(int invocations) throws IOException {
    verify(connection, times(invocations)).createChannel();
  }

  @Override
  void mockRecovery(Exception e) throws IOException {
  }
}
