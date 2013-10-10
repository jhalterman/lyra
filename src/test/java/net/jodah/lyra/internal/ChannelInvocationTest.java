package net.jodah.lyra.internal;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.testng.annotations.Test;

/**
 * Tests failures that occur as the result of a channel invocation.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ChannelInvocationTest extends AbstractInvocationTest {
  public void shouldThrowOnInvocationFailureWithNoRecoveryPolicy() {

  }

  /**
   * Asserts that a retryable channel closure on a channel invocation results in the channel being
   * recovered and the invocation being retried.
   */
  public void shouldHandleRetryableChannelClosure() throws Throwable {
    performInvocation(retryableChannelShutdownSignal());
    verifyCxnCreations(1);
    verifyChannelCreations(1, 3);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 3);
    verifyConsumerCreations(1, 2, 3);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(3);
  }

  /**
   * Asserts that a retryable channel closure on a channel invocation that fails with a retryable
   * recovery failure has the initial failure rethrown.
   */
  public void shouldHandleRetryableChannelClosureWithRetryableRecoveryFailure() throws Throwable {
    performThrowableInvocation(retryableChannelShutdownSignal(),
        retryableConnectionShutdownSignal());
    verifyCxnCreations(1);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  /**
   * Asserts that a retryable channel closure on a channel invocation that fails with a
   * non-retryable recovery failure has the initial failure rethrown.
   */
  public void shouldHandleRetryableChannelClosureWithNonRetryableRecoveryFailure() throws Throwable {
    performThrowableInvocation(retryableChannelShutdownSignal(), new IOException("test"));
    verifyCxnCreations(1);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  /**
   * Asserts that a non-retryable channel closure on a channel invocation results in the failure
   * being re-thrown and the invocation not being retried.
   */
  public void shouldHandleNonRetryableChannelClosure() throws Throwable {
    performThrowableInvocation(nonRetryableChannelShutdownSignal());
    verifyCxnCreations(1);
    verifyChannelCreations(1, 1);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  /**
   * Asserts that a retryable connection closure on a channel invocation results in the connection
   * and channel being recovered and the invocation being retried.
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
   * Asserts that an non-retryable connection closure on a channel invocation results in the
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

  public void retryableChannelClosureShouldInterruptWaiters() throws Throwable {
    performThrowableInvocation(nonRetryableConnectionShutdownSignal());
  }

  @Override
  void mockInvocation(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    doAnswer(failNTimes(2, e, null)).when(mockChannel(1).channel).basicCancel("foo-tag");
  }

  @Override
  void mockRecovery(Exception e) throws IOException {
    doThrow(e).when(connection).createChannel(eq(1));
  }

  @Override
  void performInvocation() throws IOException {
    mockChannel(1).channelProxy.basicCancel("foo-tag");
  }

  void verifyInvocations(int invocations) throws IOException {
    verify(mockChannel(1).channel, times(invocations)).basicCancel(eq("foo-tag"));
  }
}
