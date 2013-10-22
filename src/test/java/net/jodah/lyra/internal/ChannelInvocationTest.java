package net.jodah.lyra.internal;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import net.jodah.lyra.config.Config;
import net.jodah.lyra.retry.RetryPolicies;
import net.jodah.lyra.util.Duration;

import org.jodah.concurrentunit.Waiter;
import org.testng.annotations.Test;

/**
 * Tests failures that occur as the result of a channel invocation.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ChannelInvocationTest extends AbstractInvocationTest {
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
   * Asserts that a retryable channel closure on a channel invocation that fails with a recovery
   * that suffers a connection failure has the failure rethrown.
   * 
   * TODO In the future we'd like the invocation to be retried.
   */
  public void shouldHandleRetryableChannelClosureWithRetryableRecoveryFailure() throws Throwable {
    performThrowableInvocation(retryableChannelShutdownSignal(),
        retryableConnectionShutdownSignal());
    verifyCxnCreations(3);
    verifyChannelCreations(1, 4);
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 4);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
    verifyInvocations(1);
  }

  /**
   * Asserts that a retryable channel closure on a channel invocation that fails with a
   * non-retryable recovery failure has the invocation retried.
   */
  public void shouldHandleRetryableChannelClosureWithNonRetryableRecoveryFailure() throws Throwable {
    performInvocation(retryableChannelShutdownSignal(), new IOException("test"));
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
   * Asserts that a non-retryable channel closure on a channel invocation results in the failure
   * being re-thrown and the invocation not being retried.
   */
  public void shouldHandleNonRetryableChannelClosure() throws Throwable {
    performThrowableInvocation(nonRetryableChannelShutdownSignal());
    verifyCxnCreations(1);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
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

  /**
   * Asserts that a failed invocation results in the failure being rethrown immediately if retries
   * are not configured while recovery takes place in the background.
   */
  public void shouldThrowOnChannelShutdownWithNoRetryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryNever()).withRecoveryPolicy(
        RetryPolicies.retryAlways());
    performThrowableInvocation(retryableChannelShutdownSignal());

    // Assert that the channel is recovered asynchronously
    assertTrue(mockChannel(1).channelHandler.circuit.await(Duration.secs(1)));
    verifyCxnCreations(1);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  /**
   * Asserts that invocation failures are rethrown when a channel is shutdown and a recovery policy
   * is not set.
   */
  public void shouldThrowOnChannelShutdownWithNoRecoveryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryAlways()).withRecoveryPolicy(
        RetryPolicies.retryNever());
    performThrowableInvocation(retryableChannelShutdownSignal());
    verifySingleInvocation();
  }

  /**
   * Asserts that invocation failures are rethrown when a connection is shutdown and a retry policy
   * is not set, but the connection and channel should still be recovered.
   */
  public void shouldThrowOnConnectionShutdownWithNoRetryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryNever()).withRecoveryPolicy(
        RetryPolicies.retryAlways());
    performThrowableInvocation(retryableConnectionShutdownSignal());

    // Assert that the channel is recovered asynchronously
    assertTrue(mockChannel(1).channelHandler.circuit.await(Duration.secs(1)));
    verifyCxnCreations(2);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
    verifyInvocations(1);
  }

  /**
   * Asserts that invocation failures are rethrown when a connection is shutdown and a recovery
   * policy is not set.
   */
  public void shouldThrowOnConnectionShutdownWithNoRecoveryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryAlways()).withRecoveryPolicy(
        RetryPolicies.retryNever());
    performThrowableInvocation(retryableConnectionShutdownSignal());
    verifySingleInvocation();
  }

  /**
   * Asserts that invocation failures are rethrown when a connection is shutdown and a connection
   * recovery policy is not set even when a channel recovery policy is set.
   */
  public void shouldThrowOnConnectionShutdownWithNoCxnRecoveryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryAlways())
        .withConnectionRecoveryPolicy(RetryPolicies.retryNever())
        .withChannelRecoveryPolicy(RetryPolicies.retryAlways());
    performThrowableInvocation(retryableConnectionShutdownSignal());
    verifySingleInvocation();
  }

  /**
   * Asserts that invocation failures are rethrown when a channel is shutdown and a connection
   * recovery policy is set but a channel recovery policy is not.
   */
  public void shouldThrowOnConnectionShutdownWithNoChannelRecoveryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryAlways())
        .withConnectionRecoveryPolicy(RetryPolicies.retryAlways())
        .withChannelRecoveryPolicy(RetryPolicies.retryNever());
    performThrowableInvocation(retryableConnectionShutdownSignal());
    verifyCxnCreations(2);
    verifyChannelCreations(1, 1);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  /**
   * Asserts that concurrent failed invocations result in recovery being performed serially.
   */
  @Test(groups = "problematic")
  public void shouldHandleConcurrentRetryableFailures() throws Throwable {
    mockConnection();
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    doAnswer(failNTimes(4, retryableChannelShutdownSignal(), null)).when(mockChannel(1).delegate)
        .basicCancel("foo-tag");

    final Waiter waiter = new Waiter();
    for (int i = 0; i < 2; i++)
      runInThread(new Runnable() {
        public void run() {
          try {
            performInvocation();
            waiter.resume();
          } catch (Throwable t) {
            waiter.fail(t);
          }
        }
      });

    waiter.await(2000, 2);

    // Even though both threads will fail twice, only one thread will perform recovery
    verifyCxnCreations(1);
    verifyChannelCreations(1, 3);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 3);
    verifyConsumerCreations(1, 2, 3);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    // Initial failure plus two retries for each thread
    verifyInvocations(6);
  }

  @Override
  protected void mockInvocation(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    doAnswer(failNTimes(2, e, null)).when(mockChannel(1).delegate).basicCancel("foo-tag");
  }

  @Override
  protected void mockRecovery(Exception e) throws IOException {
    when(mockChannel(1).delegate.basicConsume(eq("test-queue"), eq(mockConsumer(1, 1)))).thenAnswer(
        failNTimes(2, e, "test-tag"));
  }

  @Override
  protected void performInvocation() throws IOException {
    mockChannel(1).proxy.basicCancel("foo-tag");
  }

  private void verifySingleInvocation() throws Throwable {
    verifyCxnCreations(1);
    verifyChannelCreations(1, 1);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  private void verifyInvocations(int invocations) throws IOException {
    verify(mockChannel(1).delegate, times(invocations)).basicCancel(eq("foo-tag"));
  }
}
