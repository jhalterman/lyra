package net.jodah.lyra.internal;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicies;
import net.jodah.lyra.util.Duration;

import org.testng.annotations.Test;

/**
 * Tests failures that occur as the result of a connection invocation. The general structure of
 * these tests is to mock some resources, perform some failing invocation, then assert that the
 * expected number of resource creations and invocation retries occurred.
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

  /**
   * Asserts that a failed invocation results in the failure being rethrown immediately if retries
   * are not configured while recovery takes place in the background.
   */
  public void shouldThrowImmediatelyOnInvocationFailureWithNoRetryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryNever()).withRecoveryPolicy(
        RecoveryPolicies.recoverAlways());
    performThrowableInvocation(retryableConnectionShutdownSignal());

    // Assert that the connection is recovered asynchronously
    assertTrue(connectionHandler.circuit.await(Duration.secs(1)));
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
  public void shouldThrowOnInvocationFailureWithNoRecoveryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryAlways()).withRecoveryPolicy(
        RecoveryPolicies.recoverNever());
    performThrowableInvocation(retryableConnectionShutdownSignal());
    verifyCxnCreations(1);
    verifyChannelCreations(1, 1);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
    verifyInvocations(1);
  }

  @Override
  protected void mockInvocation(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(connection.createChannel()).thenAnswer(
        failNTimes(2, e, mockChannel(2).delegate, connectionHandler));
  }

  @Override
  protected void performInvocation() throws IOException {
    connectionProxy.createChannel();
  }

  @Override
  protected void mockRecovery(Exception e) throws IOException {
  }

  private void verifyInvocations(int invocations) throws IOException {
    verify(connection, times(invocations)).createChannel();
  }
}
