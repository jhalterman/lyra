package net.jodah.lyra.internal;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.net.ConnectException;
import java.util.concurrent.ExecutorService;

import net.jodah.lyra.config.Config;
import net.jodah.lyra.retry.RetryPolicies;

import org.testng.annotations.Test;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Tests failures that occur as the result of a connection factory invocation.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ConnectionFactoryInvocationTest extends AbstractFunctionalTest {
  /**
   * Asserts that a retryable connect failure results in the connection eventually succeeding.
   */
  public void shouldHandleRetryableConnectFailure() throws Throwable {
    mockConnectionOnly();
    connectionFactory = mock(ConnectionFactory.class);
    when(connectionFactory.newConnection(any(ExecutorService.class), any(Address[].class))).thenAnswer(
        failNTimes(3, new ConnectException("fail"), connection));
    mockConnection();
    verifyCxnCreations(4);
  }

  /**
   * Asserts that an non-retryable connect failure results in the connection being rethrown.
   */
  public void shouldHandleNonRetryableConnectFailure() throws Throwable {
    connectionFactory = mock(ConnectionFactory.class);
    connection = mock(Connection.class);
    when(connectionFactory.newConnection(any(ExecutorService.class), any(Address[].class))).thenAnswer(
        failNTimes(3, new RuntimeException(), connection));

    try {
      mockConnection();
      fail();
    } catch (Exception expected) {
    }

    verifyCxnCreations(1);
  }

  /**
   * Asserts that invocation failures are rethrown when a retry policy is not set.
   */
  public void shouldThrowOnInvocationFailureWithNoRetryPolicy() throws Throwable {
    config = new Config().withRetryPolicy(RetryPolicies.retryNever());
    connectionFactory = mock(ConnectionFactory.class);
    connection = mock(Connection.class);
    when(connectionFactory.newConnection(any(ExecutorService.class), any(Address[].class))).thenAnswer(
        failNTimes(3, new ConnectException("fail"), connection));

    try {
      mockConnection();
      fail();
    } catch (Exception expected) {
    }

    verifyCxnCreations(1);
  }
}
