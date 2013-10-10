package net.jodah.lyra.internal;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.ExecutorService;

import org.testng.annotations.Test;

import com.rabbitmq.client.Address;

/**
 * Tests connection recovery failures that occur via a connection's ShutdownListener.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ConnectionFactoryRecoveryTest extends AbstractRecoveryTest {
  /**
   * Asserts that a retryable connect failure via a ShutdownListener results in the connection and
   * channel being recovered.
   */
  public void shouldHandleRetryableConnectFailure() throws Throwable {
    performRecovery(new ConnectException("fail"), 1);
    verifyCxnCreations(5);
    verifyChannelCreations(1, 2);
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
  }

  /**
   * Asserts that an non-retryable connect failure via a ShutdownListener results in the failure
   * being re-thrown.
   */
  public void shouldHandleNonRetryableConnectFailure() throws Throwable {
    performRecovery(new RuntimeException(), 1);
    verifyCxnCreations(2);
    verifyChannelCreations(1, 1);
    verifyChannelCreations(2, 1);
    verifyConsumerCreations(1, 1, 1);
    verifyConsumerCreations(1, 2, 1);
    verifyConsumerCreations(2, 5, 1);
    verifyConsumerCreations(2, 6, 1);
  }

  @Override
  void mockRecovery(Exception e) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(connectionFactory.newConnection(any(ExecutorService.class), any(Address[].class))).thenAnswer(
        failNTimes(3, e, connection));
  }
}