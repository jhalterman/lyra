package net.jodah.lyra.internal;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.testng.annotations.Test;

/**
 * Tests connection recovery failuresr. The general structure of these tests is to mock some
 * resources, trigger a recovery, then assert that the expected number of resource creations
 * occurred.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ConnectionRecoveryTest extends AbstractRecoveryTest {
  /**
   * Asserts that the connection is recovered when closed.
   */
  public void shouldHandleConnectionClosures() throws Throwable {
    performRecovery(connectionHandler, connectionHandler, 3, 2);
    verifyCxnCreations(4);
    verifyChannelCreations(1, 4); // Failed connection
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
  }

  /**
   * Asserts that a connection closure with channel closures during recovery results in everything
   * being recovered.
   */
  public void shouldHandleConnectionThenChannelClosures() throws Throwable {
    performRecovery(connectionHandler, mockChannel(1).channelHandler, 1, 2);
    verifyCxnCreations(2);
    verifyChannelCreations(1, 4); // Failed channel
    verifyChannelCreations(2, 2);
    verifyConsumerCreations(1, 1, 2);
    verifyConsumerCreations(1, 2, 2);
    verifyConsumerCreations(2, 5, 2);
    verifyConsumerCreations(2, 6, 2);
  }

  @Override
  void mockRecovery(Exception e, RetryableResource retryableResource) throws IOException {
    mockConsumer(1, 1);
    mockConsumer(1, 2);
    mockConsumer(2, 5);
    mockConsumer(2, 6);

    when(connection.createChannel(eq(1))).thenAnswer(
        failNTimes(2, e, mockChannel(1).delegate, retryableResource));
  }
}
