package net.jodah.lyra.internal;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.testng.annotations.Test;

import com.rabbitmq.client.AMQP.Exchange;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;

/**
 * Tests that exchanges, queues and bindings are recovered when a connection or channel are
 * unexpectedly shutdown.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ResourceRecoveryTest extends AbstractRecoveryTest {
  /**
   * Asserts that resources are recovered when a connection is closed.
   */
  public void shouldRecoverResourcesForConnectionClosures() throws Throwable {
    performRecovery(connectionHandler, connectionHandler, 3, 1);

    verifyCxnCreations(4);
    verifyRecoveryChannelCreations(3);
    verify(mockRecoveryChannel(), times(3)).exchangeDeclare("test-exchange", "topic");
    verify(mockRecoveryChannel()).queueDeclare("test-queue", false, false, true, null);
    verify(mockRecoveryChannel()).queueBind("test-queue", "test-exchange", "#", null);
    verifyChannelCreations(1, 2);
  }

  /**
   * Asserts that resources are recovered when a connection is closed then channels are closed
   * during recovery.
   */
  public void shouldRecoverResourcesForConnectionThenChannelClosures() throws Throwable {
    performRecovery(connectionHandler, null, 1, 1);

    verifyCxnCreations(2);
    verifyRecoveryChannelCreations(1);
    verify(mockRecoveryChannel()).exchangeDeclare("test-exchange", "topic");
    verify(mockRecoveryChannel()).queueDeclare("test-queue", false, false, true, null);
    verify(mockRecoveryChannel()).queueBind("test-queue", "test-exchange", "#", null);
    verifyChannelCreations(1, 2);
  }

  /**
   * Asserts that resources are recovered when a channel is closed then channels are closed during
   * recovery.
   */
  public void shouldRecoverResourcesForChannelClosures() throws Throwable {
    performRecovery(mockChannel(1).channelHandler, mockChannel(1).channelHandler, 0, 2);

    verifyCxnCreations(1);
    verifyRecoveryChannelCreations(0);
    verify(mockChannel(1).delegate, times(2)).exchangeDeclare("test-exchange", "topic");
    verify(mockChannel(1).delegate, times(1)).queueDeclare("test-queue", false, false, true, null);
    verify(mockChannel(1).delegate, times(1)).queueBind("test-queue", "test-exchange", "#", null);
    verifyChannelCreations(1, 3);
  }

  @Override
  @SuppressWarnings("unchecked")
  void createResources() throws IOException {
    Channel adminChannel = mockRecoveryChannel();
    MockChannel channel = mockChannel(1);

    Queue.DeclareOk declareOk = mock(Queue.DeclareOk.class);
    when(declareOk.getQueue()).thenReturn("test-queue");
    when(
        adminChannel.queueDeclare(eq("test-queue"), anyBoolean(), anyBoolean(), anyBoolean(),
            anyMap())).thenReturn(declareOk);
    when(
        channel.delegate.queueDeclare(eq("test-queue"), anyBoolean(), anyBoolean(), anyBoolean(),
            anyMap())).thenReturn(declareOk);

    channel.proxy.exchangeDeclare("test-exchange", "topic");
    channel.proxy.queueDeclare("test-queue", false, false, true, null);
    channel.proxy.queueBind("test-queue", "test-exchange", "#", null);
  }

  @Override
  void mockRecovery(Exception e, RetryableResource retryableResource) throws IOException {
    mockConsumer(1, 5);

    // Recovery fails 2 times
    when(mockChannel(1).delegate.exchangeDeclare(anyString(), anyString())).thenAnswer(
        failNTimes(2, e, mock(Exchange.DeclareOk.class), retryableResource));
    when(mockRecoveryChannel().exchangeDeclare(anyString(), anyString())).thenAnswer(
        failNTimes(2, e, mock(Exchange.DeclareOk.class), retryableResource));
  }
}
