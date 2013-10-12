package net.jodah.lyra.internal;

import static org.mockito.Mockito.doThrow;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.jodah.lyra.LyraOptions;
import net.jodah.lyra.retry.RetryPolicies;

import org.testng.annotations.Test;

import com.rabbitmq.client.Channel;

@Test(groups = "functional")
public class ChannelPoolingTest extends AbstractFunctionalTest {
  private List<Channel> channels;

  public void shouldPoolChannelsOnClosure() throws Throwable {
    createPooledMockChannels();
    assertEquals(connectionHandler.pooledChannels.size(), 5);
  }

  public void shouldClearPoolOnConnectionClosure() throws Throwable {
    createPooledMockChannels();
    callShutdownListener(retryableConnectionShutdownSignal());
    assertTrue(connectionHandler.pooledChannels.isEmpty());
  }

  /**
   * Asserts that logically closed channels are recycled.
   */
  public void shouldRecycleChannels() throws Throwable {
    options = LyraOptions.forHost("localhost").withChannelPoolSize(5);
    mockConnection();
    MockChannel channel1 = mockChannel();
    channel1.proxy.close();
    MockChannel channel2 = mockChannel();
    assertEquals(channel1.delegate, channel2.delegate);
  }

  /**
   * Asserts that a channel failure results in a recovery that utilizes a pooled channel.
   */
  public void shouldRecoverChannelFromPooledChannel() throws Throwable {
    options = LyraOptions.forHost("localhost")
        .withChannelPoolSize(5)
        .withRecoveryPolicy(RetryPolicies.retryAlways());
    mockConnection();
    MockChannel initialChannel = mockChannel();
    MockChannel channel = mockChannel();
    doThrow(new IOException(nonRetryableChannelShutdownSignal())).when(channel.delegate)
        .basicRecover();

    try {
      assertTrue(connectionHandler.pooledChannels.isEmpty());
      initialChannel.proxy.close();
      assertEquals(connectionHandler.pooledChannels.size(), 1);
      channel.proxy.basicRecover();
      fail();
    } catch (IOException expected) {
      assertNotEquals(channel.delegate, initialChannel.delegate);
      assertEquals(delegateFor(channel.proxy), initialChannel.delegate);
      assertTrue(connectionHandler.pooledChannels.isEmpty());
    }
  }

  private void createPooledMockChannels() throws Throwable {
    options = LyraOptions.forHost("localhost").withChannelPoolSize(5);
    mockConnection();
    channels = new ArrayList<Channel>();
    for (int i = 0; i < 10; i++)
      channels.add(mockChannel().proxy);
    for (Channel channel : channels)
      channel.close();
  }
}
