package net.jodah.lyra.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import net.jodah.lyra.LyraOptions;

import org.testng.annotations.Test;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;

@Test
public class ChannelHandlerTest extends AbstractFunctionalTest {
  @Test(expectedExceptions = AlreadyClosedException.class)
  public void shouldThrowOnAlreadyClosedChannelInvocation() throws Throwable {
    mockConnection();
    Channel channel = mockChannel().proxy;
    channel.close();
    channel.basicRecover();
  }

  /**
   * Asserts that a closed channel that is returned to the pool.
   */
  public void shouldThrowOnAlreadyClosedPooledChannelInvocation() throws Throwable {
    options = LyraOptions.forHost("localhost").withChannelPoolSize(5);
    mockConnection();
    Channel channel1 = mockChannel().proxy;
    channel1.close();
    assertEquals(connectionHandler.pooledChannels.size(), 1);
    Channel channel2 = mockChannel().proxy;
    assertEquals(connectionHandler.pooledChannels.size(), 0);
    channel2.basicRecover();
    try {
      channel1.basicRecover();
      fail();
    } catch (AlreadyClosedException expected) {
    }
  }
  
  public void shouldHandleCancelWithNullArgs() throws Throwable {
    mockConnection();
    mockChannel().proxy.basicCancel(null);
  }
  
  public void shouldHandleAddListenerWithNullArgs() throws Throwable {
    mockConnection();
    mockChannel().proxy.addConfirmListener(null);
  }
}
