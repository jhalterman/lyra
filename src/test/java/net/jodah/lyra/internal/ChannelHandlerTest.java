package net.jodah.lyra.internal;

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

  public void shouldHandleCancelWithNullArgs() throws Throwable {
    mockConnection();
    mockChannel().proxy.basicCancel(null);
  }

  public void shouldHandleAddListenerWithNullArgs() throws Throwable {
    mockConnection();
    mockChannel().proxy.addConfirmListener(null);
  }
}
