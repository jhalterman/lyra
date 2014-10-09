package net.jodah.lyra.internal;

import org.testng.annotations.Test;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import static org.mockito.Mockito.*;
@Test
public class ChannelHandlerTest extends AbstractFunctionalTest {
  @Test(expectedExceptions = AlreadyClosedException.class)
  public void shouldThrowOnAlreadyClosedChannelInvocation() throws Throwable {
    mockConnection();
    Channel channel = mockChannel().proxy;
    when(channel.getCloseReason()).thenReturn(channelShutdownSignal());
    channel.close();
    channel.abort();
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
