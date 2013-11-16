package net.jodah.lyra.internal;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.ConfigurableConnection;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicies;
import net.jodah.lyra.util.Duration;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQCommand;

public abstract class AbstractFunctionalTest {
  protected Config config;
  protected ConnectionOptions options;
  protected ConnectionFactory connectionFactory;
  protected Connection connection;
  protected ConnectionHandler connectionHandler;
  protected Connection connectionProxy;
  protected Map<Integer, MockChannel> channels;

  protected static class MockConsumer extends DefaultConsumer {
    final Channel channel;
    final int consumerNumber;

    MockConsumer(Channel channel, int consumerNumber) {
      super(channel);
      this.channel = channel;
      this.consumerNumber = consumerNumber;
    }

    @Override
    public String toString() {
      return String.format("consumer-%s-%s", channel.getChannelNumber(), consumerNumber);
    }
  }

  protected static class MockChannel {
    Channel proxy;
    Channel delegate;
    ChannelHandler channelHandler;
    Map<Integer, Consumer> consumers = new HashMap<Integer, Consumer>();

    protected Consumer mockConsumer(int consumerNumber) throws IOException {
      Consumer consumer = consumers.get(consumerNumber);
      if (consumer == null) {
        String consumerTag = String.format("%s-%s", delegate.getChannelNumber(), consumerNumber);
        consumer = new MockConsumer(proxy, consumerNumber);
        when(delegate.basicConsume(eq("test-queue"), argThat(matcherFor(consumer)))).thenReturn(
            consumerTag);
        proxy.basicConsume("test-queue", consumer);
        consumers.put(consumerNumber, consumer);
      }

      return consumer;
    }
  }

  @BeforeMethod
  protected void beforeMethod() {
    options = null;
    config = null;
    connectionFactory = null;
    channels = null;
  }

  protected Consumer mockConsumer(int channelNumber, int consumerNumber) throws IOException {
    return mockChannel(channelNumber).mockConsumer(consumerNumber);
  }

  protected void mockConnection() throws IOException {
    if (connectionFactory == null) {
      mockConnectionOnly();
      connectionFactory = mock(ConnectionFactory.class);
      when(connectionFactory.getVirtualHost()).thenReturn("/");
      when(connectionFactory.newConnection(any(ExecutorService.class), any(Address[].class))).thenReturn(
          connection);
    }

    if (options == null)
      options = new ConnectionOptions().withHost("test-host");
    options.withConnectionFactory(connectionFactory);
    if (config == null)
      config = new Config().withRetryPolicy(
          RetryPolicies.retryAlways().withInterval(Duration.millis(10))).withRecoveryPolicy(
          RecoveryPolicies.recoverAlways());

    connectionHandler = new ConnectionHandler(options, config);
    connectionProxy = (ConfigurableConnection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(), new Class<?>[] { ConfigurableConnection.class },
        connectionHandler);
    channels = new HashMap<Integer, MockChannel>();
  }

  protected void mockConnectionOnly() throws IOException {
    connection = mock(Connection.class);
    InetAddress inetAddress = mock(InetAddress.class);
    when(connection.getAddress()).thenReturn(inetAddress);
    when(inetAddress.getHostAddress()).thenReturn("test-host");
    when(connection.getPort()).thenReturn(5672);
  }

  protected MockChannel mockChannel() throws IOException {
    MockChannel mockChannel = new MockChannel();
    Channel channel = mock(Channel.class);
    int channelNumber = new Random().nextInt(1000) + 1000;
    when(channel.getChannelNumber()).thenReturn(channelNumber);
    when(channel.toString()).thenReturn("channel-" + channelNumber);
    when(connection.createChannel()).thenReturn(channel);
    mockChannel.proxy = connectionProxy.createChannel();
    mockChannel.channelHandler = (ChannelHandler) Proxy.getInvocationHandler(mockChannel.proxy);
    mockChannel.delegate = mockChannel.channelHandler.delegate;
    return mockChannel;
  }

  protected MockChannel mockChannel(int channelNumber) throws IOException {
    MockChannel mockChannel = channels.get(channelNumber);
    if (mockChannel == null) {
      mockChannel = new MockChannel();
      Channel channel = mock(Channel.class);
      when(connection.createChannel(eq(channelNumber))).thenReturn(channel);
      when(channel.getChannelNumber()).thenReturn(channelNumber);
      when(channel.toString()).thenReturn("channel-" + channelNumber);
      mockChannel.proxy = connectionProxy.createChannel(channelNumber);
      mockChannel.channelHandler = (ChannelHandler) Proxy.getInvocationHandler(mockChannel.proxy);
      mockChannel.delegate = mockChannel.channelHandler.delegate;
      channels.put(channelNumber, mockChannel);
    }

    return mockChannel;
  }

  protected void runInThread(final Runnable runnable) {
    new Thread(runnable).start();
  }

  protected ShutdownSignalException nonRetryableChannelShutdownSignal() {
    Method m = new AMQP.Channel.Close.Builder().replyCode(404).build();
    Command c = new AMQCommand(m);
    return new ShutdownSignalException(false, false, c, null);
  }

  protected ShutdownSignalException retryableChannelShutdownSignal() {
    Method m = new AMQP.Channel.Close.Builder().replyCode(311).build();
    Command c = new AMQCommand(m);
    return new ShutdownSignalException(false, false, c, null);
  }

  protected ShutdownSignalException retryableConnectionShutdownSignal() {
    Method m = new AMQP.Connection.Close.Builder().replyCode(320).build();
    Command c = new AMQCommand(m);
    return new ShutdownSignalException(true, false, c, null);
  }

  protected ShutdownSignalException nonRetryableConnectionShutdownSignal() {
    Method m = new AMQP.Connection.Close.Builder().replyCode(530).build();
    Command c = new AMQCommand(m);
    return new ShutdownSignalException(true, false, c, null);
  }

  protected void callShutdownListener(RetryableResource resource, ShutdownSignalException e) {
    Object reason = e.getReason();
    if (reason instanceof Command) {
      Command command = (Command) reason;
      Method method = command.getMethod();
      if (method instanceof AMQP.Connection.Close)
        connectionHandler.shutdownListeners.get(0).shutdownCompleted(e);
      else if (method instanceof AMQP.Channel.Close)
        resource.shutdownListeners.get(0).shutdownCompleted(e);
    }
  }

  void verifyCxnCreations(int expectedCreations) throws IOException {
    verify(connectionFactory, times(expectedCreations)).newConnection(any(ExecutorService.class),
        any(Address[].class));
  }

  void verifyChannelCreations(int channelNumber, int expectedCreations) throws IOException {
    verify(connection, times(expectedCreations)).createChannel(eq(channelNumber));
  }

  void verifyConsumerCreations(int channelNumber, int consumerNumber, int expectedInvocations)
      throws IOException {
    verify(mockChannel(channelNumber).delegate, times(expectedInvocations)).basicConsume(
        eq("test-queue"), argThat(matcherFor(mockConsumer(channelNumber, consumerNumber))));
  }

  Channel delegateFor(Channel channelProxy) {
    return ((ChannelHandler) Proxy.getInvocationHandler(channelProxy)).delegate;
  }

  static ArgumentMatcher<Consumer> matcherFor(final Consumer consumer) {
    return new ArgumentMatcher<Consumer>() {
      @Override
      public boolean matches(Object arg) {
        return ((ConsumerDelegate) arg).delegate.equals(consumer);
      }
    };
  }

  /**
   * Returns an answer that fails n times for each thread, throwing t for the first n invocations
   * and returning {@code returnValue} thereafter. Prior to throwing t, the connection handler's
   * shutdown listener is completed if t is a connection shutdown signal.
   */
  protected <T> Answer<T> failNTimes(final int n, final Throwable t, final T returnValue,
      final RetryableResource resource) {
    return new Answer<T>() {
      AtomicInteger failures = new AtomicInteger();

      @Override
      public T answer(InvocationOnMock invocation) throws Throwable {
        if (failures.getAndIncrement() >= n)
          return returnValue;

        if (t instanceof ShutdownSignalException)
          callShutdownListener(resource, (ShutdownSignalException) t);
        if (t instanceof ShutdownSignalException && !(t instanceof AlreadyClosedException))
          throw new IOException(t);
        else
          throw t;
      }
    };
  }
}
