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
import java.util.concurrent.TimeoutException;
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
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;

public abstract class AbstractFunctionalTest {
  protected Config config;
  protected ConnectionOptions options;
  protected ConnectionFactory connectionFactory;
  protected Connection connection;
  protected ConnectionHandler connectionHandler;
  protected Connection connectionProxy;
  private Map<Integer, MockChannel> channels;
  private Channel recoveryChannel;

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

    protected Consumer mockConsumer(String queueName, int consumerNumber) throws IOException {
      Consumer consumer = consumers.get(consumerNumber);
      if (consumer == null) {
        String consumerTag = String.format("%s-%s", delegate.getChannelNumber(), consumerNumber);
        consumer = new MockConsumer(proxy, consumerNumber);
        when(delegate.basicConsume(eq(queueName), argThat(matcherFor(consumer)))).thenReturn(
            consumerTag);
        proxy.basicConsume(queueName, consumer);
        consumers.put(consumerNumber, consumer);
      }

      return consumer;
    }
  }

  @BeforeMethod
  protected void beforeMethod() throws Exception {
    options = null;
    config = null;
    connectionFactory = null;
    connectionHandler = null;
    connectionProxy = null;
    channels = null;
    recoveryChannel = null;
  }

  protected Consumer mockConsumer(int channelNumber, int consumerNumber) throws IOException {
    return mockChannel(channelNumber).mockConsumer("test-queue", consumerNumber);
  }

  protected Consumer mockConsumer(String queueName, int channelNumber, int consumerNumber)
      throws IOException {
    return mockChannel(channelNumber).mockConsumer(queueName, consumerNumber);
  }

  protected void mockConnection() throws IOException, TimeoutException {
    if (connectionFactory == null) {
      mockConnectionOnly();
      connectionFactory = mock(ConnectionFactory.class);
      when(connectionFactory.getVirtualHost()).thenReturn("/");
      when(connectionFactory.newConnection(any(ExecutorService.class), any(Address[].class)))
          .thenReturn(connection);
    }

    if (options == null)
      options = new ConnectionOptions().withHost("test-host");
    options.withConnectionFactory(connectionFactory);
    if (config == null)
      config =
          new Config().withRetryPolicy(
              RetryPolicies.retryAlways().withInterval(Duration.millis(10))).withRecoveryPolicy(
              RecoveryPolicies.recoverAlways());

    if (connectionHandler == null) {
      connectionHandler = new ConnectionHandler(options, config, Connection.class.getClassLoader());
      connectionProxy =
          (ConfigurableConnection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
              new Class<?>[] {ConfigurableConnection.class}, connectionHandler);
      connectionHandler.createConnection(connectionProxy);
      channels = new HashMap<Integer, MockChannel>();
    }
  }

  protected void mockConnectionOnly() throws IOException {
    connection = mock(Connection.class);
    InetAddress inetAddress = mock(InetAddress.class);
    when(connection.getAddress()).thenReturn(inetAddress);
    when(inetAddress.getHostAddress()).thenReturn("test-host");
    when(connection.getPort()).thenReturn(5672);
  }

  protected Channel mockRecoveryChannel() throws IOException {
    if (recoveryChannel == null) {
      recoveryChannel = mock(Channel.class);
      when(recoveryChannel.getChannelNumber()).thenReturn(ConnectionHandler.RECOVERY_CHANNEL_NUM);
      when(recoveryChannel.toString()).thenReturn("channel-admin-recovery");
      when(connection.createChannel(eq(ConnectionHandler.RECOVERY_CHANNEL_NUM))).thenAnswer(
          new Answer<Channel>() {
            public Channel answer(InvocationOnMock invocation) throws Throwable {
              when(recoveryChannel.isOpen()).thenReturn(true);
              return recoveryChannel;
            }
          });
    }

    return recoveryChannel;
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

  protected MockChannel mockChannel(int channelNumber) {
    MockChannel mockChannel = channels.get(channelNumber);

    try {
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
    } catch (Exception notPossible) {
    }

    return mockChannel;
  }

  protected void runInThread(final Runnable runnable) {
    new Thread(runnable).start();
  }

  protected ShutdownSignalException channelShutdownSignal() {
    return retryableChannelShutdownSignal();
  }

  protected ShutdownSignalException nonRetryableChannelShutdownSignal() {
    Method m = new AMQP.Channel.Close.Builder().replyCode(404).build();
    return new ShutdownSignalException(false, false, m, null);
  }

  protected ShutdownSignalException retryableChannelShutdownSignal() {
    Method m = new AMQP.Channel.Close.Builder().replyCode(311).build();
    return new ShutdownSignalException(false, false, m, null);
  }

  protected ShutdownSignalException connectionShutdownSignal() {
    return retryableConnectionShutdownSignal();
  }

  protected ShutdownSignalException retryableConnectionShutdownSignal() {
    Method m = new AMQP.Connection.Close.Builder().replyCode(320).build();
    return new ShutdownSignalException(true, false, m, null);
  }

  protected ShutdownSignalException nonRetryableConnectionShutdownSignal() {
    Method m = new AMQP.Connection.Close.Builder().replyCode(530).build();
    return new ShutdownSignalException(true, false, m, null);
  }

  protected void callShutdownListener(RetryableResource resource, ShutdownSignalException e) {
    Method method = e.getReason();
    if (method instanceof AMQP.Connection.Close) {
      if (recoveryChannel != null)
        when(recoveryChannel.isOpen()).thenReturn(false);
      connectionHandler.shutdownListeners.get(0).shutdownCompleted(e);
    } else if (method instanceof AMQP.Channel.Close)
      resource.shutdownListeners.get(0).shutdownCompleted(e);
  }

  void verifyCxnCreations(int expectedCreations) throws IOException, TimeoutException {
    verify(connectionFactory, times(expectedCreations)).newConnection(any(ExecutorService.class),
        any(Address[].class));
  }

  void verifyRecoveryChannelCreations(int expectedCreations) throws IOException {
    verify(connection, times(expectedCreations)).createChannel(
        eq(ConnectionHandler.RECOVERY_CHANNEL_NUM));
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
        return arg instanceof MockConsumer ? ((MockConsumer) arg).equals(consumer)
            : ((ConsumerDelegate) arg).delegate.equals(consumer);
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
