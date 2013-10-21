package net.jodah.lyra.internal;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.ConfigurableChannel;
import net.jodah.lyra.config.ConnectionConfig;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.internal.util.Reflection;
import net.jodah.lyra.internal.util.concurrent.NamedThreadFactory;
import net.jodah.lyra.retry.RetryPolicy;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Handles method invocations for a connection.
 * 
 * @author Jonathan Halterman
 */
public class ConnectionHandler extends RetryableResource implements InvocationHandler {
  private static final Class<?>[] CHANNEL_TYPES = { ConfigurableChannel.class };
  private static final AtomicInteger CONNECTION_COUNTER = new AtomicInteger();
  static final ExecutorService RECOVERY_EXECUTORS = Executors.newCachedThreadPool(new NamedThreadFactory(
      "lyra-recovery-%s"));

  private final ConnectionOptions options;
  private final Config config;
  private final String connectionName;
  private final Map<String, ChannelHandler> channels = new ConcurrentHashMap<String, ChannelHandler>();
  private Connection proxy;
  private Connection delegate;

  public ConnectionHandler(ConnectionOptions options, Config config) throws IOException {
    this.options = options;
    this.config = config;
    this.connectionName = options.getName() == null ? String.format("cxn-%s",
        CONNECTION_COUNTER.incrementAndGet()) : options.getName();

    createConnection();
    ShutdownListener listener = new ConnectionShutdownListener();
    shutdownListeners.add(listener);
    delegate.addShutdownListener(listener);
  }

  /**
   * Handles connection closures.
   */
  private class ConnectionShutdownListener implements ShutdownListener {
    @Override
    public void shutdownCompleted(ShutdownSignalException e) {
      circuit.open();
      for (ChannelHandler channelHandler : channels.values())
        channelHandler.circuit.open();

      if (!e.isInitiatedByApplication()) {
        log.info("Connection {} was closed unexpectedly", ConnectionHandler.this);
        if (canRecover(e.isHardError()))
          RECOVERY_EXECUTORS.execute(new Runnable() {
            @Override
            public void run() {
              try {
                recoverConnection();
                for (ConnectionListener listener : config.getConnectionListeners())
                  listener.onRecovery(proxy);
              } catch (Throwable t) {
                log.error("Failed to recover connection {}", connectionName, t);
                for (ConnectionListener listener : config.getConnectionListeners())
                  listener.onRecoveryFailure(proxy, t);
              }
            }
          });
      }
    }
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    handleCommonMethods(delegate, method, args);

    try {
      return callWithRetries(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          if ("createChannel".equals(method.getName())) {
            Channel channel = (Channel) Reflection.invoke(delegate, method, args);
            ChannelHandler channelHandler = new ChannelHandler(ConnectionHandler.this, channel,
                new Config(config));
            Channel channelProxy = (Channel) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), CHANNEL_TYPES, channelHandler);
            channelHandler.proxy = channelProxy;
            channels.put(Integer.valueOf(channel.getChannelNumber()).toString(), channelHandler);
            log.info("Created {}", channelHandler);
            for (ChannelListener listener : config.getChannelListeners())
              listener.onCreate(channelProxy);
            return channelProxy;
          }

          return Reflection.invoke(
              ConnectionConfig.class.equals(method.getDeclaringClass()) ? config : delegate,
              method, args);
        }
      }, config.getConnectionRetryPolicy(), false);
    } catch (Throwable t) {
      if ("createChannel".equals(method.getName())) {
        log.error("Failed to create channel on {}", connectionName, t);
        for (ChannelListener listener : config.getChannelListeners())
          listener.onCreateFailure(t);
      }

      throw t;
    }
  }

  public void setProxy(Connection proxy) {
    this.proxy = proxy;
  }

  @Override
  public String toString() {
    return connectionName;
  }

  @Override
  boolean canRecover(boolean connectionClosed) {
    return config.getConnectionRecoveryPolicy() != null
        && config.getConnectionRecoveryPolicy().allowsRetries();
  }

  Channel createChannel(int channelNumber) throws IOException {
    return delegate.createChannel(channelNumber);
  }

  void removeChannel(int channelNumber) {
    channels.remove(Integer.valueOf(channelNumber).toString());
  }

  private void createConnection() throws IOException {
    try {
      createConnection(config.getConnectRetryPolicy(), false);
      for (ConnectionListener listener : config.getConnectionListeners())
        listener.onCreate(proxy);
    } catch (IOException e) {
      log.error("Failed to create connection {}", connectionName, e);
      for (ConnectionListener listener : config.getConnectionListeners())
        listener.onCreateFailure(e);
      throw e;
    }
  }

  private void createConnection(RetryPolicy retryPolicy, final boolean recovery) throws IOException {
    try {
      delegate = callWithRetries(new Callable<Connection>() {
        @Override
        public Connection call() throws IOException {
          log.info("{} connection {} to {}", recovery ? "Recovering" : "Creating", connectionName,
              options.getAddresses());
          ExecutorService consumerPool = options.getConsumerThreadPool() == null ? Executors.newCachedThreadPool(new NamedThreadFactory(
              String.format("rabbitmq-%s-consumer", connectionName)))
              : options.getConsumerThreadPool();
          Connection connection = options.getConnectionFactory().newConnection(consumerPool,
              options.getAddresses());
          log.info("{} connection {} to {}{}", recovery ? "Recovered" : "Created", connectionName,
              connection.getAddress().getHostAddress(), options.getConnectionFactory()
                  .getVirtualHost());
          return connection;
        }
      }, retryPolicy, recovery);
    } catch (Throwable t) {
      if (t instanceof IOException)
        throw (IOException) t;
      if (t instanceof RuntimeException)
        throw (RuntimeException) t;
    }
  }

  private void recoverConnection() throws Throwable {
    createConnection(config.getConnectionRecoveryPolicy(), true);

    // Recover channels
    if (config.getChannelRecoveryPolicy() != null
        && config.getChannelRecoveryPolicy().allowsRetries())
      for (ChannelHandler channelHandler : channels.values())
        channelHandler.recoverChannel();

    // Migrate connection state
    synchronized (shutdownListeners) {
      for (ShutdownListener listener : shutdownListeners)
        delegate.addShutdownListener(listener);
    }

    circuit.close();
  }
}
