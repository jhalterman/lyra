package net.jodah.lyra.internal;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import net.jodah.lyra.LyraOptions;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.internal.util.Reflection;
import net.jodah.lyra.internal.util.ThrowableCallable;
import net.jodah.lyra.internal.util.concurrent.NamedThreadFactory;
import net.jodah.lyra.retry.RetryPolicy;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Handles method invocations for a connection.
 * 
 * @author Jonathan Halterman
 */
public class ConnectionHandler extends RetryableResource implements InvocationHandler {
  private static final String CREATE_CHANNEL_METHOD_NAME = "createChannel";
  private static final Class<?>[] CHANNEL_TYPES = { Channel.class };
  private static final AtomicInteger CONNECTION_COUNTER = new AtomicInteger();
  private static final ExecutorService RECOVERY_EXECUTORS = Executors.newCachedThreadPool(new NamedThreadFactory(
      "recovery-%s"));

  private final ConnectionFactory connectionFactory;
  private final LyraOptions options;
  private final String connectionName;
  private final Map<String, ChannelHandler> channels = new ConcurrentHashMap<String, ChannelHandler>();
  final Deque<Channel> pooledChannels;
  private Connection proxy;
  private Connection delegate;

  public ConnectionHandler(ConnectionFactory connectionFactory, LyraOptions options)
      throws IOException {
    this.connectionFactory = connectionFactory;
    this.options = options;
    this.connectionName = options.getName() == null ? String.format("cxn-%s",
        CONNECTION_COUNTER.incrementAndGet()) : options.getName();
    pooledChannels = options.getChannelPoolSize() > 0 ? new LinkedBlockingDeque<Channel>() : null;

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
      if (pooledChannels != null)
        pooledChannels.clear();
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
                log.info("Recovered connection {} to {}", connectionName, delegate.getAddress());
                for (ConnectionListener listener : options.getConnectionListeners())
                  listener.onRecovery(proxy);
              } catch (Throwable t) {
                log.error("Failed to recover connection {}", connectionName, t);
                for (ConnectionListener listener : options.getConnectionListeners())
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
      return callWithRetries(new ThrowableCallable<Object>() {
        @Override
        public Object call() throws Throwable {
          if (CREATE_CHANNEL_METHOD_NAME.equals(method.getName())) {
            Channel channel = pooledChannels != null && (args == null || args.length == 0) ? pooledChannels.pollFirst()
                : null;
            if (channel == null)
              channel = (Channel) Reflection.invoke(delegate, method, args);
            ChannelHandler channelHandler = new ChannelHandler(ConnectionHandler.this, channel,
                options, args != null && args.length > 0);
            Channel channelProxy = (Channel) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), CHANNEL_TYPES, channelHandler);
            channelHandler.proxy = channelProxy;
            channels.put(Integer.valueOf(channel.getChannelNumber()).toString(), channelHandler);
            log.info("Created {}", channelHandler);
            for (ChannelListener listener : options.getChannelListeners())
              listener.onCreate(channelProxy);
            return channelProxy;
          }

          return Reflection.invoke(delegate, method, args);
        }
      }, options.getConnectionRetryPolicy(), false);
    } catch (Throwable t) {
      if (CREATE_CHANNEL_METHOD_NAME.equals(method.getName())) {
        log.error("Failed to create channel on {}", connectionName, t);
        for (ChannelListener listener : options.getChannelListeners())
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
    return options.getConnectionRecoveryPolicy() != null
        && options.getConnectionRecoveryPolicy().allowsRetries();
  }

  Channel createChannel(int channelNumber) throws IOException {
    return delegate.createChannel(channelNumber);
  }

  void removeChannel(int channelNumber) {
    channels.remove(Integer.valueOf(channelNumber).toString());
  }

  private void createConnection() throws IOException {
    try {
      createConnection(options.getConnectRetryPolicy(), false);
      log.info("Created connection {} to {}", connectionName, delegate.getAddress());
      for (ConnectionListener listener : options.getConnectionListeners())
        listener.onCreate(proxy);
    } catch (IOException e) {
      log.error("Failed to create connection {}", connectionName, e);
      for (ConnectionListener listener : options.getConnectionListeners())
        listener.onCreateFailure(e);
      throw e;
    }
  }

  private void createConnection(RetryPolicy retryPolicy, final boolean recovery) throws IOException {
    try {
      delegate = callWithRetries(new ThrowableCallable<Connection>() {
        @Override
        public Connection call() throws IOException {
          log.info("{} connection {} to {}", recovery ? "Recovering" : "Creating", connectionName,
              options.getAddresses());
          ExecutorService consumerPool = options.getConsumerThreadPool() == null ? Executors.newCachedThreadPool(new NamedThreadFactory(
              String.format("rabbitmq-%s-consumer", connectionName)))
              : options.getConsumerThreadPool();
          return connectionFactory.newConnection(consumerPool, options.getAddresses());
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
    createConnection(options.getConnectionRecoveryPolicy(), true);

    // Recover channels
    if (options.getChannelRecoveryPolicy() != null
        && options.getChannelRecoveryPolicy().allowsRetries())
      for (ChannelHandler channelHandler : channels.values())
        channelHandler.recoverChannel();

    // Migrate shutdown listeners
    synchronized (shutdownListeners) {
      for (ShutdownListener listener : shutdownListeners)
        delegate.addShutdownListener(listener);
    }

    circuit.close();
  }
}
