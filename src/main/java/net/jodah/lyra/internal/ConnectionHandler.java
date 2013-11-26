package net.jodah.lyra.internal;

import com.rabbitmq.client.*;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.ConfigurableChannel;
import net.jodah.lyra.config.ConnectionConfig;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.internal.util.Reflection;
import net.jodah.lyra.internal.util.concurrent.NamedThreadFactory;

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

/**
 * Handles connection method invocations.
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

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        RECOVERY_EXECUTORS.shutdownNow();
      }
    });
  }

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
      resourceClosed();
      if (!e.isInitiatedByApplication()) {
        log.error("Connection {} was closed unexpectedly", ConnectionHandler.this);
        if (canRecover())
          RECOVERY_EXECUTORS.execute(new Runnable() {
            @Override
            public void run() {
              try {
                recoverConnection();

              } catch (Throwable t) {
                log.error("Failed to recover connection {}", ConnectionHandler.this, t);
                interruptWaiters();
                for (ConnectionListener listener : config.getConnectionListeners())
                  try {
                    listener.onRecoveryFailure(proxy, t);
                  } catch (Exception ignore) {
                  }
              }
            }
          });
      }
    }
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    if (handleCommonMethods(delegate, method, args))
      return null;

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
              try {
                listener.onCreate(channelProxy);
              } catch (Exception ignore) {
              }
            return channelProxy;
          }

          return Reflection.invoke(
              method.getDeclaringClass().isAssignableFrom(ConnectionConfig.class) ? config
                  : delegate, method, args);
        }

        @Override
        public String toString() {
          return Reflection.toString(method);
        }
      }, config.getConnectionRetryPolicy(), null, canRecover(), true);
    } catch (Throwable t) {
      if ("createChannel".equals(method.getName())) {
        log.error("Failed to create channel on {}", connectionName, t);
        for (ChannelListener listener : config.getChannelListeners())
          try {
            listener.onCreateFailure(t);
          } catch (Exception ignore) {
          }
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

  boolean canRecover() {
    return config.getConnectionRecoveryPolicy() != null
        && config.getConnectionRecoveryPolicy().allowsAttempts();
  }

  Channel createChannel(int channelNumber) throws IOException {
    return delegate.createChannel(channelNumber);
  }

  void removeChannel(int channelNumber) {
    channels.remove(Integer.valueOf(channelNumber).toString());
  }

  @Override
  void resourceClosed() {
    circuit.open();
    for (ChannelHandler channelHandler : channels.values())
      channelHandler.resourceClosed();
  }

  private void createConnection() throws IOException {
    try {
      createConnection(config.getConnectRetryPolicy(), false);
      for (ConnectionListener listener : config.getConnectionListeners())
        try {
          listener.onCreate(proxy);
        } catch (Exception ignore) {
        }
    } catch (IOException e) {
      log.error("Failed to create connection {}", connectionName, e);
      for (ConnectionListener listener : config.getConnectionListeners())
        try {
          listener.onCreateFailure(e);
        } catch (Exception ignore) {
        }
      throw e;
    }
  }

  private void createConnection(RecurringPolicy<?> recurringPolicy, final boolean recovery)
      throws IOException {
    try {
      RecurringStats recurringStats = null;
      if (recovery) {
        recurringStats = new RecurringStats(recurringPolicy);
        recurringStats.incrementTime();
      }

      delegate = callWithRetries(new Callable<Connection>() {
        @Override
        public Connection call() throws IOException {
          Address[] addresses = options.getAddresses();
          log.info("{} connection {} to {}", recovery ? "Recovering" : "Creating", connectionName, addresses);
          ExecutorService consumerPool = options.getConsumerExecutor() == null ? Executors.newCachedThreadPool(new NamedThreadFactory(
              String.format("rabbitmq-%s-consumer", connectionName)))
              : options.getConsumerExecutor();
          ConnectionFactory cxnFactory = options.getConnectionFactory();
          Connection connection = cxnFactory.newConnection(consumerPool, addresses);
          final String amqpAddress = String.format("amqp://%s:%s/%s", connection.getAddress()
              .getHostAddress(), connection.getPort(), "/".equals(cxnFactory.getVirtualHost()) ? ""
              : cxnFactory.getVirtualHost());
          log.info("{} connection {} to {}", recovery ? "Recovered" : "Created", connectionName,
              amqpAddress);
          return connection;
        }
      }, recurringPolicy, recurringStats, true, false);
    } catch (Throwable t) {
      if (t instanceof IOException)
        throw (IOException) t;
      if (t instanceof RuntimeException)
        throw (RuntimeException) t;
    }
  }

  private void recoverConnection() throws Throwable {
    createConnection(config.getConnectionRecoveryPolicy(), true);

    // Migrate connection state
    synchronized (shutdownListeners) {
      for (ShutdownListener listener : shutdownListeners)
        delegate.addShutdownListener(listener);
    }

    for (ConnectionListener listener : config.getConnectionListeners())
      try {
        listener.onRecovery(proxy);
      } catch (Exception ignore) {
      }

    // Recover channels
    for (ChannelHandler channelHandler : channels.values())
      if (channelHandler.canRecover())
        channelHandler.recoverChannel(false);

    for (ConnectionListener listener : config.getConnectionListeners())
      try {
        listener.onChannelRecovery(proxy);
      } catch (Exception ignore) {
      }

    circuit.close();
  }
}
