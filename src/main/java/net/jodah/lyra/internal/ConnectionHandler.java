package net.jodah.lyra.internal;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.ConfigurableChannel;
import net.jodah.lyra.config.ConnectionConfig;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.internal.util.*;
import net.jodah.lyra.internal.util.concurrent.NamedThreadFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Handles connection method invocations and performs connection recovery.
 * 
 * @author Jonathan Halterman
 */
public class ConnectionHandler extends RetryableResource implements InvocationHandler {
  private static final Class<?>[] CHANNEL_TYPES = {ConfigurableChannel.class};
  private static final AtomicInteger CONNECTION_COUNTER = new AtomicInteger();
  static final ExecutorService RECOVERY_EXECUTORS =
      Executors.newCachedThreadPool(new NamedThreadFactory("lyra-recovery-%s"));
  static final int RECOVERY_CHANNEL_NUM = 100;

  final Map<String, ResourceDeclaration> exchangeDeclarations = Collections.synchronizedLinkedMap();
  final ArrayListMultiMap<String, Binding> exchangeBindings = Collections.arrayListMultiMap();
  final Map<String, QueueDeclaration> queueDeclarations = Collections.synchronizedLinkedMap();
  final ArrayListMultiMap<String, Binding> queueBindings = Collections.arrayListMultiMap();
  private final ConnectionOptions options;
  private final Config config;
  private final String connectionName;
  private final ExecutorService consumerThreadPool;
  private final ClassLoader classLoader;
  private final Map<String, ChannelHandler> channels =
      new ConcurrentHashMap<String, ChannelHandler>();
  private Connection proxy;
  private Connection delegate;
  private Channel recoveryChannel;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        RECOVERY_EXECUTORS.shutdownNow();
      }
    });
  }

  public ConnectionHandler(ConnectionOptions options, Config config, ClassLoader classLoader) throws IOException {
    this.options = options;
    this.config = config;
    this.classLoader = Assert.notNull(classLoader, "classLoader");
    this.connectionName =
        options.getName() == null ? String.format("cxn-%s", CONNECTION_COUNTER.incrementAndGet())
            : options.getName();
    consumerThreadPool =
        options.getConsumerExecutor() == null ? Executors.newCachedThreadPool(new NamedThreadFactory(
            String.format("rabbitmq-%s-consumer", connectionName))) : options.getConsumerExecutor();
  }

  /**
   * Handles connection shutdowns.
   */
  private class ConnectionShutdownListener implements ShutdownListener {
    @Override
    public void shutdownCompleted(ShutdownSignalException e) {
      connectionShutdown();
      if (!e.isInitiatedByApplication()) {
        log.error("Connection {} was closed unexpectedly", ConnectionHandler.this);
        if (canRecover())
          RECOVERY_EXECUTORS.execute(new Runnable() {
            @Override
            public void run() {
              try {
                recoverConnection();
              } catch (Exception e) {
                // Only fail on non-closures since closures will trigger a new recovery
                if (!Exceptions.isCausedByConnectionClosure(e)) {
                  log.error("Failed to recover connection {}", ConnectionHandler.this, e);
                  connectionClosed();
                  interruptWaiters();
                  for (ConnectionListener listener : config.getConnectionListeners())
                    try {
                      listener.onRecoveryFailure(proxy, e);
                    } catch (Exception ignore) {
                    }
                }
              }
            }
          });
      } else
        connectionClosed();
    }
  }

  public void createConnection(Connection proxy) throws IOException, TimeoutException {
    try {
      this.proxy = proxy;
      createConnection(config.getConnectRetryPolicy(), config.getRetryableExceptions(), false);
      ShutdownListener shutdownListener = new ConnectionShutdownListener();
      shutdownListeners.add(shutdownListener);
      delegate.addShutdownListener(shutdownListener);
      for (ConnectionListener listener : config.getConnectionListeners())
        try {
          listener.onCreate(proxy);
        } catch (Exception ignore) {
        }
    } catch (IOException e) {
      log.error("Failed to create connection {}", connectionName, e);
      connectionClosed();
      for (ConnectionListener listener : config.getConnectionListeners())
        try {
          listener.onCreateFailure(e);
        } catch (Exception ignore) {
        }
      throw e;
    }
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    if (handleCommonMethods(delegate, method, args))
      return null;

    try {
      return callWithRetries(
          new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              if ("createChannel".equals(method.getName())) {
                Channel channel = (Channel) Reflection.invoke(delegate, method, args);
                ChannelHandler channelHandler =
                    new ChannelHandler(ConnectionHandler.this, channel, new Config(config));
                Channel channelProxy =
                    (Channel) Proxy.newProxyInstance(classLoader,
                        CHANNEL_TYPES, channelHandler);
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
          }, config.getConnectionRetryPolicy(), null, config.getRetryableExceptions(),
          canRecover(),
          true);
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

  private void connectionClosed() {
    if (options.getConsumerExecutor() == null)
      consumerThreadPool.shutdown();
  }

  private void connectionShutdown() {
    circuit.open();
    for (ChannelHandler channelHandler : channels.values())
      channelHandler.channelShutdown();
  }

  /**
   * @throws IOException when recovery fails and recovery policy is exceeded
   */
  private void createConnection(RecurringPolicy<?> recurringPolicy,
      Set<Class<? extends Exception>> recurringExceptions, final boolean recovery)
      throws IOException, TimeoutException {
    try {
      RecurringStats recurringStats = null;
      if (recovery) {
        recurringStats = new RecurringStats(recurringPolicy);
        recurringStats.incrementTime();
      }

      delegate = callWithRetries(new Callable<Connection>() {
        @Override
        public Connection call() throws IOException, TimeoutException {
          log.info("{} connection {} to {}", recovery ? "Recovering" : "Creating", connectionName,
              options.getAddresses());
          ConnectionFactory cxnFactory = options.getConnectionFactory();
          Connection connection =
              cxnFactory.newConnection(consumerThreadPool, options.getAddresses());
          final String amqpAddress =
              String.format("%s://%s:%s/%s", cxnFactory.isSSL() ? "amqps" : "amqp",
                  connection.getAddress().getHostAddress(), connection.getPort(),
                  "/".equals(cxnFactory.getVirtualHost()) ? "" : cxnFactory.getVirtualHost());
          log.info("{} connection {} to {}", recovery ? "Recovered" : "Created", connectionName,
              amqpAddress);
          return connection;
        }
      }, recurringPolicy, recurringStats, recurringExceptions, true, false);
    } catch (Throwable t) {
      if (t instanceof IOException)
        throw (IOException) t;
      if (t instanceof TimeoutException)
        throw (TimeoutException)t;
      if (t instanceof RuntimeException)
        throw (RuntimeException) t;
    }
  }

  /**
   * @throws Exception when recovery fails or connection is closed
   */
  private void recoverConnection() throws Exception {
    for (ConnectionListener listener : config.getConnectionListeners())
      try {
        listener.onRecoveryStarted(proxy);
      } catch (Exception ignore) {
      }
    
    createConnection(config.getConnectionRecoveryPolicy(), config.getRecoverableExceptions(), true);

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

    recoverExchangesAndQueues();

    // Recover channels
    for (ChannelHandler channelHandler : channels.values())
      if (channelHandler.canRecover())
        channelHandler.recoverChannel(true);

    for (ConnectionListener listener : config.getConnectionListeners())
      try {
        listener.onRecoveryCompleted(proxy);
      } catch (Exception ignore) {
      }

    circuit.close();
  }

  /**
   * Recover exchanges, queues and bindings via a one-off channel.
   * 
   * @throws Exception when recovery fails due to a connection closure
   */
  private void recoverExchangesAndQueues() throws Exception {
    boolean canRecoverExchanges =
        config.isExchangeRecoveryEnabled()
            && (!exchangeDeclarations.isEmpty() || !exchangeBindings.isEmpty());
    boolean canRecoverQueues =
        config.isQueueRecoveryEnabled()
            && (!queueDeclarations.isEmpty() || !queueBindings.isEmpty());

    if (canRecoverExchanges || canRecoverQueues) {
      try {
        if (canRecoverExchanges)
          recoverExchanges();
        if (canRecoverQueues)
          recoverQueues();
      } finally {
        try {
          if (recoveryChannel != null && recoveryChannel.isOpen())
            recoveryChannel.close();
        } catch (IOException ignore) {
        }
      }
    }
  }

  /**
   * @throws Exception when recovery fails due to a connection closure
   */
  private void recoverExchanges() throws Exception {
    for (Map.Entry<String, ResourceDeclaration> entry : exchangeDeclarations.entrySet())
      recoverExchange(entry.getKey(), entry.getValue());
    recoverExchangeBindings(exchangeBindings.values());
  }

  /**
   * @throws Exception when recovery fails due to a connection closure
   */
  private void recoverQueues() throws Exception {
    Map<String, QueueDeclaration> newDeclarations = new HashMap<String, QueueDeclaration>();
    for (Iterator<Map.Entry<String, QueueDeclaration>> it = queueDeclarations.entrySet().iterator(); it.hasNext();) {
      Map.Entry<String, QueueDeclaration> entry = it.next();
      String queueName = entry.getKey();
      QueueDeclaration queueDeclaration = entry.getValue();
      String newQueueName = recoverQueue(queueName, queueDeclaration);

      // Update dependencies for new queue names
      if (!entry.getKey().equals(newQueueName)) {
        it.remove();
        newDeclarations.put(newQueueName, queueDeclaration);
        updateQueueBindingReferences(queueName, newQueueName);
      }
    }

    queueDeclarations.putAll(newDeclarations);
    recoverQueueBindings(queueBindings.values());
  }
  
  @Override
  void interruptWaiters() {
    super.interruptWaiters();
    for (ChannelHandler channel : channels.values())
      channel.interruptWaiters();
  }

  /** Updates the queue name referenced by queue bindings. */
  void updateQueueBindingReferences(String oldQueueName, String newQueueName) {
    List<Binding> bindings = queueBindings.remove(oldQueueName);
    if (bindings != null) {
      for (Binding binding : bindings)
        binding.destination = newQueueName;
      queueBindings.putAll(newQueueName, bindings);
    }
  }

  /** Gets a recovery channel, creating one if necessary. */
  @Override
  Channel getRecoveryChannel() throws IOException {
    if (recoveryChannel == null || !recoveryChannel.isOpen())
      recoveryChannel = delegate.createChannel(RECOVERY_CHANNEL_NUM);
    return recoveryChannel;
  }

  @Override
  boolean throwOnRecoveryFailure() {
    return false;
  }
}
