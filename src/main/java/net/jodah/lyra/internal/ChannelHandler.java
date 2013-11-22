package net.jodah.lyra.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jodah.lyra.config.ChannelConfig;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConsumerListener;
import net.jodah.lyra.internal.util.Collections;
import net.jodah.lyra.internal.util.Exceptions;
import net.jodah.lyra.internal.util.Reflection;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.FlowListener;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Handles channel method invocations.
 * 
 * @author Jonathan Halterman
 */
public class ChannelHandler extends RetryableResource implements InvocationHandler {
  private final ConnectionHandler connectionHandler;
  private final Config config;
  volatile long previousMaxDeliveryTag;
  volatile long maxDeliveryTag;
  Channel proxy;
  Channel delegate;

  // Recovery state
  private AtomicBoolean recoveryPending = new AtomicBoolean();
  private RecurringStats recoveryStats;
  private Map<String, Invocation> recoveryConsumers;
  private ShutdownSignalException lastShutdownSignal;

  // Delegate state
  final Map<String, Invocation> consumerInvocations = Collections.synchronizedMap();
  private List<ConfirmListener> confirmListeners = new CopyOnWriteArrayList<ConfirmListener>();
  private List<FlowListener> flowListeners = new CopyOnWriteArrayList<FlowListener>();
  private List<ReturnListener> returnListeners = new CopyOnWriteArrayList<ReturnListener>();
  private boolean flowDisabled;
  private Invocation basicQos;
  private boolean confirmSelect;
  private boolean txSelect;

  public ChannelHandler(ConnectionHandler connectionHandler, Channel delegate, Config config) {
    this.connectionHandler = connectionHandler;
    this.delegate = delegate;
    this.config = config;

    ShutdownListener listener = new ChannelShutdownListener();
    shutdownListeners.add(listener);
    delegate.addShutdownListener(listener);
  }

  /**
   * Handles channel closures.
   */
  private class ChannelShutdownListener implements ShutdownListener {
    @Override
    public void shutdownCompleted(ShutdownSignalException e) {
      resourceClosed();
      if (!e.isInitiatedByApplication()) {
        log.error("Channel {} was closed unexpectedly", ChannelHandler.this);
        lastShutdownSignal = e;
        if (!Exceptions.isConnectionClosure(e) && canRecover())
          ConnectionHandler.RECOVERY_EXECUTORS.execute(new Runnable() {
            @Override
            public void run() {
              try {
                recoveryPending.set(true);
                recoverChannel(true);
              } catch (Throwable ignore) {
              }
            }
          });
      }
    }
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    if (closed && method.getDeclaringClass().isAssignableFrom(Channel.class))
      throw new AlreadyClosedException("Attempt to use closed channel", proxy);

    Callable<Object> callable = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        if (method.getDeclaringClass().isAssignableFrom(ChannelConfig.class))
          return Reflection.invoke(config, method, args);

        String methodName = method.getName();

        if ("basicAck".equals(methodName) || "basicNack".equals(methodName)
            || "basicReject".equals(methodName)) {
          long deliveryTag = (Long) args[0] - previousMaxDeliveryTag;
          if (deliveryTag > 0) {
            args[0] = deliveryTag;
          } else {
            return null;
          }
        } else if ("basicConsume".equals(methodName)) {
          Consumer consumer = (Consumer) args[args.length - 1];
          args[args.length - 1] = new ConsumerDelegate(ChannelHandler.this, consumer);
          String consumerTag = (String) Reflection.invoke(delegate, method, args);
          if (args.length > 3)
            args[2] = consumerTag;
          consumerInvocations.put(consumerTag, new Invocation(method, args));
          log.info("Created consumer-{} of {} via {}", consumerTag, args[0], ChannelHandler.this);
          return consumerTag;
        } else if ("basicCancel".equals(methodName) && args[0] != null)
          consumerInvocations.remove((String) args[0]);

        Object result = Reflection.invoke(delegate, method, args);

        if ("flow".equals(methodName))
          flowDisabled = !(Boolean) args[0];
        else if ("basicQos".equals(methodName)) {
          // Store non-global Qos
          if (args.length < 3 || !(Boolean) args[2])
            basicQos = new Invocation(method, args);
        } else if ("confirmSelect".equals(methodName))
          confirmSelect = true;
        else if ("txSelect".equals(methodName))
          txSelect = true;
        else if ("addConfirmListener".equals(methodName))
          confirmListeners.add((ConfirmListener) args[0]);
        else if ("addFlowListener".equals(methodName))
          flowListeners.add((FlowListener) args[0]);
        else if ("addReturnListener".equals(methodName))
          returnListeners.add((ReturnListener) args[0]);
        else if ("removeConfirmListener".equals(methodName))
          confirmListeners.remove((ConfirmListener) args[0]);
        else if ("removeFlowListener".equals(methodName))
          flowListeners.remove((FlowListener) args[0]);
        else if ("removeReturnListener".equals(methodName))
          returnListeners.remove((ReturnListener) args[0]);
        else if ("clearConfirmListeners".equals(methodName))
          confirmListeners.clear();
        else if ("clearFlowListeners".equals(methodName))
          flowListeners.clear();
        else if ("clearReturnListeners".equals(methodName))
          returnListeners.clear();

        return result;
      }

      @Override
      public String toString() {
        return Reflection.toString(method);
      }
    };

    return handleCommonMethods(delegate, method, args) ? null : callWithRetries(callable,
        config.getChannelRetryPolicy(), null, canRecover(), true);
  }

  @Override
  public String toString() {
    return String.format("channel-%s on %s", delegate.getChannelNumber(), connectionHandler);
  }

  @Override
  void afterClosure() {
    connectionHandler.removeChannel(delegate.getChannelNumber());
  }

  boolean canRecover() {
    return connectionHandler.canRecover() && config.getChannelRecoveryPolicy() != null
        && config.getChannelRecoveryPolicy().allowsAttempts();
  }

  /**
   * Atomically recovers the channel.
   * 
   * @throws Exception when recovery fails due to a connection closure
   */
  synchronized void recoverChannel(boolean returnOnFailedRecovery) throws Exception {
    recoveryPending.set(false);
    if (circuit.isClosed())
      return;

    if (recoveryStats == null) {
      recoveryConsumers = consumerInvocations.isEmpty() ? null : new HashMap<String, Invocation>(
          consumerInvocations);
      recoveryStats = new RecurringStats(config.getChannelRecoveryPolicy());
      recoveryStats.incrementTime();
    } else if (recoveryStats.isPolicyExceeded()) {
      recoveryFailed(lastShutdownSignal);
      if (returnOnFailedRecovery)
        return;
    }

    try {
      delegate = callWithRetries(new Callable<Channel>() {
        @Override
        public Channel call() throws Exception {
          log.info("Recovering {}", ChannelHandler.this);
          previousMaxDeliveryTag = maxDeliveryTag;
          Channel channel = connectionHandler.createChannel(delegate.getChannelNumber());
          migrateConfiguration(channel);
          return channel;
        }
      }, config.getChannelRecoveryPolicy(), recoveryStats, true, false);
      for (ChannelListener listener : config.getChannelListeners())
        try {
          if (!recoveryPending.get())
            listener.onRecovery(proxy);
        } catch (Exception ignore) {
        }
      if (config.isConsumerRecoveryEnabled() && !recoveryPending.get())
        recoverConsumers(recoveryConsumers);
      recoverySucceeded();
    } catch (Exception e) {
      ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
      if (sse != null) {
        if (Exceptions.isConnectionClosure(sse))
          throw e;
      } else if (recoveryStats.isPolicyExceeded())
        recoveryFailed(e);
    }
  }

  @Override
  void resourceClosed() {
    circuit.open();
    synchronized (consumerInvocations) {
      for (Invocation invocation : consumerInvocations.values())
        ((ConsumerDelegate) invocation.args[invocation.args.length - 1]).close();
    }
  }

  /**
   * Migrates the channel's configuration to the given {@code channel}.
   */
  private void migrateConfiguration(Channel channel) throws Exception {
    channel.setDefaultConsumer(delegate.getDefaultConsumer());
    if (flowDisabled)
      channel.flow(false);
    if (basicQos != null)
      Reflection.invoke(channel, basicQos.method, basicQos.args);
    if (confirmSelect)
      channel.confirmSelect();
    if (txSelect)
      channel.txSelect();
    synchronized (shutdownListeners) {
      for (ShutdownListener listener : shutdownListeners)
        channel.addShutdownListener(listener);
    }
    for (ConfirmListener listener : confirmListeners)
      channel.addConfirmListener(listener);
    for (FlowListener listener : flowListeners)
      channel.addFlowListener(listener);
    for (ReturnListener listener : returnListeners)
      channel.addReturnListener(listener);
  }

  /**
   * Recovers the channel's consumers to given {@code channel}. If a consumer recovery fails due to
   * a channel closure, then we will not attempt to recover that consumer again.
   * 
   * @throws Exception when recovery fails due to a resource closure
   */
  private void recoverConsumers(Map<String, Invocation> consumers) throws Exception {
    if (consumers != null)
      for (Iterator<Map.Entry<String, Invocation>> it = consumers.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, Invocation> entry = it.next();
        Object[] args = entry.getValue().args;
        ConsumerDelegate consumer = (ConsumerDelegate) args[args.length - 1];

        try {
          for (ConsumerListener listener : config.getConsumerListeners())
            try {
              listener.onBeforeRecovery(consumer, proxy);
            } catch (Exception ignore) {
            }
          log.info("Recovering consumer-{} via {}", entry.getKey(), this);
          consumer.open();
          Reflection.invoke(delegate, entry.getValue().method, entry.getValue().args);
          for (ConsumerListener listener : config.getConsumerListeners())
            try {
              listener.onAfterRecovery(consumer, proxy);
            } catch (Exception ignore) {
            }
        } catch (Exception e) {
          ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
          log.error("Failed to recover consumer-{} via {}", entry.getKey(), this, e);
          for (ConsumerListener listener : config.getConsumerListeners())
            try {
              listener.onRecoveryFailure(consumer, proxy, e);
            } catch (Exception ignore) {
            }
          if (sse != null) {
            if (!Exceptions.isConnectionClosure(sse))
              it.remove();
            throw e;
          }
        }
      }

    for (ChannelListener listener : config.getChannelListeners())
      try {
        listener.onConsumerRecovery(proxy);
      } catch (Exception ignore) {
      }
  }

  private void recoveryComplete() {
    recoveryStats = null;
    recoveryConsumers = null;
    lastShutdownSignal = null;
  }

  private void recoveryFailed(Exception e) {
    log.error("Failed to recover {}", this, e);
    recoveryComplete();
    interruptWaiters();
    for (ChannelListener listener : config.getChannelListeners())
      try {
        listener.onRecoveryFailure(proxy, e);
      } catch (Exception ignore) {
      }
  }

  private void recoverySucceeded() {
    if (!recoveryPending.get()) {
      recoveryComplete();
      circuit.close();
    }
  }
}
