package net.jodah.lyra.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
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

public class ChannelHandler extends RetryableResource implements InvocationHandler {
  private final ConnectionHandler connectionHandler;
  private final Config config;
  private final AtomicBoolean recovering = new AtomicBoolean();
  Channel proxy;
  Channel delegate;

  // Delegate state
  private final Map<String, Invocation> consumerInvocations = Collections.synchronizedMap();
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
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    if (closed && Channel.class.equals(method.getDeclaringClass()))
      throw new AlreadyClosedException("Attempt to use closed channel", proxy);

    return handleCommonMethods(delegate, method, args) ? null : callWithRetries(
        new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            if (ChannelConfig.class.equals(method.getDeclaringClass()))
              return Reflection.invoke(config, method, args);

            String methodName = method.getName();
            if ("basicCancel".equals(methodName) && args[0] != null)
              consumerInvocations.remove((String) args[0]);

            Object result = Reflection.invoke(delegate, method, args);

            if ("basicConsume".equals(methodName)) {
              // Replace actual consumerTag
              if (args.length > 3)
                args[2] = result;
              Consumer consumer = (Consumer) args[args.length - 1];
              args[args.length - 1] = new ConsumerDelegate(consumer);
              consumerInvocations.put((String) result, new Invocation(method, args));
              log.info("Created consumer-{} of {} via {}", result, args[0], ChannelHandler.this);
            } else if ("flow".equals(methodName))
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
        }, config.getChannelRetryPolicy(), false, true);
  }

  @Override
  public String toString() {
    return String.format("channel-%s on %s", delegate.getChannelNumber(), connectionHandler);
  }

  @Override
  void afterClosure() {
    connectionHandler.removeChannel(delegate.getChannelNumber());
  }

  @Override
  boolean canRecover(boolean connectionClosed) {
    boolean recoverable = config.getChannelRecoveryPolicy() != null
        && config.getChannelRecoveryPolicy().allowsRetries();
    return connectionClosed ? recoverable && connectionHandler.canRecover(connectionClosed)
        : recoverable;
  }

  @Override
  RecoveryResult recoverChannel(boolean waitForRecovery) throws Exception {
    // If not currently recovering
    if (recovering.compareAndSet(false, true))
      if (waitForRecovery)
        return recoverChannel();
      else
        ConnectionHandler.RECOVERY_EXECUTORS.submit(new Callable<RecoveryResult>() {
          public RecoveryResult call() throws Exception {
            return recoverChannel();
          }
        });

    return RecoveryResult.Pending;
  }

  /**
   * Atomically recovers the channel.
   * 
   * @throws Exception when recovery fails due to a connection closure
   */
  synchronized RecoveryResult recoverChannel() throws Exception {
    if (circuit.isClosed())
      return RecoveryResult.Succeeded;

    final Map<String, Invocation> consumers = consumerInvocations.isEmpty() ? null
        : new HashMap<String, Invocation>(consumerInvocations);

    try {
      callWithRetries(new Callable<Channel>() {
        @Override
        public Channel call() throws Exception {
          log.info("Recovering {} ", ChannelHandler.this);
          Channel channel = connectionHandler.createChannel(delegate.getChannelNumber());
          migrateConfiguration(channel);
          delegate = channel;
          if (config.isConsumerRecoveryEnabled())
            recoverConsumers(channel, consumers);
          circuit.close();
          return channel;
        }
      }, config.getChannelRecoveryPolicy(), true, false);
      for (ChannelListener listener : config.getChannelListeners())
        listener.onRecovery(proxy);
      return RecoveryResult.Succeeded;
    } catch (Exception e) {
      log.error("Failed to recover {}", this, e);
      interruptWaiters();
      for (ChannelListener listener : config.getChannelListeners())
        listener.onRecoveryFailure(proxy, e);
      ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
      if (sse != null && Exceptions.isConnectionClosure(sse))
        throw e;
      return RecoveryResult.Failed;
    } finally {
      recovering.set(false);
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
  private void recoverConsumers(Channel channel, Map<String, Invocation> consumers)
      throws Exception {
    if (consumers != null)
      for (final Map.Entry<String, Invocation> entry : consumers.entrySet()) {
        Consumer consumer = (Consumer) entry.getValue().args[entry.getValue().args.length - 1];

        try {
          for (ConsumerListener listener : config.getConsumerListeners())
            listener.onBeforeRecovery(consumer, proxy);
          log.info("Recovering consumer-{} via {}", entry.getKey(), this);
          Reflection.invoke(channel, entry.getValue().method, entry.getValue().args);
          for (ConsumerListener listener : config.getConsumerListeners())
            listener.onAfterRecovery(consumer, proxy);
        } catch (Exception e) {
          ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
          log.error("Failed to recover consumer-{} via {}", entry.getKey(), this, e);
          for (ConsumerListener listener : config.getConsumerListeners())
            listener.onRecoveryFailure(consumer, proxy, e);
          if (sse != null) {
            if (!Exceptions.isConnectionClosure(sse))
              consumers.remove(entry.getKey());
            throw e;
          }
        }
      }
  }
}
