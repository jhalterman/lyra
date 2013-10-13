package net.jodah.lyra.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jodah.lyra.LyraOptions;
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
  private final LyraOptions options;
  private final AtomicBoolean recovering = new AtomicBoolean();
  private final Map<String, Invocation> consumerInvocations = Collections.synchronizedMap();
  private List<ConfirmListener> confirmListeners = new CopyOnWriteArrayList<ConfirmListener>();
  private List<FlowListener> flowListeners = new CopyOnWriteArrayList<FlowListener>();
  private List<ReturnListener> returnListeners = new CopyOnWriteArrayList<ReturnListener>();
  private boolean flowDisabled;
  private Invocation basicQos;
  private boolean confirmSelect;
  private boolean txSelect;
  Channel proxy;
  Channel delegate;

  public ChannelHandler(ConnectionHandler connectionHandler, Channel delegate, LyraOptions options) {
    this.connectionHandler = connectionHandler;
    this.delegate = delegate;
    this.options = options;
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    if (closed)
      throw new AlreadyClosedException("Attempt to use closed channel", proxy);
    handleCommonMethods(delegate, method, args);

    return callWithRetries(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        String methodName = method.getName();
        if ("basicCancel".equals(methodName) && args[0] != null)
          consumerInvocations.remove((String) args[0]);

        Object result = Reflection.invoke(delegate, method, args);

        if ("basicConsume".equals(methodName)) {
          // Replace actual consumerTag
          if (args.length > 3)
            args[2] = result;
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
    }, options.getChannelRetryPolicy(), false);
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
    boolean recoverable = options.getChannelRecoveryPolicy() != null
        && options.getChannelRecoveryPolicy().allowsRetries();
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
   * Migrates the channel's configuration to the given {@code channel}.
   */
  private void migrateConfiguration(Channel channel) throws Exception {
    channel.setDefaultConsumer(channel.getDefaultConsumer());
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

  synchronized RecoveryResult recoverChannel() throws Exception {
    if (circuit.isClosed())
      return RecoveryResult.Succeeded;

    final Map<String, Invocation> consumers = consumerInvocations.isEmpty() ? null
        : new HashMap<String, Invocation>(consumerInvocations);

    try {
      delegate = callWithRetries(new Callable<Channel>() {
        @Override
        public Channel call() throws Exception {
          log.info("Recovering {} ", ChannelHandler.this);
          Channel channel = connectionHandler.createChannel(delegate.getChannelNumber());
          recoverConsumers(channel, consumers);
          migrateConfiguration(channel);
          circuit.close();
          for (ChannelListener listener : options.getChannelListeners())
            listener.onRecovery(proxy);
          return channel;
        }
      }, options.getChannelRecoveryPolicy(), true);
      return RecoveryResult.Succeeded;
    } catch (Exception e) {
      ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
      log.error("Failed to recover {}", this, e);
      for (ChannelListener listener : options.getChannelListeners())
        listener.onRecoveryFailure(proxy, e);
      if (sse != null && sse.isHardError())
        throw e;
      return RecoveryResult.Failed;
    } finally {
      recovering.set(false);
    }
  }

  /**
   * Recovers the channel's consumers to given {@code channel}. If a consumer recovery fails due to
   * a channel closure, then we will not attempt to recover that consumer again.
   */
  private void recoverConsumers(Channel channel, Map<String, Invocation> consumers)
      throws Exception {
    if (consumers != null)
      for (final Map.Entry<String, Invocation> entry : consumers.entrySet()) {
        Consumer consumer = (Consumer) entry.getValue().args[entry.getValue().args.length - 1];

        try {
          log.info("Recovering consumer-{} via {}", entry.getKey(), this);
          Reflection.invoke(channel, entry.getValue().method, entry.getValue().args);
          for (ConsumerListener listener : options.getConsumerListeners())
            listener.onRecovery(consumer);
        } catch (Exception e) {
          ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
          log.error("Failed to recover consumer-{} via {}", entry.getKey(), this, e);
          for (ConsumerListener listener : options.getConsumerListeners())
            listener.onRecoveryFailure(consumer, e);
          if (sse != null) {
            if (!sse.isHardError())
              consumers.remove(entry.getKey());
            throw e;
          }
        }
      }
  }
}
