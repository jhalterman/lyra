package net.jodah.lyra.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.jodah.lyra.LyraOptions;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConsumerListener;
import net.jodah.lyra.internal.util.Exceptions;
import net.jodah.lyra.internal.util.Reflection;
import net.jodah.lyra.internal.util.ThrowableCallable;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class ChannelHandler extends RetryableResource implements InvocationHandler {
  private static final String BASIC_CONSUME_METHOD_NAME = "basicConsume";
  private static final String BASIC_CANCEL_METHOD_NAME = "basicCancel";

  private final ConnectionHandler connectionHandler;
  private final LyraOptions options;
  private final boolean explicitNumber;
  private final Map<String, Invocation> consumerInvocations = new ConcurrentHashMap<String, Invocation>();
  Channel proxy;
  Channel delegate;

  public ChannelHandler(ConnectionHandler connectionHandler, Channel delegate, LyraOptions options,
      boolean explicitNumber) {
    this.connectionHandler = connectionHandler;
    this.delegate = delegate;
    this.options = options;
    this.explicitNumber = explicitNumber;
  }

  @Override
  public void afterClosure() {
    if (!explicitNumber && connectionHandler.pooledChannels != null
        && connectionHandler.pooledChannels.size() < options.getChannelPoolSize())
      connectionHandler.pooledChannels.addFirst(delegate);
    else
      connectionHandler.removeChannel(delegate.getChannelNumber());
  }

  @Override
  public Object invoke(Object ignored, final Method method, final Object[] args) throws Throwable {
    if (closed)
      throw new AlreadyClosedException("Attempt to use closed channel", proxy);
    handleCommonMethods(delegate, method, args);

    return callWithRetries(new ThrowableCallable<Object>() {
      @Override
      public Object call() throws Throwable {
        if (method.getName().equals(BASIC_CANCEL_METHOD_NAME) && args[0] != null)
          consumerInvocations.remove((String) args[0]);

        Object result = Reflection.invoke(delegate, method, args);

        if (method.getName().equals(BASIC_CONSUME_METHOD_NAME)) {
          // Replace actual consumerTag
          if (args.length > 3)
            args[2] = result;
          consumerInvocations.put((String) result, new Invocation(method, args));
          log.info("Created consumer-{} of {} via {}", result, args[0], ChannelHandler.this);
        }

        return result;
      }
    }, options.getChannelRetryPolicy(), false);
  }

  @Override
  public String toString() {
    return String.format("channel-%s on %s", delegate.getChannelNumber(), connectionHandler);
  }

  @Override
  boolean canRecover(boolean connectionClosed) {
    boolean recoverable = options.getChannelRecoveryPolicy() != null
        && options.getChannelRecoveryPolicy().allowsRetries();
    return connectionClosed ? recoverable && connectionHandler.canRecover(connectionClosed)
        : recoverable;
  }

  @Override
  boolean recoverChannel() throws Throwable {
    final Map<String, Invocation> consumers = consumerInvocations.isEmpty() ? Collections.<String, Invocation>emptyMap()
        : new HashMap<String, Invocation>(consumerInvocations);

    try {
      delegate = callWithRetries(new ThrowableCallable<Channel>() {
        @Override
        public Channel call() throws Throwable {
          log.info("Recovering {} ", ChannelHandler.this);
          Channel channel = !explicitNumber && connectionHandler.pooledChannels != null ? connectionHandler.pooledChannels.pollFirst()
              : null;
          if (channel == null)
            channel = connectionHandler.createChannel(delegate.getChannelNumber());
          recoverConsumers(consumers);

          // Migrate shutdown listeners
          synchronized (shutdownListeners) {
            for (ShutdownListener listener : shutdownListeners)
              channel.addShutdownListener(listener);
          }

          circuit.close();
          for (ChannelListener listener : options.getChannelListeners())
            listener.onRecovery(proxy);
          return channel;
        }
      }, options.getChannelRecoveryPolicy(), true);
      return true;
    } catch (Throwable t) {
      ShutdownSignalException sse = Exceptions.extractCause(t, ShutdownSignalException.class);
      log.error("Failed to recover {}", this, t);
      for (ChannelListener listener : options.getChannelListeners())
        listener.onRecoveryFailure(proxy, t);
      if (sse != null && sse.isHardError())
        throw t;
      return false;
    }
  }

  void setProxy(Channel proxy) {
    this.proxy = proxy;
  }

  /**
   * Recovers consumers. If a consumer recovery fails due to a channel closure, then we will not
   * attempt to recover that consumer again.
   */
  private void recoverConsumers(Map<String, Invocation> consumers) throws Throwable {
    for (final Map.Entry<String, Invocation> entry : consumers.entrySet()) {
      Consumer consumer = (Consumer) entry.getValue().args[entry.getValue().args.length - 1];

      try {
        log.info("Recovering consumer-{} via {}", entry.getKey(), this);
        Reflection.invoke(delegate, entry.getValue().method, entry.getValue().args);
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
