package net.jodah.lyra.config;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeoutException;

import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.event.ConsumerListener;
import net.jodah.lyra.internal.util.Assert;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Lyra configuration. Changes are reflected in the resources created with this configuration.
 * 
 * @author Jonathan Halterman
 */
public class Config implements ConnectionConfig {
  @SuppressWarnings("unchecked") private static final Class<Exception>[] RECURRING_EXCEPTIONS = (Class<Exception>[]) new Class<?>[] {
      SocketTimeoutException.class, ConnectException.class, AlreadyClosedException.class, TimeoutException.class,
      NoRouteToHostException.class };

  private final Config parent;
  private RetryPolicy retryPolicy;
  private RecoveryPolicy recoveryPolicy;
  private RetryPolicy connectRetryPolicy;
  private RecoveryPolicy connectionRecoveryPolicy;
  private RetryPolicy connectionRetryPolicy;
  private RecoveryPolicy channelRecoveryPolicy;
  private RetryPolicy channelRetryPolicy;
  private Boolean exchangeRecovery;
  private Boolean queueRecovery;
  private Boolean consumerRecovery;
  private Collection<ConnectionListener> connectionListeners;
  private Collection<ChannelListener> channelListeners;
  private Collection<ConsumerListener> consumerListeners;
  private Set<Class<? extends Exception>> retryableExceptions;
  private Set<Class<? extends Exception>> recoverableExceptions;

  public Config() {
    parent = null;
    retryableExceptions = new CopyOnWriteArraySet<Class<? extends Exception>>();
    recoverableExceptions = new CopyOnWriteArraySet<Class<? extends Exception>>();
    for (Class<Exception> e : RECURRING_EXCEPTIONS) {
      retryableExceptions.add(e);
      recoverableExceptions.add(e);
    }
  }

  /**
   * Creates a new Config object that inherits configuration from the {@code parent}.
   */
  public Config(Config parent) {
    this.parent = parent;
  }

  /**
   * Returns the {@code channel} as a {@link ConfigurableChannel}.
   * 
   * @throws IllegalArgumentException if {@code channel} was not created by Lyra
   */
  public static ConfigurableChannel of(Channel channel) {
    Assert.isTrue(channel instanceof ConfigurableChannel, "The channel {} was not created by Lyra", channel);
    return (ConfigurableChannel) channel;
  }

  /**
   * Returns the {@code connection} as a {@link ConfigurableConnection}.
   * 
   * @throws IllegalArgumentException if {@code connection} was not created by Lyra
   */
  public static ConfigurableConnection of(Connection connection) {
    Assert.isTrue(connection instanceof ConfigurableConnection, "The connection {} was not created by Lyra", connection);
    return (ConfigurableConnection) connection;
  }

  @Override
  public Collection<ChannelListener> getChannelListeners() {
    return channelListeners != null ? channelListeners : parent != null ? parent.getChannelListeners()
      : Collections.<ChannelListener>emptyList();
  }

  @Override
  public RecoveryPolicy getChannelRecoveryPolicy() {
    RecoveryPolicy result = channelRecoveryPolicy == null ? recoveryPolicy : channelRecoveryPolicy;
    return result != null ? result : parent != null ? parent.getChannelRecoveryPolicy() : null;
  }

  @Override
  public RetryPolicy getChannelRetryPolicy() {
    RetryPolicy result = channelRetryPolicy == null ? retryPolicy : channelRetryPolicy;
    return result != null ? result : parent != null ? parent.getChannelRetryPolicy() : null;
  }

  @Override
  public Collection<ConnectionListener> getConnectionListeners() {
    return connectionListeners != null ? connectionListeners : parent != null ? parent.getConnectionListeners()
      : Collections.<ConnectionListener>emptyList();
  }

  @Override
  public RecoveryPolicy getConnectionRecoveryPolicy() {
    RecoveryPolicy result = connectionRecoveryPolicy == null ? recoveryPolicy : connectionRecoveryPolicy;
    return result != null ? result : parent != null ? parent.getConnectionRecoveryPolicy() : null;
  }

  @Override
  public RetryPolicy getConnectionRetryPolicy() {
    RetryPolicy result = connectionRetryPolicy == null ? retryPolicy : connectionRetryPolicy;
    return result != null ? result : parent != null ? parent.getConnectionRetryPolicy() : null;
  }

  /**
   * Sets the policy to use for handling {@link Connections#create(ConnectionOptions, Config, ClassLoader)
   * connection attempt} errors. Overrides the {@link #withRetryPolicy(RetryPolicy) global retry
   * policy}.
   * 
   * @see #withConnectRetryPolicy(RetryPolicy)
   * @see #withRetryPolicy(RetryPolicy)
   */
  public RetryPolicy getConnectRetryPolicy() {
    RetryPolicy result = connectRetryPolicy == null ? retryPolicy : connectRetryPolicy;
    return result != null ? result : parent != null ? parent.getConnectRetryPolicy() : null;
  }

  @Override
  public Collection<ConsumerListener> getConsumerListeners() {
    return consumerListeners != null ? consumerListeners : parent != null ? parent.getConsumerListeners()
      : Collections.<ConsumerListener>emptyList();
  }

  /**
   * Returns the exceptions which will be recovered from. By default these will include
   * {@code SocketTimeoutException}, {@code ConnectException}, {@code AlreadyClosedException}, and
   * {@code TimeoutException}, but this set can be mutated directly to change the recoverable
   * exceptions.
   */
  public Set<Class<? extends Exception>> getRecoverableExceptions() {
    return recoverableExceptions != null ? recoverableExceptions : parent != null ? parent.getRecoverableExceptions()
      : Collections.<Class<? extends Exception>>emptySet();
  }

  /**
   * Returns the exceptions for which invocations will be retried. By default these will include
   * {@code SocketTimeoutException}, {@code ConnectException}, {@code AlreadyClosedException}, and
   * {@code TimeoutException}, but this set can be mutated directly to change the retryable
   * exceptions.
   */
  public Set<Class<? extends Exception>> getRetryableExceptions() {
    return retryableExceptions != null ? retryableExceptions : parent != null ? parent.getRetryableExceptions()
      : Collections.<Class<? extends Exception>>emptySet();
  }

  @Override
  public boolean isConsumerRecoveryEnabled() {
    Boolean result = consumerRecovery != null ? consumerRecovery : parent != null ? parent.isConsumerRecoveryEnabled()
      : null;
    return isRecoveryEnabled(result);
  }

  @Override
  public boolean isExchangeRecoveryEnabled() {
    Boolean result = exchangeRecovery != null ? exchangeRecovery : parent != null ? parent.isExchangeRecoveryEnabled()
      : null;
    return isRecoveryEnabled(result);
  }

  @Override
  public boolean isQueueRecoveryEnabled() {
    Boolean result = queueRecovery != null ? queueRecovery : parent != null ? parent.isQueueRecoveryEnabled() : null;
    return isRecoveryEnabled(result);
  }

  @Override
  public Config withChannelListeners(ChannelListener... channelListeners) {
    this.channelListeners = Arrays.asList(channelListeners);
    return this;
  }

  @Override
  public Config withChannelRecoveryPolicy(RecoveryPolicy channelRecoveryPolicy) {
    this.channelRecoveryPolicy = channelRecoveryPolicy;
    return this;
  }

  @Override
  public Config withChannelRetryPolicy(RetryPolicy channelRetryPolicy) {
    this.channelRetryPolicy = channelRetryPolicy;
    return this;
  }

  @Override
  public Config withConnectionListeners(ConnectionListener... connectionListeners) {
    this.connectionListeners = Arrays.asList(connectionListeners);
    return this;
  }

  @Override
  public Config withConnectionRecoveryPolicy(RecoveryPolicy connectionRecoveryPolicy) {
    this.connectionRecoveryPolicy = connectionRecoveryPolicy;
    return this;
  }

  @Override
  public Config withConnectionRetryPolicy(RetryPolicy connectionRetryPolicy) {
    this.connectionRetryPolicy = connectionRetryPolicy;
    return this;
  }

  /**
   * Sets the policy to use for handling initial
   * {@link Connections#create(ConnectionOptions, Config, ClassLoader) connection attempt} errors. Overrides the
   * {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   */
  public Config withConnectRetryPolicy(RetryPolicy connectRetryPolicy) {
    this.connectRetryPolicy = connectRetryPolicy;
    return this;
  }

  @Override
  public Config withConsumerListeners(ConsumerListener... consumerListeners) {
    this.consumerListeners = Arrays.asList(consumerListeners);
    return this;
  }

  @Override
  public Config withConsumerRecovery(boolean enabled) {
    consumerRecovery = Boolean.valueOf(enabled);
    return this;
  }

  @Override
  public Config withExchangeRecovery(boolean enabled) {
    exchangeRecovery = Boolean.valueOf(enabled);
    return this;
  }

  @Override
  public Config withQueueRecovery(boolean enabled) {
    queueRecovery = Boolean.valueOf(enabled);
    return this;
  }

  /**
   * Sets the policy to use for the recovery of Connections/Channels/Consumers after an unexpected
   * Connection/Channel closure. Can be overridden with specific policies via
   * {@link #withConnectionRecoveryPolicy(RecoveryPolicy)} and
   * {@link #withChannelRecoveryPolicy(RecoveryPolicy)}.
   */
  public Config withRecoveryPolicy(RecoveryPolicy recoveryPolicy) {
    this.recoveryPolicy = recoveryPolicy;
    return this;
  }

  /**
   * Sets the policy to use for handling {@link Connections#create(ConnectionOptions, Config, ClassLoader)
   * connection attempt}, {@link Connection} invocation, and {@link Channel} invocation errors. Can
   * be overridden with specific policies via {@link #withConnectRetryPolicy(RetryPolicy)},
   * {@link #withConnectionRetryPolicy(RetryPolicy)}, and
   * {@link #withChannelRetryPolicy(RetryPolicy)}.
   */
  public Config withRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  private Boolean isRecoveryEnabled(Boolean recovery) {
    if (recovery != null)
      return recovery;

    RecoveryPolicy policy = getChannelRecoveryPolicy();
    return policy != null && policy.allowsAttempts();
  }
}
