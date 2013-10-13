package net.jodah.lyra;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.event.ConsumerListener;
import net.jodah.lyra.internal.util.Assert;
import net.jodah.lyra.retry.RetryPolicy;
import net.jodah.lyra.util.Duration;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * Lyra Options.
 * 
 * @author Jonathan Halterman
 */
public final class LyraOptions {
  private String host;
  private int port = 5672;
  private Address[] addresses;
  private String virtualHost = "/";
  private String username;
  private String password;
  private int requestedHeartbeat;
  private int connectionTimeout;
  private String name;
  private int prefetchCount;
  private ExecutorService consumerThreadPool;
  private RetryPolicy retryPolicy;
  private RetryPolicy recoveryPolicy;
  private RetryPolicy connectRetryPolicy;
  private RetryPolicy connectionRetryPolicy;
  private RetryPolicy channelRetryPolicy;
  private RetryPolicy connectionRecoveryPolicy;
  private RetryPolicy channelRecoveryPolicy;
  Collection<ConnectionListener> connectionListeners = Collections.emptyList();
  Collection<ChannelListener> channelListeners = Collections.emptyList();
  Collection<ConsumerListener> consumerListeners = Collections.emptyList();

  private LyraOptions(Address... addresses) {
    this.addresses = addresses;
  }

  private LyraOptions(String host) {
    this.host = host;
  }

  public Address[] getAddresses() {
    return addresses == null ? new Address[] { new Address(host, port) } : addresses;
  }

  public Collection<ChannelListener> getChannelListeners() {
    return channelListeners;
  }

  public RetryPolicy getChannelRecoveryPolicy() {
    return channelRecoveryPolicy == null ? recoveryPolicy : channelRecoveryPolicy;
  }

  public RetryPolicy getChannelRetryPolicy() {
    return channelRetryPolicy == null ? retryPolicy : channelRetryPolicy;
  }

  public Collection<ConnectionListener> getConnectionListeners() {
    return connectionListeners;
  }

  public RetryPolicy getConnectionRecoveryPolicy() {
    return connectionRecoveryPolicy == null ? recoveryPolicy : connectionRecoveryPolicy;
  }

  public RetryPolicy getConnectionRetryPolicy() {
    return connectionRetryPolicy == null ? retryPolicy : connectionRetryPolicy;
  }

  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public RetryPolicy getConnectRetryPolicy() {
    return connectRetryPolicy == null ? retryPolicy : connectRetryPolicy;
  }

  public Collection<ConsumerListener> getConsumerListeners() {
    return consumerListeners;
  }

  public ExecutorService getConsumerThreadPool() {
    return consumerThreadPool;
  }

  public String getName() {
    return name;
  }

  public String getPassword() {
    return password;
  }

  public int getPrefetchCount() {
    return prefetchCount;
  }

  public int getRequestedHeartbeat() {
    return requestedHeartbeat;
  }

  public String getUsername() {
    return username;
  }

  public String getVirtualHost() {
    return virtualHost;
  }

  /**
   * Sets the {@code channelListeners} to call on channel related events.
   */
  public LyraOptions withChannelListeners(ChannelListener... channelListeners) {
    this.channelListeners = Arrays.asList(channelListeners);
    return this;
  }

  /**
   * Sets the RetryPolicy to use for the recovery of each Channel after an unexpected
   * Connection/Channel closure. Overrides the {@link #withRecoveryPolicy(RetryPolicy) global
   * recovery policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public LyraOptions withChannelRecoveryPolicy(RetryPolicy retryPolicy) {
    this.channelRecoveryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the RetryPolicy to use for handling {@link Channel} invocation errors. Overrides the
   * {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public LyraOptions withChannelRetryPolicy(RetryPolicy retryPolicy) {
    this.channelRetryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the {@code connectionListeners} to call on connection related events.
   */
  public LyraOptions withConnectionListeners(ConnectionListener... connectionListeners) {
    this.connectionListeners = Arrays.asList(connectionListeners);
    return this;
  }

  /**
   * Sets the RetryPolicy to use for the recovery of Connections after an unexpected Connection
   * closure. Overrides the {@link #withRecoveryPolicy(RetryPolicy) global recovery policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public LyraOptions withConnectionRecoveryPolicy(RetryPolicy retryPolicy) {
    this.connectionRecoveryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the RetryPolicy to use for handling {@link Connection} invocation errors. Overrides the
   * {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public LyraOptions withConnectionRetryPolicy(RetryPolicy retryPolicy) {
    this.connectionRetryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Set the connection timeout, zero for infinite, for an individual connection attempt. Overrides
   * the {@link #withRecoveryPolicy(RetryPolicy) global recovery policy}.
   * 
   * @throws NullPointerException if {@code connectionTimeout} is null
   */
  public LyraOptions withConnectionTimeout(Duration connectionTimeout) {
    Assert.notNull(connectionTimeout, "connectionTimeout");
    this.connectionTimeout = (int) connectionTimeout.toMillis();
    return this;
  }

  /**
   * Sets the RetryPolicy to use for handling {@link Connections#create(LyraOptions) connection
   * attempt} errors. Overrides the {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public LyraOptions withConnectRetryPolicy(RetryPolicy retryPolicy) {
    this.connectRetryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the {@code consumerListeners} to call on consumer related events.
   */
  public LyraOptions withConsumerListeners(ConsumerListener... consumerListeners) {
    this.consumerListeners = Arrays.asList(consumerListeners);
    return this;
  }

  /**
   * Sets the thread pool for threads on which consumers will be called. The
   * {@code consumerThreadPool} will not be shutdown when a connection is closed.
   * 
   * @throws NullPointerException if {@code consumerThreadPool} is null
   */
  public LyraOptions withConsumerThreadPool(ExecutorService consumerThreadPool) {
    this.consumerThreadPool = Assert.notNull(consumerThreadPool, "consumerThreadPool");
    return this;
  }

  /**
   * Sets the connection name. Used for logging and consumer thread naming.
   * 
   * @throws NullPointerException if {@code name} is null
   */
  public LyraOptions withName(String name) {
    this.name = Assert.notNull(name, "name");
    return this;
  }

  /**
   * Sets the password.
   * 
   * @throws NullPointerException if {@code password} is null
   */
  public LyraOptions withPassword(String password) {
    this.password = Assert.notNull(password, "password");
    return this;
  }

  /**
   * Set the port.
   */
  public LyraOptions withPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Sets the prefetch count for all channels on the connection.
   */
  public LyraOptions withPrefetchCount(int prefetchCount) {
    this.prefetchCount = prefetchCount;
    return this;
  }

  /**
   * Sets the global RetryPolicy for the recovery of Connections/Channels/Consumers after an
   * unexpected Connection/Channel closure. Can be overridden with specific policies via
   * {@link #withConnectionRecoveryPolicy(RetryPolicy)} and
   * {@link #withChannelRecoveryPolicy(RetryPolicy)}.
   */
  public LyraOptions withRecoveryPolicy(RetryPolicy retryPolicy) {
    this.recoveryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Set the requested heartbeat, zero for none.
   * 
   * @throws NullPointerException if {@code requestedHeartbeat} is null
   */
  public LyraOptions withRequestedHeartbeat(Duration requestedHeartbeat) {
    Assert.notNull(requestedHeartbeat, "requestedHeartbeat");
    this.requestedHeartbeat = (int) requestedHeartbeat.toSeconds();
    return this;
  }

  /**
   * Sets the global RetryPolicy to use for handling {@link Connections#create(LyraOptions)
   * connection attempt}, {@link Connection} invocation, and {@link Channel} invocation errors. Can
   * be overridden with specific policies via {@link #withConnectRetryPolicy(RetryPolicy)},
   * {@link #withConnectionRetryPolicy(RetryPolicy)}, and
   * {@link #withChannelRetryPolicy(RetryPolicy)}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public LyraOptions withRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the username.
   * 
   * @throws NullPointerException if {@code username} is null
   */
  public LyraOptions withUsername(String username) {
    this.username = Assert.notNull(username, "username");
    return this;
  }

  /**
   * Sets the virtual host.
   * 
   * @throws NullPointerException if {@code virtualHost} is null
   */
  public LyraOptions withVirtualHost(String virtualHost) {
    this.virtualHost = Assert.notNull(virtualHost, "virtualHost");
    return this;
  }

  /**
   * Sets the addresses to connect to. Addresses will be attempted in order.
   * 
   * @param addresses formatted as "host1[:port],host2[:port]", etc.
   * @throws NullPointerException if {@code addresses} is null
   * @throws IllegalStateException if host or addresses have already been set
   */
  public static LyraOptions forAddress(String addresses) {
    Assert.state(addresses == null, "Cannot set addresses when a host has already been set");
    return new LyraOptions(Address.parseAddresses(Assert.notNull(addresses, "addresses")));
  }

  /**
   * Sets the addresses to connect to. Addresses will be attempted in order.
   * 
   * @throws NullPointerException if {@code addresses} is null
   * @throws IllegalStateException if host or address has already been set
   */
  public static LyraOptions forAddresses(Address... addresses) {
    Assert.state(addresses == null, "Cannot set addresses when a host has already been set");
    return new LyraOptions(Assert.notNull(addresses, "addresses"));
  }

  /**
   * Sets the host to connect to.
   * 
   * @throws NullPointerException if {@code host} is null
   */
  public static LyraOptions forHost(String host) {
    return new LyraOptions(Assert.notNull(host, "host"));
  }
}
