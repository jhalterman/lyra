package net.jodah.lyra;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import net.jodah.lyra.event.ChannelListener;
import net.jodah.lyra.event.ConnectionListener;
import net.jodah.lyra.event.ConsumerListener;
import net.jodah.lyra.internal.util.Addresses;
import net.jodah.lyra.internal.util.Assert;
import net.jodah.lyra.retry.RetryPolicy;
import net.jodah.lyra.util.Duration;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Lyra Options.
 * 
 * @author Jonathan Halterman
 */
public class Options {
  private ConnectionFactory factory;
  private String host = "localhost";
  private Address[] addresses;
  private String name;
  private ExecutorService consumerThreadPool;
  private RetryPolicy retryPolicy;
  private RetryPolicy recoveryPolicy;
  private RetryPolicy connectRetryPolicy;
  private RetryPolicy connectionRecoveryPolicy;
  private RetryPolicy connectionRetryPolicy;
  private RetryPolicy channelRecoveryPolicy;
  private RetryPolicy channelRetryPolicy;
  Collection<ConnectionListener> connectionListeners = Collections.emptyList();
  Collection<ChannelListener> channelListeners = Collections.emptyList();
  Collection<ConsumerListener> consumerListeners = Collections.emptyList();

  public Options() {
    factory = new ConnectionFactory();
  }

  /**
   * Creates a new Options object for the {@code connectionFactory}.
   * 
   * @throws NullPointerException if {@code connectionFactory} is null
   */
  public Options(ConnectionFactory connectionFactory) {
    this.factory = Assert.notNull(connectionFactory, "connectionFactory");
  }

  private Options(Options options) {
    factory = new ConnectionFactory();
    factory.setClientProperties(options.factory.getClientProperties());
    factory.setConnectionTimeout(options.factory.getConnectionTimeout());
    factory.setHost(options.factory.getHost());
    factory.setPort(options.factory.getPort());
    factory.setUsername(options.factory.getUsername());
    factory.setPassword(options.factory.getPassword());
    factory.setVirtualHost(options.factory.getVirtualHost());
    factory.setRequestedChannelMax(options.factory.getRequestedChannelMax());
    factory.setRequestedFrameMax(options.factory.getRequestedFrameMax());
    factory.setRequestedHeartbeat(options.factory.getRequestedHeartbeat());
    factory.setSaslConfig(options.factory.getSaslConfig());
    factory.setSocketFactory(options.factory.getSocketFactory());
    host = options.host;
    addresses = options.addresses;
    name = options.name;
    consumerThreadPool = options.consumerThreadPool;
    retryPolicy = options.retryPolicy;
    recoveryPolicy = options.recoveryPolicy;
    connectRetryPolicy = options.connectRetryPolicy;
    connectionRecoveryPolicy = options.connectionRecoveryPolicy;
    connectionRetryPolicy = options.connectionRetryPolicy;
    channelRecoveryPolicy = options.channelRecoveryPolicy;
    channelRetryPolicy = options.channelRetryPolicy;
    connectionListeners = new ArrayList<ConnectionListener>(options.connectionListeners);
    channelListeners = new ArrayList<ChannelListener>(options.channelListeners);
    consumerListeners = new ArrayList<ConsumerListener>(options.consumerListeners);
  }

  /**
   * Returns a new copy of the options.
   */
  public Options copy() {
    return new Options(this);
  }

  public Address[] getAddresses() {
    return addresses == null ? new Address[] { new Address(host, factory.getPort()) } : addresses;
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

  /**
   * Returns the ConnectionFactory for the options.
   */
  public ConnectionFactory getConnectionFactory() {
    return factory;
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

  /**
   * Sets the {@code addresses}.
   * 
   * @throws NullPointerException if {@code addresses} is null
   */
  public Options withAddresses(Address... addresses) {
    addresses = Assert.notNull(addresses, "addresses");
    return this;
  }

  /**
   * Sets the {@code addresses}.
   * 
   * @param addresses formatted as "host1[:port],host2[:port]", etc.
   * @throws NullPointerException if {@code addresses} is null
   */
  public Options withAddresses(String addresses) {
    this.addresses = Address.parseAddresses(Assert.notNull(addresses, "addresses"));
    return this;
  }

  /**
   * Sets the {@code channelListeners} to call on channel related events.
   */
  public Options withChannelListeners(ChannelListener... channelListeners) {
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
  public Options withChannelRecoveryPolicy(RetryPolicy retryPolicy) {
    this.channelRecoveryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the RetryPolicy to use for handling {@link Channel} invocation errors. Overrides the
   * {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public Options withChannelRetryPolicy(RetryPolicy retryPolicy) {
    this.channelRetryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the client properties.
   * 
   * @throws NullPointerException if {@code clientProperties} is null
   */
  public Options withClientProperties(Map<String, Object> clientProperties) {
    factory.setClientProperties(Assert.notNull(clientProperties, "clientProperties"));
    return this;
  }

  /**
   * Sets the {@code connectionFactory}.
   * 
   * @throws NullPointerException if {@code connectionFactory} is null
   */
  public Options withConnectionFactory(ConnectionFactory connectionFactory) {
    this.factory = Assert.notNull(connectionFactory, "connectionFactory");
    return this;
  }

  /**
   * Sets the {@code connectionListeners} to call on connection related events.
   */
  public Options withConnectionListeners(ConnectionListener... connectionListeners) {
    this.connectionListeners = Arrays.asList(connectionListeners);
    return this;
  }

  /**
   * Sets the RetryPolicy to use for the recovery of Connections after an unexpected Connection
   * closure. Overrides the {@link #withRecoveryPolicy(RetryPolicy) global recovery policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public Options withConnectionRecoveryPolicy(RetryPolicy retryPolicy) {
    this.connectionRecoveryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the RetryPolicy to use for handling {@link Connection} invocation errors. Overrides the
   * {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public Options withConnectionRetryPolicy(RetryPolicy retryPolicy) {
    this.connectionRetryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Set the connection timeout, zero for infinite, for an individual connection attempt. Overrides
   * the {@link #withRecoveryPolicy(RetryPolicy) global recovery policy}.
   * 
   * @throws NullPointerException if {@code connectionTimeout} is null
   */
  public Options withConnectionTimeout(Duration connectionTimeout) {
    factory.setConnectionTimeout((int) connectionTimeout.toMillis());
    return this;
  }

  /**
   * Sets the RetryPolicy to use for handling {@link Connections#create(Options) connection attempt}
   * errors. Overrides the {@link #withRetryPolicy(RetryPolicy) global retry policy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public Options withConnectRetryPolicy(RetryPolicy retryPolicy) {
    this.connectRetryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the {@code consumerListeners} to call on consumer related events.
   */
  public Options withConsumerListeners(ConsumerListener... consumerListeners) {
    this.consumerListeners = Arrays.asList(consumerListeners);
    return this;
  }

  /**
   * Sets the thread pool for threads on which consumers will be called. The
   * {@code consumerThreadPool} will not be shutdown when a connection is closed.
   * 
   * @throws NullPointerException if {@code consumerThreadPool} is null
   */
  public Options withConsumerThreadPool(ExecutorService consumerThreadPool) {
    this.consumerThreadPool = Assert.notNull(consumerThreadPool, "consumerThreadPool");
    return this;
  }

  /**
   * Sets the {@code host}.
   * 
   * @throws NullPointerException if {@code host} is null
   */
  public Options withHost(String host) {
    this.host = Assert.notNull(host, "host");
    return this;
  }

  /**
   * Sets the {@code hosts}.
   * 
   * @throws NullPointerException if {@code hosts} is null
   */
  public Options withHosts(String... hosts) {
    this.addresses = Addresses.addressesFor(Assert.notNull(hosts, "hosts"), 5672);
    return this;
  }

  /**
   * Sets the connection name. Used for logging and consumer thread naming.
   * 
   * @throws NullPointerException if {@code name} is null
   */
  public Options withName(String name) {
    this.name = Assert.notNull(name, "name");
    return this;
  }

  /**
   * Sets the password.
   */
  public Options withPassword(String password) {
    factory.setPassword(password);
    return this;
  }

  /**
   * Set the port.
   */
  public Options withPort(int port) {
    factory.setPort(port);
    return this;
  }

  /**
   * Sets the global RetryPolicy for the recovery of Connections/Channels/Consumers after an
   * unexpected Connection/Channel closure. Can be overridden with specific policies via
   * {@link #withConnectionRecoveryPolicy(RetryPolicy)} and
   * {@link #withChannelRecoveryPolicy(RetryPolicy)}.
   */
  public Options withRecoveryPolicy(RetryPolicy retryPolicy) {
    this.recoveryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Set the requested heartbeat, zero for none.
   * 
   * @throws NullPointerException if {@code requestedHeartbeat} is null
   */
  public Options withRequestedHeartbeat(Duration requestedHeartbeat) {
    factory.setRequestedHeartbeat((int) requestedHeartbeat.toMillis());
    return this;
  }

  /**
   * Sets the global RetryPolicy to use for handling {@link Connections#create(Options) connection
   * attempt}, {@link Connection} invocation, and {@link Channel} invocation errors. Can be
   * overridden with specific policies via {@link #withConnectRetryPolicy(RetryPolicy)},
   * {@link #withConnectionRetryPolicy(RetryPolicy)}, and
   * {@link #withChannelRetryPolicy(RetryPolicy)}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public Options withRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Assert.notNull(retryPolicy, "retryPolicy");
    return this;
  }

  /**
   * Sets the SocketFactory to create connections with.
   * 
   * @throws NullPointerException if {@code hosts} is null
   */
  public Options withSocketFactory(SocketFactory socketFactory) {
    factory.setSocketFactory(Assert.notNull(socketFactory, "socketFactory"));
    return this;
  }

  /**
   * Enabled SSL using SSLv3.
   */
  public Options withSsl() throws NoSuchAlgorithmException, KeyManagementException {
    factory.useSslProtocol();
    return this;
  }

  /**
   * Sets the initialized {@code sslContext} to use.
   * 
   * @throws NullPointerException if {@code sslContext} is null
   */
  public Options withSslProtocol(SSLContext sslContext) {
    factory.useSslProtocol(sslContext);
    return this;
  }

  /**
   * Sets the {@code sslProtocol} to use.
   * 
   * @throws NullPointerException if {@code sslProtocol} is null
   */
  public Options withSslProtocol(String sslProtocol) throws NoSuchAlgorithmException,
      KeyManagementException {
    factory.useSslProtocol(Assert.notNull(sslProtocol, "sslProtocol"));
    return this;
  }

  /**
   * Sets the {@code sslProtocol} and {@code trustManager} to use.
   * 
   * @throws NullPointerException if {@code sslProtocol} or {@code trustManager} are null
   */
  public Options withSslProtocol(String sslProtocol, TrustManager trustManager)
      throws NoSuchAlgorithmException, KeyManagementException {
    factory.useSslProtocol(Assert.notNull(sslProtocol, "sslProtocol"),
        Assert.notNull(trustManager, "trustManager"));
    return this;
  }

  /**
   * Sets the username.
   * 
   * @throws NullPointerException if {@code username} is null
   */
  public Options withUsername(String username) {
    factory.setUsername(Assert.notNull(username, "username"));
    return this;
  }

  /**
   * Sets the virtual host.
   * 
   * @throws NullPointerException if {@code virtualHost} is null
   */
  public Options withVirtualHost(String virtualHost) {
    factory.setVirtualHost(Assert.notNull(virtualHost, "virtualHost"));
    return this;
  }
}
