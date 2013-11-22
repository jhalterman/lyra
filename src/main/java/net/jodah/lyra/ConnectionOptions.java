package net.jodah.lyra;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import net.jodah.lyra.internal.util.Addresses;
import net.jodah.lyra.internal.util.Assert;
import net.jodah.lyra.util.Duration;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Connection options. Changes will not effect connections that have already been created.
 *
 * @author Jonathan Halterman
 */
public class ConnectionOptions {
  private ConnectionFactory factory;
  private String host = "localhost";
  private AddressResolver addressResolver;
  private String name;
  private ExecutorService executor;

  public ConnectionOptions() {
    factory = new ConnectionFactory();
  }

  /**
   * Creates a new Options object for the {@code connectionFactory}.
   *
   * @throws NullPointerException if {@code connectionFactory} is null
   */
  public ConnectionOptions(ConnectionFactory connectionFactory) {
    this.factory = Assert.notNull(connectionFactory, "connectionFactory");
    withHost(factory.getHost());
  }

  private ConnectionOptions(ConnectionOptions options) {
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
    addressResolver = options.addressResolver;
    name = options.name;
    executor = options.executor;
  }

  /**
   * Returns a new copy of the options.
   */
  public ConnectionOptions copy() {
    return new ConnectionOptions(this);
  }

  /**
   * Returns the addresses to attempt connections to, in round-robin order.
   *
   * @see #withAddresses(Address...)
   * @see #withAddresses(String)
   * @see #withHost(String)
   * @see #withHosts(String...)
   */
  public Address[] getAddresses() {
    Address address = new Address(host, factory.getPort());
    return addressResolver == null ? new Address[] {address} : new Address[] {addressResolver.nextAddress()};
  }

  /**
   * Returns the ConnectionFactory for the options.
   */
  public ConnectionFactory getConnectionFactory() {
    return factory;
  }

  /**
   * Returns the consumer executor.
   *
   * @see #withConsumerExecutor(ExecutorService)
   */
  public ExecutorService getConsumerExecutor() {
    return executor;
  }

  public String getName() {
    return name;
  }

  /**
   * Sets the {@code addressResolver} to resolve addresses
   *
   * @throws NullPointerException if {@code addressResolver} is null
   */
  public ConnectionOptions withAddressResolver(AddressResolver addressResolver) {
    this.addressResolver = Assert.notNull(addressResolver, "addressResolver");
    return this;
  }

  /**
   * Sets the {@code addresses} to attempt connections to, in round-robin order.
   *
   * @throws NullPointerException if {@code addresses} is null
   */
  public ConnectionOptions withAddresses(Address... addresses) {
    Address[] _addresses = Assert.notNull(addresses, "addresses");
    this.addressResolver = new DefaultAddressResolver(_addresses);
    return this;
  }

  /**
   * Sets the {@code addresses}.
   *
   * @param addresses formatted as "host1[:port],host2[:port]", etc.
   * @throws NullPointerException if {@code addresses} is null
   */
  public ConnectionOptions withAddresses(String addresses) {
    Address[] _addresses = Address.parseAddresses(Assert.notNull(addresses, "addresses"));
    this.addressResolver = new DefaultAddressResolver(_addresses);
    return this;
  }

  /**
   * Sets the client properties.
   *
   * @throws NullPointerException if {@code clientProperties} is null
   */
  public ConnectionOptions withClientProperties(Map<String, Object> clientProperties) {
    factory.setClientProperties(Assert.notNull(clientProperties, "clientProperties"));
    return this;
  }

  /**
   * Sets the {@code connectionFactory}.
   *
   * @throws NullPointerException if {@code connectionFactory} is null
   */
  public ConnectionOptions withConnectionFactory(ConnectionFactory connectionFactory) {
    this.factory = Assert.notNull(connectionFactory, "connectionFactory");
    return this;
  }

  /**
   * Set the connection timeout, zero for infinite, for an individual connection attempt.
   *
   * @throws NullPointerException if {@code connectionTimeout} is null
   */
  public ConnectionOptions withConnectionTimeout(Duration connectionTimeout) {
    factory.setConnectionTimeout((int) connectionTimeout.toMillis());
    return this;
  }

  /**
   * Sets the executor used to handle consumer callbacks. The {@code executor} will not be shutdown
   * when a connection is closed.
   *
   * @throws NullPointerException if {@code executor} is null
   */
  public ConnectionOptions withConsumerExecutor(ExecutorService executor) {
    this.executor = Assert.notNull(executor, "executor");
    return this;
  }

  /**
   * Sets the {@code host}.
   *
   * @throws NullPointerException if {@code host} is null
   */
  public ConnectionOptions withHost(String host) {
    this.host = Assert.notNull(host, "host");
    return this;
  }

  /**
   * Sets the {@code hosts} to attempt connections to, in round-robin order.
   *
   * @throws NullPointerException if {@code hosts} is null
   */
  public ConnectionOptions withHosts(String... hosts) {
    Address[] _addresses = Addresses.addressesFor(Assert.notNull(hosts, "hosts"), 5672);
    this.addressResolver = new DefaultAddressResolver(_addresses);
    return this;
  }

  /**
   * Sets the connection name. Used for logging and consumer thread naming.
   *
   * @throws NullPointerException if {@code name} is null
   */
  public ConnectionOptions withName(String name) {
    this.name = Assert.notNull(name, "name");
    return this;
  }

  /**
   * Sets the password.
   */
  public ConnectionOptions withPassword(String password) {
    factory.setPassword(password);
    return this;
  }

  /**
   * Set the port.
   */
  public ConnectionOptions withPort(int port) {
    factory.setPort(port);
    return this;
  }

  /**
   * Set the requested heartbeat, zero for none.
   *
   * @throws NullPointerException if {@code requestedHeartbeat} is null
   */
  public ConnectionOptions withRequestedHeartbeat(Duration requestedHeartbeat) {
    factory.setRequestedHeartbeat((int) requestedHeartbeat.toMillis());
    return this;
  }

  /**
   * Sets the SocketFactory to create connections with.
   *
   * @throws NullPointerException if {@code hosts} is null
   */
  public ConnectionOptions withSocketFactory(SocketFactory socketFactory) {
    factory.setSocketFactory(Assert.notNull(socketFactory, "socketFactory"));
    return this;
  }

  /**
   * Enabled SSL using SSLv3.
   */
  public ConnectionOptions withSsl() throws NoSuchAlgorithmException, KeyManagementException {
    factory.useSslProtocol();
    return this;
  }

  /**
   * Sets the initialized {@code sslContext} to use.
   *
   * @throws NullPointerException if {@code sslContext} is null
   */
  public ConnectionOptions withSslProtocol(SSLContext sslContext) {
    factory.useSslProtocol(sslContext);
    return this;
  }

  /**
   * Sets the {@code sslProtocol} to use.
   *
   * @throws NullPointerException if {@code sslProtocol} is null
   */
  public ConnectionOptions withSslProtocol(String sslProtocol) throws NoSuchAlgorithmException,
      KeyManagementException {
    factory.useSslProtocol(Assert.notNull(sslProtocol, "sslProtocol"));
    return this;
  }

  /**
   * Sets the {@code sslProtocol} and {@code trustManager} to use.
   *
   * @throws NullPointerException if {@code sslProtocol} or {@code trustManager} are null
   */
  public ConnectionOptions withSslProtocol(String sslProtocol, TrustManager trustManager)
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
  public ConnectionOptions withUsername(String username) {
    factory.setUsername(Assert.notNull(username, "username"));
    return this;
  }

  /**
   * Sets the virtual host.
   *
   * @throws NullPointerException if {@code virtualHost} is null
   */
  public ConnectionOptions withVirtualHost(String virtualHost) {
    factory.setVirtualHost(Assert.notNull(virtualHost, "virtualHost"));
    return this;
  }
}
