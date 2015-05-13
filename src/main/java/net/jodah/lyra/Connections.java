package net.jodah.lyra;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeoutException;

import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.ConfigurableConnection;
import net.jodah.lyra.internal.ConnectionHandler;
import net.jodah.lyra.internal.util.Assert;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Creates Lyra managed Connections through which Lyra managed Channels and Consumers can be
 * created.
 * 
 * @author Jonathan Halterman
 */
public final class Connections {
  private static final Class<?>[] CONNECTION_TYPES = { ConfigurableConnection.class };

  private Connections() {
  }

  /**
   * Creates and returns a new Lyra managed ConfigurableConnection for the given {@code config}. If
   * the connection attempt fails, retries will be performed according to the
   * {@link Config#getConnectRetryPolicy() configured RetryPolicy} before throwing the failure.
   * 
   * @throws NullPointerException if {@code connectionFactory} or {@code config} are null
   * @throws IOException if the connection could not be created
   */
  public static ConfigurableConnection create(Config config) throws IOException, TimeoutException {
    return create(new ConnectionOptions(), config);
  }

  /**
   * Creates and returns a new Lyra managed ConfigurableConnection for the given
   * {@code connectionFactory} and {@code config}. If the connection attempt fails, retries will be
   * performed according to the {@link Config#getConnectRetryPolicy() configured RetryPolicy} before
   * throwing the failure.
   * 
   * @throws NullPointerException if {@code connectionFactory} or {@code config} are null
   * @throws IOException if the connection could not be created
   */
  public static ConfigurableConnection create(ConnectionFactory connectionFactory, Config config)
      throws IOException, TimeoutException {
    Assert.notNull(connectionFactory, "connectionFactory");
    return create(new ConnectionOptions(connectionFactory), config);
  }

  /**
   * Creates and returns a new Lyra managed ConfigurableConnection for the given {@code options} and
   * {@code config}. If the connection attempt fails, retries will be performed according to the
   * {@link Config#getConnectRetryPolicy() configured RetryPolicy} before throwing the failure.
   * 
   * @throws NullPointerException if {@code options} or {@code config} are null
   * @throws IOException if the connection could not be created
   */
  public static ConfigurableConnection create(ConnectionOptions options, Config config)
      throws IOException, TimeoutException {
    Assert.notNull(options, "options");
    Assert.notNull(config, "config");
    ConnectionHandler handler = new ConnectionHandler(options.copy(), new Config(config));
    ConfigurableConnection proxy = (ConfigurableConnection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(), CONNECTION_TYPES, handler);
    handler.createConnection(proxy);
    return proxy;
  }
}
