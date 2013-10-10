package net.jodah.lyra;

import java.io.IOException;
import java.lang.reflect.Proxy;

import net.jodah.lyra.internal.ConnectionHandler;
import net.jodah.lyra.internal.util.Assert;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public final class Connections {
  private static final Class<?>[] CONNECTION_TYPES = { Connection.class };

  private Connections() {
  }

  /**
   * Creates and returns a new connection for the given {@code options}. If the connection attempt
   * fails, retries will be automatically performed according to the
   * {@link LyraOptions#getConnectRetryPolicy() configured RetryPolicy} before throwing the failure.
   * 
   * @throws NullPointerException if {@code options} is null
   * @throws IOException if the connection could not be created
   */
  public static Connection create(LyraOptions options) throws IOException {
    Assert.notNull(options, "options");
    ConnectionHandler handler = new ConnectionHandler(connectionFactoryFor(options), options);
    Connection proxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        CONNECTION_TYPES, handler);
    handler.setProxy(proxy);
    return proxy;
  }

  private static ConnectionFactory connectionFactoryFor(LyraOptions options) {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUsername(options.getUsername());
    connectionFactory.setPassword(options.getPassword());
    connectionFactory.setRequestedHeartbeat(options.getRequestedHeartbeat());
    connectionFactory.setVirtualHost(options.getVirtualHost());
    connectionFactory.setConnectionTimeout(options.getConnectionTimeout());
    return connectionFactory;
  }
}
