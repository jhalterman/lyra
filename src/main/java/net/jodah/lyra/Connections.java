package net.jodah.lyra;

import java.io.IOException;
import java.lang.reflect.Proxy;

import net.jodah.lyra.internal.ConnectionHandler;
import net.jodah.lyra.internal.util.Assert;

import com.rabbitmq.client.Connection;

/**
 * Creates Lyra managed Connections.
 * 
 * @author Jonathan Halterman
 */
public final class Connections {
  private static final Class<?>[] CONNECTION_TYPES = { Connection.class };

  private Connections() {
  }

  /**
   * Creates and returns a new Lyra managed connection for the given {@code options}. If the
   * connection attempt fails, retries will be performed according to the
   * {@link Options#getConnectRetryPolicy() configured RetryPolicy} before throwing the failure.
   * 
   * @throws NullPointerException if {@code options} is null
   * @throws IOException if the connection could not be created
   */
  public static Connection create(Options options) throws IOException {
    Assert.notNull(options, "options");
    ConnectionHandler handler = new ConnectionHandler(options.copy());
    Connection proxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
        CONNECTION_TYPES, handler);
    handler.setProxy(proxy);
    return proxy;
  }
}
