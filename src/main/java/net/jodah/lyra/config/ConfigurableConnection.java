package net.jodah.lyra.config;

import com.rabbitmq.client.Connection;

/**
 * Configurable Connection.
 * 
 * @author Jonathan Halterman
 */
public interface ConfigurableConnection extends ConnectionConfig, Connection {
}
