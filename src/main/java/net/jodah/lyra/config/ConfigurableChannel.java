package net.jodah.lyra.config;

import com.rabbitmq.client.Channel;

/**
 * Configurable Channel.
 * 
 * @author Jonathan Halterman
 */
public interface ConfigurableChannel extends ChannelConfig, Channel {
}