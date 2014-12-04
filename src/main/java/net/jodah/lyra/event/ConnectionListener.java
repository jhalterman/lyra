package net.jodah.lyra.event;

import com.rabbitmq.client.Connection;

/**
 * Listens for {@link Connection} related events.
 * 
 * @author Jonathan Halterman
 */
public interface ConnectionListener {
  /**
   * Called when the {@code connection} is successfully created.
   */
  void onCreate(Connection connection);

  /**
   * Called when connection creation fails.
   */
  void onCreateFailure(Throwable failure);

  /**
   * Called when recovery of the {@code connection} is started.
   */
  void onRecoveryStarted(Connection connection);

  /**
   * Called when the {@code connection} is successfully recovered from an unexpected closure, but
   * before its associated exchanges, queues, bindings, channels and consumers are recovered.
   */
  void onRecovery(Connection connection);

  /**
   * Called when recovery of the {@code connection} and its associated resources is completed. Note:
   * The success of failure of an individual channel's recovery can be tracked with a
   * {@link ChannelListener}.
   */
  void onRecoveryCompleted(Connection connection);

  /**
   * Called when the {@code connection} fails to recover from an unexpected closure.
   */
  void onRecoveryFailure(Connection connection, Throwable failure);
}
