package net.jodah.lyra.event;

import com.rabbitmq.client.Connection;

/**
 * No-op connection listener for sub-classing.
 * 
 * @author Jonathan Halterman
 */
public abstract class DefaultConnectionListener implements ConnectionListener {
  @Override
  public void onCreate(Connection connection) {
  }

  @Override
  public void onCreateFailure(Throwable failure) {
  }

  @Override
  public void onRecovery(Connection connection) {
  }

  @Override
  public void onRecoveryCompleted(Connection connection) {
  }

  @Override
  public void onRecoveryFailure(Connection connection, Throwable failure) {
  }

  @Override
  public void onRecoveryStarted(Connection connection) {
  }
}
