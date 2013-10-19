package net.jodah.lyra.internal;

import java.io.IOException;

import net.jodah.lyra.event.DefaultConnectionListener;

import org.jodah.concurrentunit.Waiter;

import com.rabbitmq.client.Connection;

public abstract class AbstractRecoveryTest extends AbstractFunctionalTest {
  void performRecovery(Exception e, int expectedShutdownListenerCalls) throws Throwable {
    mockConnection();

    final Waiter waiter = new Waiter();
    config.withConnectionListeners(new DefaultConnectionListener() {
      @Override
      public void onRecovery(Connection connection) {
        waiter.resume();
      }

      @Override
      public void onRecoveryFailure(Connection connection, Throwable failure) {
        waiter.resume();
      }
    });

    mockRecovery(e);
    callShutdownListener(retryableConnectionShutdownSignal());
    waiter.await(1000, expectedShutdownListenerCalls);
    Thread.sleep(100);
  }

  abstract void mockRecovery(Exception e) throws IOException;
}
