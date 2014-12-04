package net.jodah.lyra.internal;

import java.io.IOException;

import net.jodah.concurrentunit.Waiter;
import net.jodah.lyra.event.DefaultChannelListener;
import net.jodah.lyra.event.DefaultConnectionListener;

import org.testng.annotations.BeforeMethod;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public abstract class AbstractRecoveryTest extends AbstractFunctionalTest {
  @Override
  @BeforeMethod
  protected void beforeMethod() throws Exception {
    super.beforeMethod();
    mockConnection();
  }

  /**
   * Performs recovery, calling the {@code initialResource}'s ShutdownListener with an and mocking
   * recovery for some {@code recoveryResource}'s ShutdownListener.
   */
  void performRecovery(RetryableResource initialResource, RetryableResource recoveryResource,
      int expectedConnectionRecoveryAttempts, int expectedChannelRecoveryAttempts) throws Throwable {
    createResources();
    int expectedResumes = expectedConnectionRecoveryAttempts + expectedChannelRecoveryAttempts;

    final Waiter waiter = new Waiter();
    waiter.expectResumes(expectedResumes);
    config.withConnectionListeners(new DefaultConnectionListener() {
      @Override
      public void onRecoveryCompleted(Connection connection) {
        waiter.resume();
      }

      @Override
      public void onRecoveryFailure(Connection connection, Throwable failure) {
        waiter.resume();
      }
    });

    config.withChannelListeners(new DefaultChannelListener() {
      @Override
      public void onRecovery(Channel channel) {
        waiter.resume();
      }

      @Override
      public void onRecoveryFailure(Channel channel, Throwable failure) {
        waiter.resume();
      }
    });

    // Mock recovery handling
    mockRecovery(recoveryResource instanceof ConnectionHandler ? connectionShutdownSignal()
        : channelShutdownSignal(), recoveryResource);

    // Call initial shutdown listener
    callShutdownListener(initialResource,
        initialResource instanceof ConnectionHandler ? connectionShutdownSignal()
            : channelShutdownSignal());
    if (expectedResumes > 0)
      waiter.await(1000);
    Thread.sleep(100);
  }

  /** Mock recovery for the resource, with recovery attempts failing because of {@code e}. */
  abstract void mockRecovery(Exception e, RetryableResource retryableResource) throws IOException;

  /** Create the resources to recover. */
  void createResources() throws IOException {
  }
}
