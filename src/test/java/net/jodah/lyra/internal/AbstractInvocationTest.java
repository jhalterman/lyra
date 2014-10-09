package net.jodah.lyra.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.IOException;

import net.jodah.concurrentunit.Waiter;

import com.rabbitmq.client.ShutdownSignalException;

public abstract class AbstractInvocationTest extends AbstractFunctionalTest {
  void performInvocation(ShutdownSignalException invocationFailure) throws Throwable {
    performInvocation(invocationFailure, null);
  }

  void performInvocation(ShutdownSignalException invocationFailure, Exception recoveryFailure)
      throws Throwable {
    mockConnection();
    mockInvocation(invocationFailure);
    if (recoveryFailure != null)
      mockRecovery(recoveryFailure);

    final Waiter waiter = new Waiter();
    runInThread(new Runnable() {
      public void run() {
        try {
          performInvocation();
          waiter.resume();
        } catch (Throwable t) {
          waiter.fail(t);
        }
      }
    });

    waiter.await(1000);
  }

  void performThrowableInvocation(ShutdownSignalException invocationFailure) throws Throwable {
    performThrowableInvocation(invocationFailure, null);
  }

  void performThrowableInvocation(ShutdownSignalException invocationFailure,
      Exception recoveryFailure) throws Throwable {
    mockConnection();
    mockInvocation(invocationFailure);
    if (recoveryFailure != null)
      mockRecovery(recoveryFailure);

    try {
      performInvocation();
      fail("Invocation should have thrown an exception");
    } catch (IOException expected) {
      assertEquals(invocationFailure, expected.getCause());
    }

    Thread.sleep(100);
  }

  protected abstract void performInvocation() throws IOException;

  protected abstract void mockInvocation(Exception e) throws IOException;

  protected abstract void mockRecovery(Exception e) throws IOException;
}
