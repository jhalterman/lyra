package net.jodah.lyra.internal;

import static org.mockito.Mockito.doThrow;

import java.io.IOException;

import net.jodah.concurrentunit.Waiter;

import org.testng.annotations.Test;

import com.rabbitmq.client.ShutdownSignalException;

/**
 * Tests that connection closures behave correctly.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ConnectionClosureTest extends AbstractFunctionalTest {
  /**
   * Asserts that a retryable invocation blocked on connection closure is interrupted by a
   * connection closure.
   */
  public void closeShouldInterruptWaitersOnRetryableClosure() throws Throwable {
    Waiter waiter = new Waiter();
    performInvocation(retryableConnectionShutdownSignal(), waiter);
  }

  /**
   * Asserts that a non-retryable invocation blocked on connection closure throws immediately.
   */
  public void closeShouldInterruptWaitersOnNonRetryableClosure() throws Throwable {
    Waiter waiter = new Waiter();
    performInvocation(nonRetryableConnectionShutdownSignal(), waiter);
  }

  private void performInvocation(final ShutdownSignalException e, final Waiter waiter)
      throws Throwable {
    mockConnection();
    mockInvocation(e);
    closeConnectionAfterDelay();

    waiter.expectResume();
    runInThread(new Runnable() {
      public void run() {
        try {
          connectionProxy.createChannel();
          waiter.fail("Invocation should have thrown an exception");
        } catch (Exception actual) {
          if (!actual.equals(e))
            actual.printStackTrace();
          waiter.assertEquals(actual, e);
          waiter.resume();
        }
      }
    });

    waiter.await(10000);
  }

  private void mockInvocation(Exception e) throws IOException {
    doThrow(e).when(connection).createChannel();
  }

  private void closeConnectionAfterDelay() throws Throwable {
    runInThread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(200);
          ConnectionClosureTest.this.connectionProxy.close();
        } catch (Throwable e) {
        }
      }
    });
  }
}
