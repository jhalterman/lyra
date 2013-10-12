package net.jodah.lyra.internal;

import static org.mockito.Mockito.doThrow;

import java.io.IOException;

import org.jodah.concurrentunit.Waiter;
import org.testng.annotations.Test;

import com.rabbitmq.client.ShutdownSignalException;

/**
 * Tests that channel closures behave correctly.
 * 
 * @author Jonathan Halterman
 */
@Test(groups = "functional")
public class ChannelClosureTest extends AbstractFunctionalTest {
  /**
   * Asserts that a retryable invocation blocked on channel closure is interrupted by a channel
   * closure.
   */
  public void closeShouldInterruptInvocationsOnRetryableChannelClosure() throws Throwable {
    Waiter waiter = new Waiter();
    performInvocation(retryableChannelShutdownSignal(), waiter);
  }

  /**
   * Asserts that a non-retryable invocation failure blocked on channel closure throws immediately.
   */
  public void closeShouldInterruptWaitersOnNonRetryableChannelClosure() throws Throwable {
    Waiter waiter = new Waiter();
    performInvocation(nonRetryableChannelShutdownSignal(), waiter);
  }

  /**
   * Asserts that a retryable invocation blocked on connection closure is interrupted by a channel
   * closure.
   */
  public void closeShouldInterruptWaitersOnRetryableConnectionClosure() throws Throwable {
    Waiter waiter = new Waiter();
    performInvocation(retryableConnectionShutdownSignal(), waiter);
  }

  /**
   * Asserts that a non-retryable invocation blocked on connection closure throws immediately.
   */
  public void closeShouldInterruptWaitersOnNonRetryableConnectionClosure() throws Throwable {
    Waiter waiter = new Waiter();
    performInvocation(nonRetryableConnectionShutdownSignal(), waiter);
  }

  private void performInvocation(final ShutdownSignalException e, final Waiter waiter)
      throws Throwable {
    mockConnection();
    mockInvocation(e);
    closeChannelAfterDelay();

    runInThread(new Runnable() {
      public void run() {
        try {
          mockChannel(1).proxy.basicCancel("foo-tag");
          waiter.fail("Invocation should have thrown an exception");
        } catch (Exception expected) {
          waiter.assertEquals(e, expected);
          waiter.resume();
        }
      }
    });

    waiter.await(100000);
  }

  private void mockInvocation(Exception e) throws IOException {
    mockChannel(1);
    doThrow(e).when(mockChannel(1).delegate).basicCancel("foo-tag");
  }

  private void closeChannelAfterDelay() throws Throwable {
    runInThread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(200);
          mockChannel(1).proxy.close();
        } catch (Throwable e) {
        }
      }
    });
  }
}
