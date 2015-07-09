package net.jodah.lyra.internal.util.concurrent;

import net.jodah.concurrentunit.Waiter;
import net.jodah.lyra.util.Duration;

import org.testng.annotations.Test;

@Test
public class InterruptableWaiterTest {
  public void shouldInteruptForeverWaiters() throws Throwable {
    final InterruptableWaiter iw = new InterruptableWaiter();
    final Waiter waiter = new Waiter();

    for (int i = 0; i < 3; i++)
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            iw.await();
          } catch (InterruptedException expected) {
            waiter.resume();
          }
        }
      }).start();

    Thread.sleep(100);
    iw.interruptWaiters();
    waiter.await(500);
  }

  public void shouldInterruptTimedWaiters() throws Throwable {
    final InterruptableWaiter iw = new InterruptableWaiter();
    final Waiter waiter = new Waiter();
    
    for (int i = 0; i < 3; i++)
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            iw.await(Duration.mins(1));
          } catch (InterruptedException expected) {
            waiter.resume();
          }
        }
      }).start();

    Thread.sleep(100);
    iw.interruptWaiters();
    waiter.await(500);
  }

  public void timedWaiterShouldTimeoutQuietly() throws Throwable {
    final InterruptableWaiter iw = new InterruptableWaiter();
    iw.await(Duration.millis(100));
  }
}
