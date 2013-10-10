package net.jodah.lyra.internal.util.concurrent;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import net.jodah.lyra.util.Duration;

/**
 * A waiter where waiting threads can be interrupted (as opposed to awakened).
 * 
 * @author Jonathan Halterman
 */
public class InterruptableWaiter {
  private final Sync sync = new Sync();

  private static final class Sync extends AbstractQueuedSynchronizer {
    private static final long serialVersionUID = 4016766900138538852L;

    @Override
    protected int tryAcquireShared(int acquires) {
      // Disallow acquisition
      return -1;
    }
  }

  /**
   * Waits forever, aborting if interrupted.
   */
  public void await() throws InterruptedException {
    sync.acquireSharedInterruptibly(0);
  }

  /**
   * Waits for the {@code waitDuration}, aborting if interrupted.
   */
  public void await(Duration waitDuration) throws InterruptedException {
    sync.tryAcquireSharedNanos(0, waitDuration.toNanos());
  }

  /**
   * Interrupts waiting threads.
   */
  public void interruptWaiters() {
    for (Thread t : sync.getSharedQueuedThreads())
      t.interrupt();
  }
}
