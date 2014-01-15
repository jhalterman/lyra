package net.jodah.lyra.internal.util.concurrent;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import net.jodah.lyra.util.Duration;

import org.jodah.concurrentunit.Waiter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class ReentrantCircuitTest {
  ReentrantCircuit circuit;

  @BeforeMethod
  protected void beforeMethod() {
    circuit = new ReentrantCircuit();
  }

  public void shouldInitiallyBeClosed() {
    assertTrue(circuit.isClosed());
  }

  public void shouldHandleOpenCloseCycles() {
    for (int i = 0; i < 3; i++) {
      circuit.open();
      circuit.close();
    }

    assertTrue(circuit.isClosed());
  }

  public void shouldHandleRepeatedOpens() {
    for (int i = 0; i < 3; i++)
      circuit.open();

    assertFalse(circuit.isClosed());
  }

  public void shouldHandleRepeatedClosed() {
    for (int i = 0; i < 3; i++)
      circuit.close();

    assertTrue(circuit.isClosed());
  }

  public void shouldReturnWhenAwaitAndAlreadyClosed() throws Throwable {
    long t = System.currentTimeMillis();
    circuit.await();
    circuit.await(Duration.mins(3));

    // Awaits should return immediately
    assertTrue(System.currentTimeMillis() - t < 500);
  }

  public void shouldHandleSequentialWaiters() throws Throwable {
    final Waiter waiter = new Waiter();
    for (int i = 0; i < 1; i++) {
      circuit.open();

      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            System.out.println("Waiting for circuit to be closed");
            circuit.await();
            System.out.println("Circuit closed");
            waiter.resume();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }).start();

      Thread.sleep(500);
      circuit.close();
      waiter.await(500);
    }
  }

  public void shouldHandleConcurrentWaiters() throws Throwable {
    circuit.open();

    final Waiter waiter = new Waiter();
    for (int i = 0; i < 3; i++)
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            System.out.println("Waiting for circuit to be closed");
            circuit.await();
            System.out.println("Circuit closed");
            waiter.resume();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }).start();

    Thread.sleep(1000);
    circuit.close();
    waiter.await(500, 3);
  }

  public void shouldInterruptWaiters() throws Throwable {
    circuit.open();

    final Waiter waiter = new Waiter();
    for (int i = 0; i < 3; i++)
      new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            circuit.await();
          } catch (InterruptedException e) {
            waiter.resume();
          }
        }
      }).start();

    Thread.sleep(300);
    circuit.interruptWaiters();
    waiter.await(500, 3);
  }

  public void shouldNotBlockOpenWhenSyncAcquired() throws Throwable {
    circuit.open();

    final Waiter waiter = new Waiter();
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          circuit.await();
          waiter.resume();
        } catch (InterruptedException e) {
        }
      }
    }).start();

    Thread.sleep(300);
    circuit.open();
    circuit.close();
    waiter.await(500);
  }
}
