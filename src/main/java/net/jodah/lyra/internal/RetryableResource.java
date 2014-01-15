package net.jodah.lyra.internal;

import static net.jodah.lyra.internal.util.Exceptions.extractCause;
import static net.jodah.lyra.internal.util.Exceptions.isRetryable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;

import net.jodah.lyra.internal.util.Collections;
import net.jodah.lyra.internal.util.Reflection;
import net.jodah.lyra.internal.util.concurrent.InterruptableWaiter;
import net.jodah.lyra.internal.util.concurrent.ReentrantCircuit;
import net.jodah.lyra.util.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * A resource which supports invocation retries and failure recovery.
 * 
 * @author Jonathan Halterman
 */
abstract class RetryableResource {
  final Logger log = LoggerFactory.getLogger(getClass());
  final ReentrantCircuit circuit = new ReentrantCircuit();
  final InterruptableWaiter retryWaiter = new InterruptableWaiter();
  final List<ShutdownListener> shutdownListeners = Collections.synchronizedList();
  volatile boolean closed;

  void afterClosure() {
  }

  /**
   * Calls the {@code callable} with retries, throwing a failure if retries are exhausted.
   */
  <T> T callWithRetries(Callable<T> callable, RecurringPolicy<?> recurringPolicy,
      RecurringStats retryStats, boolean recoverable, boolean logFailures) throws Exception {
    boolean recovery = retryStats != null;

    while (true) {
      try {
        return callable.call();
      } catch (Exception e) {
        ShutdownSignalException sse = extractCause(e, ShutdownSignalException.class);
        if (sse == null && logFailures && recurringPolicy != null
            && recurringPolicy.allowsAttempts())
          log.error("Invocation of {} failed.", callable, e);

        if (sse != null && (recovery || !recoverable))
          throw e;

        if (!closed) {
          try {
            // Retry on channel recovery failure or retryable exception
            boolean retryable = recurringPolicy != null && recurringPolicy.allowsAttempts()
                && isRetryable(e, sse);
            long startTime = System.nanoTime();

            if (retryable) {
              // Wait for pending recovery
              if (sse != null) {
                if (recurringPolicy.getMaxDuration() == null)
                  circuit.await();
                else if (!circuit.await(retryStats.getMaxWaitTime())) {
                  log.debug("Exceeded max wait time while waiting for {} to recover", this);
                  throw e;
                }
              }

              // Continue retries
              if (retryStats == null)
                retryStats = new RecurringStats(recurringPolicy);
              retryStats.incrementAttempts();
              if (!retryStats.isPolicyExceeded()) {
                long remainingWaitTime = retryStats.getWaitTime().toNanos()
                    - (System.nanoTime() - startTime);
                if (remainingWaitTime > 0)
                  retryWaiter.await(Duration.nanos(remainingWaitTime));
                continue;
              }
            }
          } catch (Throwable ignore) {
          }
        }

        throw e;
      }
    }
  }

  /**
   * Handles common method invocations.
   */
  boolean handleCommonMethods(Object delegate, Method method, Object[] args) throws Throwable {
    if ("abort".equals(method.getName()) || "close".equals(method.getName())) {
      try {
        Reflection.invoke(delegate, method, args);
        return true;
      } finally {
        closed = true;
        afterClosure();
        interruptWaiters();
      }
    } else if ("addShutdownListener".equals(method.getName()) && args[0] != null)
      shutdownListeners.add((ShutdownListener) args[0]);
    else if ("removeShutdownListener".equals(method.getName()) && args[0] != null)
      shutdownListeners.remove((ShutdownListener) args[0]);
    return false;
  }

  void interruptWaiters() {
    circuit.interruptWaiters();
    retryWaiter.interruptWaiters();
  }
}
