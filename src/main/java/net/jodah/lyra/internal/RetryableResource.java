package net.jodah.lyra.internal;

import java.lang.reflect.Method;
import java.util.List;

import net.jodah.lyra.internal.util.Collections;
import net.jodah.lyra.internal.util.Exceptions;
import net.jodah.lyra.internal.util.Reflection;
import net.jodah.lyra.internal.util.ThrowableCallable;
import net.jodah.lyra.internal.util.concurrent.InterruptableWaiter;
import net.jodah.lyra.internal.util.concurrent.ReentrantCircuit;
import net.jodah.lyra.retry.RetryPolicy;
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
  private static final String ADD_SHUTDOWN_LISTENER_METHOD_NAME = "addShutdownListener";
  private static final String REMOVE_SHUTDOWN_LISTENER_METHOD_NAME = "removeShutdownListener";
  static final String ABORT_METHOD_NAME = "abort";
  static final String CLOSE_METHOD_NAME = "close";

  final Logger log = LoggerFactory.getLogger(getClass());
  final ReentrantCircuit circuit = new ReentrantCircuit();
  final InterruptableWaiter retryWaiter = new InterruptableWaiter();
  final List<ShutdownListener> shutdownListeners = Collections.synchronizedList();
  private volatile boolean closed;

  void afterClosure() {
  }

  /**
   * Calls the {@code callable} with retries, throwing a failure if retries are exhausted.
   */
  <T> T callWithRetries(ThrowableCallable<T> callable, RetryPolicy retryPolicy, boolean recovery)
      throws Throwable {
    RetryStats retryStats = null;
    if (recovery) {
      if (retryPolicy == null || !retryPolicy.allowsRetries())
        return null;
      retryStats = new RetryStats(retryPolicy);
      retryStats.canRetryForUpdatedStats();
    }

    while (true) {
      try {
        return callable.call();
      } catch (Exception e) {
        ShutdownSignalException sse = Exceptions.extractCause(e, ShutdownSignalException.class);
        if (sse != null) {
          circuit.open();
          if (!canRecover(sse.isHardError()) || (recovery && sse.isHardError()))
            throw e;
        }

        if (!closed) {
          try {
            long startTime = System.nanoTime();

            // Recover resource
            if (!recovery && sse != null) {
              if (!sse.isHardError()) {
                if (!recoverChannel())
                  throw e;
              } else if (retryPolicy.getMaxDuration() == null)
                circuit.await();
              else if (!circuit.await(retryStats.getMaxWaitTime())) {
                log.debug("Exceeded max wait time while waiting for {} to recover", toString());
                throw e;
              }
            }

            // Continue retries
            if (retryPolicy != null && retryPolicy.allowsRetries()
                && Exceptions.isFailureRetryable(e, sse)) {
              if (retryStats == null)
                retryStats = new RetryStats(retryPolicy);
              if (retryStats.canRetryForUpdatedStats()) {
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
   * Returns whether the resource can be recovered.
   */
  abstract boolean canRecover(boolean connectionClosed);

  /**
   * Handles common method invocations.
   */
  boolean handleCommonMethods(Object delegate, Method method, Object[] args) throws Throwable {
    if (method.getName().equals(ABORT_METHOD_NAME) || method.getName().equals(CLOSE_METHOD_NAME)) {
      try {
        Reflection.invoke(delegate, method, args);
        return true;
      } finally {
        closed = true;
        afterClosure();
        circuit.interruptWaiters();
        retryWaiter.interruptWaiters();
      }
    } else if (method.getName().equals(ADD_SHUTDOWN_LISTENER_METHOD_NAME) && args[0] != null)
      shutdownListeners.add((ShutdownListener) args[0]);
    else if (method.getName().equals(REMOVE_SHUTDOWN_LISTENER_METHOD_NAME) && args[0] != null)
      shutdownListeners.remove((ShutdownListener) args[0]);
    return false;
  }

  /**
   * Recovers the channel when it's unexpectedly closed, returning true if it could be recovered
   * else false.
   */
  boolean recoverChannel() throws Throwable {
    return false;
  }
}
