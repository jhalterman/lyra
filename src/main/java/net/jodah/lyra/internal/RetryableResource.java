package net.jodah.lyra.internal;

import static net.jodah.lyra.internal.util.Exceptions.extractCause;
import static net.jodah.lyra.internal.util.Exceptions.isConnectionClosure;
import static net.jodah.lyra.internal.util.Exceptions.isRetryable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;

import net.jodah.lyra.internal.util.Collections;
import net.jodah.lyra.internal.util.Reflection;
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
  final Logger log = LoggerFactory.getLogger(getClass());
  final ReentrantCircuit circuit = new ReentrantCircuit();
  final InterruptableWaiter retryWaiter = new InterruptableWaiter();
  final List<ShutdownListener> shutdownListeners = Collections.synchronizedList();
  volatile boolean closed;

  enum RecoveryResult {
    Succeeded, Failed, Pending
  }

  void afterClosure() {
  }

  /**
   * Calls the {@code callable} with retries, throwing a failure if retries are exhausted.
   */
  <T> T callWithRetries(Callable<T> callable, RetryPolicy retryPolicy, boolean recovery,
      boolean logFailures) throws Exception {
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
        if (logFailures && retryPolicy != null && retryPolicy.allowsRetries())
          log.error("Invocation of {} failed.", callable, e);

        ShutdownSignalException sse = extractCause(e, ShutdownSignalException.class);
        boolean connectionClosure = sse != null && isConnectionClosure(sse);
        boolean channelClosure = sse != null && !connectionClosure;

        if (sse != null) {
          if (channelClosure)
            circuit.open();
          if ((recovery && connectionClosure) || !canRecover(connectionClosure))
            throw e;
        }

        if (!closed) {
          try {
            // Retry on channel recovery failure or retryable exception
            boolean retryable = retryPolicy != null && retryPolicy.allowsRetries()
                && ((recovery && channelClosure) || isRetryable(e, sse));
            long startTime = System.nanoTime();

            // Recover channel if needed
            boolean recoveryNeeded = !recovery && sse != null;
            RecoveryResult recoveryResult = recoveryNeeded ? connectionClosure ? RecoveryResult.Pending
                : recoverChannel(retryable || recovery)
                : null;

            if (retryable && !RecoveryResult.Failed.equals(recoveryResult)) {
              // Wait for pending recovery
              if (RecoveryResult.Pending.equals(recoveryResult)) {
                if (retryPolicy.getMaxDuration() == null)
                  circuit.await();
                else if (!circuit.await(retryStats.getMaxWaitTime())) {
                  log.debug("Exceeded max wait time while waiting for {} to recover", this);
                  throw e;
                }
              }

              // Continue retries
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

  /**
   * Recovers the channel when it's unexpectedly closed, returning {@link RecoveryResult#Pending} if
   * the recovery is to be performed without waiting, {@link RecoveryResult#Succeeded} if the
   * recovery succeeds or {@link RecoveryResult#Failed} if the recover fails.
   * 
   * @param waitForRecovery whether the invoking thread should wait for recovery or return
   *          immediately
   * @throws Execption when recovery fails due to a connection closure
   */
  RecoveryResult recoverChannel(boolean waitForRecovery) throws Exception {
    return RecoveryResult.Failed;
  }

  void interruptWaiters() {
    circuit.interruptWaiters();
    retryWaiter.interruptWaiters();
  }
}
