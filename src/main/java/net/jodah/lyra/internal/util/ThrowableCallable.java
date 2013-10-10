package net.jodah.lyra.internal.util;


public interface ThrowableCallable<V> {
  V call() throws Throwable;
}
