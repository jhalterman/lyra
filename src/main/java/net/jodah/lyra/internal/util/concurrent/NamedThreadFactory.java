package net.jodah.lyra.internal.util.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory that names threads with increasing numbers.
 * 
 * @author Jonathan Halterman
 */
public class NamedThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String nameFormat;
  private final boolean daemon;

  /**
   * Creates a thread factory that names threads according to the {@code nameFormat} by suppling a
   * single argument to the format representing the thread number.
   */
  public NamedThreadFactory(String nameFormat, boolean daemon) {
    this.nameFormat = nameFormat;
    this.daemon = daemon;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r, String.format(nameFormat, threadNumber.getAndIncrement()));
    thread.setDaemon(daemon);
    return thread;
  }
}