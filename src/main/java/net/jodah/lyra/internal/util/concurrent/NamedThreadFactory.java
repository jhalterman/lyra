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

  /**
   * Creates a thread factory that names threads according to the {@code nameFormat} by suppling a
   * single argument to the format representing the thread number.
   */
  public NamedThreadFactory(String nameFormat) {
    this.nameFormat = nameFormat;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread t = new Thread(r, String.format(nameFormat, threadNumber.getAndIncrement()));
    t.setDaemon(true);
    return t;
  }
}