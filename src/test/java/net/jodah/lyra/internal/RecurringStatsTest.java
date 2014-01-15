package net.jodah.lyra.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;

import org.testng.annotations.Test;

/**
 * @author Jonathan Halterman
 */
@Test
public class RecurringStatsTest {
  public void shouldAttemptForeverWithDefaultPolicy() throws Exception {
    RecurringStats stats = new RecurringStats(new RetryPolicy());
    stats.incrementAttempts();
    assertFalse(stats.isPolicyExceeded());
    Thread.sleep(50);
    stats.incrementAttempts();
    assertFalse(stats.isPolicyExceeded());
  }

  public void shouldNotAttemptWhenMaxAttemptsExceeded() throws Exception {
    RecurringStats stats = new RecurringStats(new RetryPolicy().withMaxAttempts(3));
    stats.incrementAttempts();
    stats.incrementAttempts();
    assertFalse(stats.isPolicyExceeded());
    stats.incrementAttempts();
    assertTrue(stats.isPolicyExceeded());
  }

  public void shouldNotAttemptWhenMaxDurationExceeded() throws Exception {
    RecurringStats stats = new RecurringStats(new RetryPolicy().withMaxDuration(Duration.millis(25)));
    stats.incrementTime();
    assertFalse(stats.isPolicyExceeded());
    assertFalse(stats.isPolicyExceeded());
    Thread.sleep(50);
    assertTrue(stats.isPolicyExceeded());
  }

  public void shouldAdjustWaitTimeForMaxDuration() throws Throwable {
    RecurringStats stats = new RecurringStats(new RetryPolicy().withInterval(Duration.millis(150))
        .withMaxDuration(Duration.millis(100)));
    stats.incrementTime();
    assertFalse(stats.isPolicyExceeded());
    assertEquals(stats.getWaitTime().toMillis(), 100);
    Thread.sleep(25);
    stats.incrementTime();
    assertFalse(stats.isPolicyExceeded());
    assertTrue(stats.getWaitTime().toMillis() < 100);
  }

  public void waitTimeShouldDefaultToZero() {
    RecurringStats stats = new RecurringStats(new RetryPolicy());
    assertEquals(stats.getWaitTime().toMillis(), 0);
    stats.incrementAttempts();
    assertEquals(stats.getWaitTime().toMillis(), 0);
  }

  public void waitTimeShouldBeConstant() {
    RecurringStats stats = new RecurringStats(new RetryPolicy().withInterval(Duration.millis(5)));
    assertEquals(stats.getWaitTime().toMillis(), 5);
    stats.incrementAttempts();
    assertEquals(stats.getWaitTime().toMillis(), 5);
  }

  public void waitTimeShouldIncreaseExponentially() throws Exception {
    RecurringStats stats = new RecurringStats(new RetryPolicy().withBackoff(Duration.millis(1),
        Duration.millis(5)));
    stats.incrementTime();
    assertEquals(stats.getWaitTime().toMillis(), 1);
    stats.incrementTime();
    assertEquals(stats.getWaitTime().toMillis(), 2);
    stats.incrementTime();
    assertEquals(stats.getWaitTime().toMillis(), 4);
    stats.incrementTime();
    assertEquals(stats.getWaitTime().toMillis(), 5);
    stats.incrementTime();
    assertEquals(stats.getWaitTime().toMillis(), 5);
  }
}
