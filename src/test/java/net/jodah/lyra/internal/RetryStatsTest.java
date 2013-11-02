package net.jodah.lyra.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import net.jodah.lyra.internal.RetryStats;
import net.jodah.lyra.retry.RetryPolicy;
import net.jodah.lyra.util.Duration;

import org.testng.annotations.Test;

/**
 * @author Jonathan Halterman
 */
@Test
public class RetryStatsTest {
  public void shouldRetryForeverWithDefaultPolicy() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy());
    stats.incrementRetries();
    assertFalse(stats.isPolicyExceeded());
    Thread.sleep(50);
    stats.incrementRetries();
    assertFalse(stats.isPolicyExceeded());
  }

  public void shouldNotRetryWhenMaxAttemptsExceeded() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy().withMaxRetries(3));
    stats.incrementRetries();
    stats.incrementRetries();
    assertFalse(stats.isPolicyExceeded());
    stats.incrementRetries();
    assertTrue(stats.isPolicyExceeded());
  }

  public void shouldNotRetryWhenMaxDurationExceeded() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy().withMaxDuration(Duration.millis(25)));
    stats.incrementTime();
    assertFalse(stats.isPolicyExceeded());
    assertFalse(stats.isPolicyExceeded());
    Thread.sleep(50);
    assertTrue(stats.isPolicyExceeded());
  }

  public void shouldAdjustWaitTimeForMaxDuration() throws Throwable {
    RetryStats stats = new RetryStats(new RetryPolicy().withRetryInterval(Duration.millis(150))
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
    RetryStats stats = new RetryStats(new RetryPolicy());
    assertEquals(stats.getWaitTime().toMillis(), 0);
    stats.incrementRetries();
    assertEquals(stats.getWaitTime().toMillis(), 0);
  }

  public void waitTimeShouldBeConstant() {
    RetryStats stats = new RetryStats(new RetryPolicy().withRetryInterval(Duration.millis(5)));
    assertEquals(stats.getWaitTime().toMillis(), 5);
    stats.incrementRetries();
    assertEquals(stats.getWaitTime().toMillis(), 5);
  }

  public void waitTimeShouldIncreaseExponentially() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy().withBackoff(Duration.millis(1),
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
