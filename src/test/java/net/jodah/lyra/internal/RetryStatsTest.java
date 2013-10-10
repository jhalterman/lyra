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
    assertTrue(stats.canRetryForUpdatedStats());
    Thread.sleep(50);
    assertTrue(stats.canRetryForUpdatedStats());
  }

  public void shouldNotRetryWhenMaxAttemptsExceeded() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy().withMaxRetries(3));
    assertTrue(stats.canRetryForUpdatedStats());
    assertTrue(stats.canRetryForUpdatedStats());
    assertTrue(stats.canRetryForUpdatedStats());
    assertFalse(stats.canRetryForUpdatedStats());
  }

  public void shouldNotRetryWhenMaxDurationExceeded() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy().withMaxDuration(Duration.millis(25)));
    assertTrue(stats.canRetryForUpdatedStats());
    assertTrue(stats.canRetryForUpdatedStats());
    Thread.sleep(50);
    assertFalse(stats.canRetryForUpdatedStats());
  }

  public void shouldAdjustWaitTimeForMaxDuration() throws Throwable {
    RetryStats stats = new RetryStats(new RetryPolicy().withRetryInterval(Duration.millis(150))
        .withMaxDuration(Duration.millis(100)));
    assertTrue(stats.canRetryForUpdatedStats());
    assertEquals(stats.getWaitTime().toMillis(), 100);
    Thread.sleep(25);
    assertTrue(stats.canRetryForUpdatedStats());
    assertTrue(stats.getWaitTime().toMillis() < 100);
  }

  public void waitTimeShouldDefaultToZero() {
    RetryStats stats = new RetryStats(new RetryPolicy());
    assertEquals(stats.getWaitTime().toMillis(), 0);
    stats.canRetryForUpdatedStats();
    assertEquals(stats.getWaitTime().toMillis(), 0);
  }

  public void waitTimeShouldBeConstant() {
    RetryStats stats = new RetryStats(new RetryPolicy().withRetryInterval(Duration.millis(5)));
    assertEquals(stats.getWaitTime().toMillis(), 5);
    stats.canRetryForUpdatedStats();
    assertEquals(stats.getWaitTime().toMillis(), 5);
  }

  public void waitTimeShouldIncreaseExponentially() throws Exception {
    RetryStats stats = new RetryStats(new RetryPolicy().withBackoff(Duration.millis(1),
        Duration.millis(5)));
    assertTrue(stats.canRetryForUpdatedStats());
    assertEquals(stats.getWaitTime().toMillis(), 1);
    assertTrue(stats.canRetryForUpdatedStats());
    assertEquals(stats.getWaitTime().toMillis(), 2);
    assertTrue(stats.canRetryForUpdatedStats());
    assertEquals(stats.getWaitTime().toMillis(), 4);
    assertTrue(stats.canRetryForUpdatedStats());
    assertEquals(stats.getWaitTime().toMillis(), 5);
    assertTrue(stats.canRetryForUpdatedStats());
    assertEquals(stats.getWaitTime().toMillis(), 5);
  }
}
