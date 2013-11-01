package net.jodah.lyra.config;

import static org.testng.Assert.assertTrue;
import net.jodah.lyra.config.RetryPolicy;

import org.testng.annotations.Test;

@Test
public class RetryPolicyTest {
  public void emptyPolicyShouldAllowRetries() {
    assertTrue(new RetryPolicy().allowsAttempts());
  }
}
