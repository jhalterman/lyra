package net.jodah.lyra.internal;

import net.jodah.lyra.util.Duration;
import org.testng.annotations.Test;

@Test
public class RecurringPolicyTest  extends AbstractFunctionalTest {

    public void shouldAllowDifferentUnitsForIntervalDurations() throws Throwable {
        RecurringPolicy recurringPolicy = new RecurringPolicy(){};
        recurringPolicy.withBackoff(Duration.millis(100), Duration.days(1));
    }
}
