package net.jodah.lyra.util;

import static org.junit.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DurationTest {

  public void testValidDurationStrings(){
    Assert.assertEquals(Duration.of("5ns"), Duration.nanoseconds(5));
    Assert.assertEquals(Duration.of("5microsecond"), Duration.microseconds(5));
    Assert.assertEquals(Duration.of("5milliseconds"), Duration.millis(5));
    Assert.assertEquals(Duration.of("5 seconds"), Duration.seconds(5));
    Assert.assertEquals(Duration.of("5 minutes"), Duration.mins(5));
    Assert.assertEquals(Duration.of("5 hours"), Duration.hours(5));
    Assert.assertEquals(Duration.of("5 days"), Duration.days(5));
    Assert.assertEquals(Duration.of("inf"), Duration.inf());
    Assert.assertEquals(Duration.of("infinite"), Duration.inf());
    Assert.assertEquals(Duration.of("∞"), Duration.infinite());

    // Interesting value but legal nevertheless
    Assert.assertEquals(Duration.of("0s"), Duration.seconds(0));
  }

  private void testInvalidDurationString(String duration){
    try{
      Duration.of(duration);
      fail("Duration string '" + duration + "' should not parse correctly." );
    }
    catch(IllegalArgumentException iae){
      //Expected
    }
  }

  public void testInvalidDurationStrings(){
    testInvalidDurationString("foobar");
    testInvalidDurationString("ms3");
    testInvalidDurationString("34 lightyears");
    testInvalidDurationString("34 seconds a day");
    testInvalidDurationString("5 days a week");
    testInvalidDurationString("");
    testInvalidDurationString("2");
    testInvalidDurationString("ns");
  }
}
