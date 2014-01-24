package net.jodah.lyra.internal.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class ArrayListMultiMapTest {
  ArrayListMultiMap<String, Integer> mm;

  @BeforeMethod
  protected void createFixtures() {
    mm = new ArrayListMultiMap<String, Integer>();
    mm.put("a", 1);
    mm.put("a", 2);
    mm.put("a", 3);
    mm.put("b", 4);
    mm.put("b", 5);
    mm.put("b", 6);
    mm.put("c", 7);
    mm.put("c", 8);
    mm.put("c", 9);
  }

  public void testValuesIterable() {
    List<Integer> r = new ArrayList<Integer>();
    for (Integer i : mm.values())
      r.add(i);
    assertEquals(r, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  public void testKeySet() {
    assertEquals(mm.keySet(), Arrays.asList("a", "b", "c"));
  }

  public void testRemove() {
    assertTrue(mm.remove("a", 1));
    assertTrue(mm.remove("a", 2));
    assertFalse(mm.remove("a", 555));
    assertEquals(mm.keySet(), Arrays.asList("a", "b", "c"));
    assertTrue(mm.remove("a", 3));
    assertEquals(mm.keySet(), Arrays.asList("b", "c"));
  }
}
