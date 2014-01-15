package net.jodah.lyra.internal.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Collections {
  public static <T> List<T> synchronizedList() {
    return java.util.Collections.<T>synchronizedList(new ArrayList<T>());
  }

  public static <K, V> Map<K, V> synchronizedMap() {
    return java.util.Collections.<K, V>synchronizedMap(new HashMap<K, V>());
  }
}
