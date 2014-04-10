package net.jodah.lyra.internal.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Collections {
  public static <K, V> ArrayListMultiMap<K, V> arrayListMultiMap() {
    return new ArrayListMultiMap<K, V>();
  }

  public static <T> List<T> synchronizedList() {
    return java.util.Collections.<T>synchronizedList(new ArrayList<T>());
  }

  public static <K, V> Map<K, V> synchronizedLinkedMap() {
    return java.util.Collections.<K, V>synchronizedMap(new LinkedHashMap<K, V>());
  }
}
