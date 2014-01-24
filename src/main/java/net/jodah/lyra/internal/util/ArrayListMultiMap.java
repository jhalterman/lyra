package net.jodah.lyra.internal.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe ArrayList MultiMap implementation.
 * 
 * @author Jonathan Halterman
 */
public class ArrayListMultiMap<K, V> {
  private final Map<K, List<V>> map = new ConcurrentHashMap<K, List<V>>();

  public void clear() {
    map.clear();
  }

  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  /**
   * Gets the values for the {@code key}. Result must be externally synchronized.
   */
  public List<V> get(K key) {
    return map.get(key);
  }

  public boolean isEmpty() {
    return map.isEmpty();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public boolean put(K key, V value) {
    List<V> set = map.get(key);
    if (set == null)
      synchronized (map) {
        set = map.get(key);
        if (set == null) {
          set = new ArrayList<V>();
          map.put(key, set);
        }
      }

    synchronized (set) {
      return set.add(value);
    }
  }

  public List<V> putAll(K key, List<V> values) {
    return map.put(key, values);
  }

  public List<V> remove(K key) {
    return map.remove(key);
  }

  public boolean remove(K key, V value) {
    List<V> set = map.get(key);
    if (set == null)
      return false;
    else
      synchronized (set) {
        boolean result = set.remove(value);
        if (set.isEmpty())
          map.remove(key);
        return result;
      }
  }

  /**
   * Returns an iterable over the multimap's values. Unsafe for concurrent modification.
   */
  public Iterable<V> values() {
    return new Iterable<V>() {
      @Override
      public Iterator<V> iterator() {
        return new Iterator<V>() {
          final Iterator<List<V>> valuesIterator = map.values().iterator();
          Iterator<V> current;

          {
            if (valuesIterator.hasNext())
              current = valuesIterator.next().iterator();
          }

          @Override
          public boolean hasNext() {
            return current != null && current.hasNext();
          }

          @Override
          public V next() {
            V value = current.next();
            while (!current.hasNext() && valuesIterator.hasNext())
              current = valuesIterator.next().iterator();
            return value;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
