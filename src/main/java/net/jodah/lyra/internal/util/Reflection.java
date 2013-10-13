package net.jodah.lyra.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Reflection {
  @SuppressWarnings("unchecked")
  public static <T> T invoke(Object object, Method method, Object[] args) throws Exception {
    try {
      return (T) method.invoke(object, args);
    } catch (InvocationTargetException ite) {
      throw (Exception) ite.getTargetException();
    }
  }
}
