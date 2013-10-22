package net.jodah.lyra.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
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

  /**
   * Returns a simplified String representation of the {@code member}.
   */
  public static String toString(Member member) {
    if (member instanceof Method)
      return member.getDeclaringClass().getSimpleName() + "." + member.getName() + "()";
    return null;
  }
}
