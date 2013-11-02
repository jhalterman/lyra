package net.jodah.lyra.internal;

import java.lang.reflect.Method;

/**
 * Encapsulates the state of a method invocation.
 * 
 * @author Jonathan Halterman
 */
class Invocation {
  final Method method;
  final Object[] args;

  Invocation(Method method, Object[] args) {
    this.method = method;
    this.args = args;
  }

  Object invoke(Object object) throws Exception {
    return method.invoke(object, args);
  }
}
