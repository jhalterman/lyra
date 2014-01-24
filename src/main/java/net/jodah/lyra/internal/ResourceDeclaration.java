package net.jodah.lyra.internal;

import java.lang.reflect.Method;

import net.jodah.lyra.internal.util.Reflection;

/**
 * Encapsulates a named resource declaration.
 * 
 * @author Jonathan Halterman
 */
class ResourceDeclaration {
  final Method method;
  final Object[] args;

  ResourceDeclaration(Method method, Object[] args) {
    this.method = method;
    this.args = args;
  }

  <T> T invoke(Object subject) throws Exception {
    return Reflection.invoke(subject, method, args);
  }
}
