package net.jodah.lyra.internal;

import java.lang.reflect.Method;

import net.jodah.lyra.internal.util.Reflection;

/**
 * Represents a consumer declaration.
 * 
 * @author Jonathan Halterman
 */
class ConsumerDeclaration extends ResourceDeclaration {
  final QueueDeclaration queueDeclaration;

  ConsumerDeclaration(QueueDeclaration queueDeclaration, Method method, Object[] args) {
    super(method, args);
    this.queueDeclaration = queueDeclaration;
  }

  <T> T invoke(Object subject) throws Exception {
    if (queueDeclaration != null)
      args[0] = queueDeclaration.name;
    return Reflection.invoke(subject, method, args);
  }

  @Override
  public String toString() {
    return "ConsumerDeclaration of [queue=" + queueDeclaration == null ? (String) args[0]
        : queueDeclaration.name + "]";
  }
}
