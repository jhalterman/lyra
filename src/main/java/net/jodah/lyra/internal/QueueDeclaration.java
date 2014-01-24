package net.jodah.lyra.internal;

import java.lang.reflect.Method;

/**
 * Encapsulates a queue declaration.
 * 
 * @author Jonathan Halterman
 */
class QueueDeclaration extends ResourceDeclaration {
  String name;

  QueueDeclaration(String name, Method method, Object[] args) {
    super(method, args);
    this.name = name;
  }

  @Override
  public String toString() {
    return "QueueDeclaration [name=" + name + "]";
  }
}
