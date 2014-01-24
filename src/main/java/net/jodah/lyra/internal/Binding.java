package net.jodah.lyra.internal;

import java.util.Map;

/**
 * Encapsulates a binding from a resource name to a destination resource declaration.
 * 
 * @author Jonathan Halterman
 */
class Binding {
  String destination;
  final String source;
  final String routingKey;
  final Map<String, Object> arguments;

  @SuppressWarnings("unchecked")
  Binding(Object[] args) {
    destination = (String) args[0];
    source = (String) args[1];
    routingKey = (String) args[2];
    arguments = args.length > 3 ? (Map<String, Object>) args[3] : null;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((arguments == null) ? 0 : arguments.hashCode());
    result = prime * result + ((destination == null) ? 0 : destination.hashCode());
    result = prime * result + ((routingKey == null) ? 0 : routingKey.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Binding other = (Binding) obj;
    if (arguments == null) {
      if (other.arguments != null)
        return false;
    } else if (!arguments.equals(other.arguments))
      return false;
    if (destination == null) {
      if (other.destination != null)
        return false;
    } else if (!destination.equals(other.destination))
      return false;
    if (routingKey == null) {
      if (other.routingKey != null)
        return false;
    } else if (!routingKey.equals(other.routingKey))
      return false;
    if (source == null) {
      if (other.source != null)
        return false;
    } else if (!source.equals(other.source))
      return false;
    return true;
  }
}
