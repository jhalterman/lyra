package net.jodah.lyra.internal.util;

import com.rabbitmq.client.Address;

public final class Addresses {
  private Addresses() {
  }

  /**
   * Returns an array of Addresses for the {@code hosts} and {@code port}.
   */
  public static Address[] addressesFor(String[] hosts, int port) {
    Address[] hostAddresses = new Address[hosts.length];
    for (int i = 0; i < hosts.length; i++)
      hostAddresses[i] = new Address(hosts[i].trim(), port);
    return hostAddresses;
  }
}
