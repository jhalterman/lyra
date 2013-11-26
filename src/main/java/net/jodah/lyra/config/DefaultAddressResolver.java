package net.jodah.lyra.config;

import com.rabbitmq.client.Address;

/**
 * Address resolver to resolve the addresses in a round-robin manner
 *
 * @author Srinath C
 */
public class DefaultAddressResolver implements AddressResolver {

  private final Address[] addresses;

  public DefaultAddressResolver(Address[] addresses) {
    this.addresses = addresses;
  }

  @Override
  public Address[] resolveAddresses() {
    return addresses;
  }
}
