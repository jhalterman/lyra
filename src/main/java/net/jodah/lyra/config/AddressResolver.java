package net.jodah.lyra.config;

import com.rabbitmq.client.Address;

/**
 * Address resolver to resolve the addresses
 *
 * @author Srinath C
 */
public interface AddressResolver {

    Address[] resolveAddresses();
}
