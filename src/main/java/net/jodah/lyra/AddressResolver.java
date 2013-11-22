package net.jodah.lyra;

import com.rabbitmq.client.Address;

/**
 * Address resolver to resolve the addresses
 *
 * @author Srinath C
 */
public interface AddressResolver {

    Address nextAddress();
}
