package net.jodah.lyra;

import com.rabbitmq.client.Address;

/**
 * Address resolver to resolve the addresses
 *
 * @author Srinath C
 */
public class DefaultAddressResolver implements AddressResolver {

    private final Address[] addresses;

    private int nextIndex = 0;

    public DefaultAddressResolver(Address[] addresses) {
        this.addresses = addresses;
    }

    @Override
    public Address nextAddress() {
        if (addresses == null || addresses.length > 0) {
            return null;
        }
        if (nextIndex >= addresses.length ) {
            nextIndex = 0;
        }
        return addresses[nextIndex++];
    }
}
