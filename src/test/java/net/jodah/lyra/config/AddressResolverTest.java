package net.jodah.lyra.config;

import com.rabbitmq.client.Address;
import net.jodah.lyra.ConnectionOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class AddressResolverTest {

  @Test
  public void resolveAddresses() {
    ConnectionOptions options = new ConnectionOptions();
    Address[] testAddresses = Address.parseAddresses("localhost:5672,localhost:5673,localhost:5674");
    options = options.withAddresses(testAddresses);
    assertEquals(options.getAddresses(), testAddresses, "Addresses retrieved did not match expected address");
  }

  @Test
  public void customResolver() {
    ConnectionOptions options = new ConnectionOptions();
    final Address[] addresses1 = Address.parseAddresses("host1:5672,host2:5672,host3:5672");
    final Address[] addresses2 = Address.parseAddresses("host4:5672,host5:5672,host6:5672");
    options.withAddressResolver(new AddressResolver() {
        int counter = -1;

        @Override
        public Address[] resolveAddresses() {
            counter++;
            return (counter % 2 == 0) ? addresses1 : addresses2;
        }
    });
    assertEquals(options.getAddresses(), addresses1, "Addresses retrieved did not match expected address");
    assertEquals(options.getAddresses(), addresses2, "Addresses retrieved did not match expected address");
  }
}
