package net.jodah.lyra.internal;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;
import net.jodah.lyra.config.ConfigurableChannel;
import net.jodah.lyra.convention.RecoveryPolicies;

import org.testng.annotations.Test;

import com.rabbitmq.client.Consumer;

@Test
public class ChannelConfigTest extends AbstractFunctionalTest {
  /**
   * Asserts that channel specific configuration overrides global config.
   */
  public void shouldOverrideGlobalConfig() throws Throwable {
    mockConnection();
    MockChannel mc = mockChannel(1);
    ((ConfigurableChannel) mc.proxy).withChannelRecoveryPolicy(RecoveryPolicies.recoverNever())
        .withChannelRecoveryPolicy(RecoveryPolicies.recoverNever());

    when(mc.delegate.basicConsume(anyString(), any(Consumer.class))).thenThrow(
        retryableChannelShutdownSignal());

    try {
      mc.proxy.basicConsume("foo", null);
      fail();
    } catch (Exception e) {
      verify(mc.delegate).basicConsume(anyString(), any(Consumer.class));
    }
  }
}
