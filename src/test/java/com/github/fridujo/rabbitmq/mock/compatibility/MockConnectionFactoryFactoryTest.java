package com.github.fridujo.rabbitmq.mock.compatibility;

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;

import static com.github.fridujo.rabbitmq.mock.tool.SafeArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

class MockConnectionFactoryFactoryTest {

    @Test
    void current_version_resolve_AddressResolver() {
        ConnectionFactory connectionFactory = MockConnectionFactoryFactory.build();

        assertThat(connectionFactory).isExactlyInstanceOf(MockConnectionFactory.class);
    }

    @Test
    void unresolved_AddressResolver_leads_to_alternate_connectionFactory() throws ClassNotFoundException {
        ClassLoader classLoader = spy(new URLClassLoader(new URL[0], this.getClass().getClassLoader()));
        doThrow(new ClassNotFoundException()).when(classLoader).loadClass(eq("com.rabbitmq.client.AddressResolver"));
        ConnectionFactory connectionFactory = MockConnectionFactoryFactory.build(classLoader);

        assertThat(connectionFactory).isExactlyInstanceOf(MockConnectionFactoryWithoutAddressResolver.class);
    }
}
