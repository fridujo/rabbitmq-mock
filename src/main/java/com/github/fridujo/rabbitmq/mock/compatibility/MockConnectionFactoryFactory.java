package com.github.fridujo.rabbitmq.mock.compatibility;

import static com.github.fridujo.rabbitmq.mock.tool.Classes.missingClass;

import com.github.fridujo.rabbitmq.mock.ConfigurableConnectionFactory;
import com.github.fridujo.rabbitmq.mock.MockConnectionFactory;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Factory building a mock implementation of {@link ConnectionFactory} according to the
 * version of **amqp-client** present in the classpath.
 */
public class MockConnectionFactoryFactory {

    public static ConfigurableConnectionFactory<?> build() {
        return build(MockConnectionFactoryFactory.class.getClassLoader());
    }

    public static ConfigurableConnectionFactory<?> build(ClassLoader classLoader) {
        if (missingClass(classLoader, "com.rabbitmq.client.AddressResolver")) {
            // AddressResolver appears in version 3.6.6 of amqp-client
            // This execution branch is tested in spring-boot integration test with version 1.4.0.RELEASE
            return new MockConnectionFactoryWithoutAddressResolver();
        } else {
            return new MockConnectionFactory();
        }
    }
}
