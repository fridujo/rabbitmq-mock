package com.github.fridujo.rabbitmq.mock.compatibility;

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory;
import com.rabbitmq.client.ConnectionFactory;

import static com.github.fridujo.rabbitmq.mock.tool.Classes.missingClass;

public class MockConnectionFactoryFactory {

    public static ConnectionFactory build() {
        return build(MockConnectionFactoryFactory.class.getClassLoader());
    }

    public static ConnectionFactory build(ClassLoader classLoader) {
        if (missingClass(classLoader, "com.rabbitmq.client.AddressResolver")) {
            // AddressResolver appears in version 3.6.6 of amqp-client
            // This execution branch is tested in spring-boot integration test with version 1.4.0.RELEASE
            return new MockConnectionFactoryWithoutAddressResolver();
        } else {
            return new MockConnectionFactory();
        }
    }


}
