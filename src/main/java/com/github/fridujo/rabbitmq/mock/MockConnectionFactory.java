package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.ExecutorService;

public class MockConnectionFactory extends ConnectionFactory {

    private final MockNode mockNode = new MockNode();

    @Override
    public boolean isAutomaticRecoveryEnabled() {
        return false;
    }

    @Override
    public MockConnection newConnection(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName) {
        return newConnection();
    }

    public MockConnection newConnection() {
        return new MockConnection(mockNode);
    }
}
