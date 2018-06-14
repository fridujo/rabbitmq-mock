package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.NoOpMetricsCollector;

import java.util.concurrent.ExecutorService;

public class MockConnectionFactory extends ConnectionFactory {

    private final MockNode mockNode = new MockNode();

    public MockConnectionFactory() {
        setAutomaticRecoveryEnabled(false);
        setMetricsCollector(new NoOpMetricsCollector());
    }

    @Override
    public MockConnection newConnection(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName) {
        return newConnection();
    }

    public MockConnection newConnection() {
        MockConnection mockConnection = new MockConnection(mockNode, this::getMetricsCollector);
        return mockConnection;
    }
}
