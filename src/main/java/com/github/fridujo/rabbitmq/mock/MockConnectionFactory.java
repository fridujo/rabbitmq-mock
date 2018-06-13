package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.NoOpMetricsCollector;

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
        MetricsCollector metricsCollector = getMetricsCollector();
        if (metricsCollector == null) {
            setMetricsCollector(new NoOpMetricsCollector());
        }
        MockConnection mockConnection = new MockConnection(mockNode, getMetricsCollector());
        return mockConnection;
    }
}
