package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.metrics.MetricsCollectorWrapper;
import com.rabbitmq.client.AddressResolver;
import com.rabbitmq.client.ConnectionFactory;

import java.util.concurrent.ExecutorService;

public class MockConnectionFactory extends ConfigurableConnectionFactory<MockConnectionFactory> {

    public MockConnectionFactory() {
        setAutomaticRecoveryEnabled(false);
    }

    @Override
    public MockConnection newConnection(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName) {
        return newConnection();
    }

    public MockConnection newConnection() {
        MetricsCollectorWrapper metricsCollectorWrapper = MetricsCollectorWrapper.Builder.build(this);
        MockConnection mockConnection = new MockConnection(mockNode, metricsCollectorWrapper);
        return mockConnection;
    }
}
