package com.github.fridujo.rabbitmq.mock.compatibility;

import com.github.fridujo.rabbitmq.mock.MockConnection;
import com.github.fridujo.rabbitmq.mock.MockNode;
import com.github.fridujo.rabbitmq.mock.metrics.MetricsCollectorWrapper;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class MockConnectionFactoryWithoutAddressResolver extends ConnectionFactory {

    private final MockNode mockNode = new MockNode();


    public MockConnectionFactoryWithoutAddressResolver() {
        setAutomaticRecoveryEnabled(false);
    }

    public Connection newConnection(ExecutorService executor, List<Address> addrs, String clientProvidedName) {
        return newConnection();
    }

    public MockConnection newConnection() {
        MetricsCollectorWrapper metricsCollectorWrapper = MetricsCollectorWrapper.Builder.build(this);
        MockConnection mockConnection = new MockConnection(mockNode, metricsCollectorWrapper);
        return mockConnection;
    }
}
