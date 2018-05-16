package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.ConnectionFactory;

public class MockConnectionFactory extends ConnectionFactory {

    private final MockNode mockNode = new MockNode();

    @Override
    public MockConnection newConnection() {
        return new MockConnection(mockNode);
    }
}
