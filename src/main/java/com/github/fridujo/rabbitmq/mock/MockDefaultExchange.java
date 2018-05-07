package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

class MockDefaultExchange implements MockExchange {
    private final MockNode node;

    MockDefaultExchange(MockNode mockNode) {
        this.node = mockNode;
    }

    @Override
    public void publish(String routingKey, AMQP.BasicProperties props, byte[] body) {
        node.getQueue(routingKey).ifPresent(q -> q.publish("default", routingKey, props, body));
    }

    @Override
    public void bind(MockQueue mockQueue, String routingKey) {
        // nothing needed
    }
}
