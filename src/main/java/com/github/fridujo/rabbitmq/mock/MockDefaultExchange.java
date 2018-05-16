package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

class MockDefaultExchange implements MockExchange {
    private final MockNode node;

    MockDefaultExchange(MockNode mockNode) {
        this.node = mockNode;
    }

    @Override
    public void publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        node.getQueue(routingKey).ifPresent(q -> q.publish("default", routingKey, props, body));
    }

    @Override
    public void bind(ReceiverPointer receiver, String routingKey) {
        // nothing needed
    }

    @Override
    public void unbind(ReceiverPointer pointer, String routingKey) {
        // nothing needed
    }

    @Override
    public ReceiverPointer pointer() {
        throw new IllegalStateException("No ReceiverPointer (internal use) should be needed for the default exchange");
    }
}
