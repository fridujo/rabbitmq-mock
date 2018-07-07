package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.MockNode;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.rabbitmq.client.AMQP;

import java.util.Map;

public class MockDefaultExchange implements MockExchange {
    private final MockNode node;

    public MockDefaultExchange(MockNode mockNode) {
        this.node = mockNode;
    }

    @Override
    public void publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        node.getQueue(routingKey).ifPresent(q -> q.publish("default", routingKey, props, body));
    }

    @Override
    public void bind(ReceiverPointer receiver, String routingKey, Map<String, Object> arguments) {
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
