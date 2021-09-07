package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;
import java.util.Optional;

import com.github.fridujo.rabbitmq.mock.MockPolicy;
import com.rabbitmq.client.AMQP;

import com.github.fridujo.rabbitmq.mock.MockNode;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;

public class MockDefaultExchange implements MockExchange {

    public static final String TYPE = "default";
    public static final String NAME = "";
    
    private final MockNode node;

    public MockDefaultExchange(MockNode mockNode) {
        this.node = mockNode;
    }

    @Override
    public boolean publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        return node.getQueue(routingKey).map(q -> q.publish(NAME, routingKey, props, body)).orElse(false);
    }

    @Override
    public String getType() {
        return TYPE;
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
    public void setPolicy(Optional<MockPolicy> mockPolicy) {
        throw new IllegalStateException("No policy should be applied for the default exchange");
    }

    @Override
    public ReceiverPointer pointer() {
        throw new IllegalStateException("No ReceiverPointer (internal use) should be needed for the default exchange");
    }
}
