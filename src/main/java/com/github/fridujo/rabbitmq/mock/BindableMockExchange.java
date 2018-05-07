package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BindableMockExchange implements MockExchange {

    private final Map<String, MockQueue> queuesByBindingKeys = new ConcurrentHashMap<>();
    private final String name;

    protected BindableMockExchange(String name) {
        this.name = name;
    }

    @Override
    public void publish(String routingKey, AMQP.BasicProperties props, byte[] body) {
        queuesByBindingKeys
            .entrySet()
            .stream()
            .filter(e -> match(e.getKey(), routingKey))
            .forEach(e -> e.getValue().publish(name, routingKey, props, body));
    }

    protected abstract boolean match(String bindingKey, String routingKey);

    @Override
    public void bind(MockQueue mockQueue, String routingKey) {
        queuesByBindingKeys.put(routingKey, mockQueue);
    }
}
