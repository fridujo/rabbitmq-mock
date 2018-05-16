package com.github.fridujo.rabbitmq.mock;

public class MockExchangeFactory {
    public static BindableMockExchange build(String exchangeName, String type) {
        if ("topic".equals(type)) {
            return new MockTopicExchange(exchangeName);
        } else if ("direct".equals(type)) {
            return new MockDirectExchange(exchangeName);
        } else if ("fanout".equals(type)) {
            return new MockFanoutExchange(exchangeName);
        }
        throw new IllegalArgumentException("No exchange type " + type);
    }
}
