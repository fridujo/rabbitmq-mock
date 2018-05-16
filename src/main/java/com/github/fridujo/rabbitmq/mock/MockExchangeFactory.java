package com.github.fridujo.rabbitmq.mock;

public class MockExchangeFactory {
    public static BindableMockExchange build(String exchangeName, String type, ReceiverRegistry receiverRegistry) {
        if ("topic".equals(type)) {
            return new MockTopicExchange(exchangeName, receiverRegistry);
        } else if ("direct".equals(type)) {
            return new MockDirectExchange(exchangeName, receiverRegistry);
        } else if ("fanout".equals(type)) {
            return new MockFanoutExchange(exchangeName, receiverRegistry);
        }
        throw new IllegalArgumentException("No exchange type " + type);
    }
}
