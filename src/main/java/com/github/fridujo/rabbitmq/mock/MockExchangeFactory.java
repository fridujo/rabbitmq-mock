package com.github.fridujo.rabbitmq.mock;

import java.util.Map;

public class MockExchangeFactory {
    public static BindableMockExchange build(String exchangeName, String type, Map<String, Object> arguments, ReceiverRegistry receiverRegistry) {
        if ("topic".equals(type)) {
            return new MockTopicExchange(exchangeName, arguments, receiverRegistry);
        } else if ("direct".equals(type)) {
            return new MockDirectExchange(exchangeName, arguments, receiverRegistry);
        } else if ("fanout".equals(type)) {
            return new MockFanoutExchange(exchangeName, arguments, receiverRegistry);
        }
        throw new IllegalArgumentException("No exchange type " + type);
    }
}
