package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public abstract class MockExchangeFactory {
    public static BindableMockExchange build(String exchangeName, String type, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        if ("topic".equals(type)) {
            return new MockTopicExchange(exchangeName, arguments, receiverRegistry);
        } else if ("direct".equals(type)) {
            return new MockDirectExchange(exchangeName, arguments, receiverRegistry);
        } else if ("fanout".equals(type)) {
            return new MockFanoutExchange(exchangeName, arguments, receiverRegistry);
        } else if ("headers".equals(type)) {
            return new MockHeadersExchange(exchangeName, arguments, receiverRegistry);
        }
        throw new IllegalArgumentException("No exchange type " + type);
    }
}
