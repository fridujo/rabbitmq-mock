package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class MockExchangeFactory {

    @FunctionalInterface
    public interface MockExchangeCreator {
        BindableMockExchange createMockExchange(String exchangeName, AmqArguments arguments, ReceiverRegistry receiverRegistry);
    }

    private static final Map<String, MockExchangeCreator> mockExchanges = new HashMap<>();

    static {
        mockExchanges.put("topic", (exchangeName, arguments, receiverRegistry) -> new MockTopicExchange(exchangeName, arguments, receiverRegistry));
        mockExchanges.put("direct", (exchangeName, arguments, receiverRegistry) -> new MockDirectExchange(exchangeName, arguments, receiverRegistry));
        mockExchanges.put("fanout", (exchangeName, arguments, receiverRegistry) -> new MockFanoutExchange(exchangeName, arguments, receiverRegistry));
        mockExchanges.put("headers", (exchangeName, arguments, receiverRegistry) -> new MockHeadersExchange(exchangeName, arguments, receiverRegistry));
    }

    public static void registerMockExchange(String type, MockExchangeCreator factoryCreator){
        mockExchanges.put( type, factoryCreator);
    }

    public static BindableMockExchange build(String exchangeName, String type, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        return Optional.ofNullable(mockExchanges.get(type))
            .map(factoryCreator -> factoryCreator.createMockExchange(exchangeName, arguments, receiverRegistry))
            .orElseThrow(() -> new IllegalArgumentException("No exchange type " + type));
    }
}
