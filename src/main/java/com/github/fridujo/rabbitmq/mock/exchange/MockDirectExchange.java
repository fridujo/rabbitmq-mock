package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

import java.util.Map;

public class MockDirectExchange extends BindableMockExchange {

    public MockDirectExchange(String name, Map<String, Object> arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
    }

    @Override
    protected boolean match(String bindingKey, Map<String, Object> bindArguments, String routingKey, Map<String, Object> headers) {
        return bindingKey.equals(routingKey);
    }
}
