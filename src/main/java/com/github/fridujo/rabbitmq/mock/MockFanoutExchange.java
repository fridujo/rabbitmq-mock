package com.github.fridujo.rabbitmq.mock;

import java.util.Map;

public class MockFanoutExchange extends BindableMockExchange {

    protected MockFanoutExchange(String name, Map<String, Object> arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
    }

    @Override
    protected boolean match(String bindingKey, Map<String, Object> bindArguments, String routingKey, Map<String, Object> headers) {
        return true;
    }
}
