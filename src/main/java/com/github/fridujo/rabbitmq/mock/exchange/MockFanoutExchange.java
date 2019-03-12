package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

import java.util.Map;

public class MockFanoutExchange extends BindableMockExchange {

    public static final String TYPE = "fanout";

    protected MockFanoutExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, TYPE, arguments, receiverRegistry);
    }

    @Override
    protected boolean match(String bindingKey, Map<String, Object> bindArguments, String routingKey, Map<String, Object> headers) {
        return true;
    }
}
