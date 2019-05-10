package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public class MockFanoutExchange extends MultipleReceiverExchange {

    public static final String TYPE = "fanout";

    protected MockFanoutExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, TYPE, arguments, receiverRegistry);
    }

    @Override
    protected boolean match(BindConfiguration bindConfiguration, String routingKey, Map<String, Object> headers) {
        return true;
    }
}
