package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

import java.util.Map;

public class MockTopicExchange extends BindableMockExchange {

    public MockTopicExchange(String name, Map<String, Object> arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
    }

    protected boolean match(String bindingKey, Map<String, Object> bindArguments, String routingKey, Map<String, Object> headers) {
        String bindingRegex = bindingKey
            .replace("*", "([^\\.]+)")
            .replace("#", "(.+)");
        return routingKey.matches(bindingRegex);
    }
}
