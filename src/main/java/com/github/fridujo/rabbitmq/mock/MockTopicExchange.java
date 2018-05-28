package com.github.fridujo.rabbitmq.mock;

import java.util.Map;

public class MockTopicExchange extends BindableMockExchange {

    public MockTopicExchange(String name, Map<String, Object> arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
    }

    protected boolean match(String bindingKey, String routingKey) {
        String bindingRegex = bindingKey
            .replace("*", "([^\\.]+)")
            .replace("#", "(.+)");
        return routingKey.matches(bindingRegex);
    }
}
