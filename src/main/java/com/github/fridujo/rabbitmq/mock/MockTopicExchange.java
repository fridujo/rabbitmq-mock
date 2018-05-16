package com.github.fridujo.rabbitmq.mock;

public class MockTopicExchange extends BindableMockExchange {

    public MockTopicExchange(String name, ReceiverRegistry receiverRegistry) {
        super(name, receiverRegistry);
    }

    protected boolean match(String bindingKey, String routingKey) {
        String bindingRegex = bindingKey
            .replace("*", "([^\\.]+)")
            .replace("#", "(.+)");
        return routingKey.matches(bindingRegex);
    }
}
