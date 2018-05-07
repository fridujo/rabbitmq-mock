package com.github.fridujo.rabbitmq.mock;

public class MockTopicExchange extends BindableMockExchange {

    public MockTopicExchange(String name) {
        super(name);
    }

    protected boolean match(String bindingKey, String routingKey) {
        return true;
    }
}
