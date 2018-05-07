package com.github.fridujo.rabbitmq.mock;

public class MockDirectExchange extends BindableMockExchange {

    public MockDirectExchange(String name) {
        super(name);
    }

    @Override
    protected boolean match(String bindingKey, String routingKey) {
        return bindingKey.equals(routingKey);
    }
}
