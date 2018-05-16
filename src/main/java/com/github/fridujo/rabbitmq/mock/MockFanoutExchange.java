package com.github.fridujo.rabbitmq.mock;

public class MockFanoutExchange extends BindableMockExchange {

    protected MockFanoutExchange(String name) {
        super(name);
    }

    @Override
    protected boolean match(String bindingKey, String routingKey) {
        return true;
    }
}
