package com.github.fridujo.rabbitmq.mock;

public class MockDirectExchange extends BindableMockExchange {

    public MockDirectExchange(String name, ReceiverRegistry receiverRegistry) {
        super(name, receiverRegistry);
    }

    @Override
    protected boolean match(String bindingKey, String routingKey) {
        return bindingKey.equals(routingKey);
    }
}
