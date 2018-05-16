package com.github.fridujo.rabbitmq.mock;

public class MockFanoutExchange extends BindableMockExchange {

    protected MockFanoutExchange(String name, ReceiverRegistry receiverRegistry) {
        super(name, receiverRegistry);
    }

    @Override
    protected boolean match(String bindingKey, String routingKey) {
        return true;
    }
}
