package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.ConnectionFactory;

import com.github.fridujo.rabbitmq.mock.exchange.MockExchangeCreator;

public abstract class ConfigurableConnectionFactory<T extends ConfigurableConnectionFactory> extends ConnectionFactory {

    protected final MockNode mockNode = new MockNode();
    
    @SuppressWarnings("unchecked")
    public T withAdditionalExchange(MockExchangeCreator mockExchangeCreator) {
        mockNode.getConfiguration().registerAdditionalExchangeCreator(mockExchangeCreator);
        return (T) this;
    }
}
