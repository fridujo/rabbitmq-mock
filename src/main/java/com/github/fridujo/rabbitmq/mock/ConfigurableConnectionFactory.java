package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.ConnectionFactory;

public abstract class ConfigurableConnectionFactory<T extends ConfigurableConnectionFactory> extends ConnectionFactory {

    protected final MockNode mockNode = new MockNode();
}
