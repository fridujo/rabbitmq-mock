package com.github.fridujo.rabbitmq.mock;

import static com.github.fridujo.rabbitmq.mock.exchange.MockExchangeCreator.creatorWithExchangeType;

import com.github.fridujo.rabbitmq.mock.exchange.ConsistentHashExchange;
import com.github.fridujo.rabbitmq.mock.exchange.TypedMockExchangeCreator;
import com.rabbitmq.client.ConnectionFactory;

public abstract class ConfigurableConnectionFactory<T extends ConfigurableConnectionFactory> extends ConnectionFactory {

    protected final MockNode mockNode = new MockNode();

    @SuppressWarnings("unchecked")
    public T withAdditionalExchange(TypedMockExchangeCreator mockExchangeCreator) {
        mockNode.getConfiguration().registerAdditionalExchangeCreator(mockExchangeCreator);
        return (T) this;
    }

    /**
     * Make available the {@value ConsistentHashExchange#TYPE}'' exchange.
     * <p>
     * See <a href="https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange">https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange</a>.
     * @return  this {@link ConfigurableConnectionFactory} instance (for chaining)
     */
    public T enableConsistentHashPlugin() {
        return withAdditionalExchange(
            creatorWithExchangeType(ConsistentHashExchange.TYPE, ConsistentHashExchange::new)
        );
    }
}
