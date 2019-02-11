package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

@FunctionalInterface
public interface MockExchangeCreator {

    static TypedMockExchangeCreator creatorWithExchangeType(String type, MockExchangeCreator creator) {
        return new TypedMockExchangeCreatorImpl(type, creator);
    }

    BindableMockExchange createMockExchange(String exchangeName, AmqArguments arguments, ReceiverRegistry receiverRegistry);

    public final class TypedMockExchangeCreatorImpl implements TypedMockExchangeCreator {

        private final String type;
        private final MockExchangeCreator creator;

        public TypedMockExchangeCreatorImpl(String type, MockExchangeCreator creator) {
            this.type = type;
            this.creator = creator;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public BindableMockExchange createMockExchange(String exchangeName, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
            return creator.createMockExchange(exchangeName, arguments, receiverRegistry);
        }
    }
}
