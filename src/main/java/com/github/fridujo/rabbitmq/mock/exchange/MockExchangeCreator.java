package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public interface MockExchangeCreator {

    String getType();

    BindableMockExchange createMockExchange(String exchangeName, AmqArguments arguments, ReceiverRegistry receiverRegistry);
}
