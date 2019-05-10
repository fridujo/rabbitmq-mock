package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Optional;
import java.util.stream.Stream;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.AMQP;

public abstract class SingleReceiverExchange extends BindableMockExchange {
    
    protected SingleReceiverExchange(String name, String type, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, type, arguments, receiverRegistry);
    }

    @Override
    protected Stream<ReceiverPointer> matchingReceivers(String routingKey, AMQP.BasicProperties props) {
        return Stream.of(selectReceiver(routingKey, props))
            .filter(Optional::isPresent)
            .map(Optional::get);
    }
    
    protected abstract Optional<ReceiverPointer> selectReceiver(String routingKey, AMQP.BasicProperties props);
}
