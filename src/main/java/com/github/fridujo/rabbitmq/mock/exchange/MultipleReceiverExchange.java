package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;
import java.util.stream.Stream;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.AMQP;

public abstract class MultipleReceiverExchange extends BindableMockExchange {

    protected MultipleReceiverExchange(String name, String type, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, type, arguments, receiverRegistry);
    }

    protected Stream<ReceiverPointer> matchingReceivers(String routingKey, AMQP.BasicProperties props) {
        return bindConfigurations
            .stream()
            .filter(bindConfiguration -> match(bindConfiguration, routingKey, props.getHeaders()))
            .map(BindableMockExchange.BindConfiguration::receiverPointer);
    }

    protected abstract boolean match(BindableMockExchange.BindConfiguration bindConfiguration, String routingKey, Map<String, Object> headers);
}
