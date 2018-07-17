package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.Receiver;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.AMQP;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BindableMockExchange implements MockExchange {

    private final Set<BindConfiguration> bindConfigurations = new LinkedHashSet<>();
    private final String name;
    private final AmqArguments arguments;
    private final ReceiverPointer pointer;
    private final ReceiverRegistry receiverRegistry;

    protected BindableMockExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        this.name = name;
        this.arguments = arguments;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, name);
        this.receiverRegistry = receiverRegistry;
    }

    @Override
    public void publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        Set<Receiver> matchingReceivers = bindConfigurations
            .stream()
            .filter(bindConfiguration -> match(bindConfiguration.bindingKey, bindConfiguration.bindArguments, routingKey, props.getHeaders()))
            .map(BindConfiguration::receiverPointer)
            .map(receiverRegistry::getReceiver)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .distinct()
            .collect(Collectors.toSet());

        if (matchingReceivers.isEmpty()) {
            getAlternateExchange().ifPresent(e -> e.publish(name, routingKey, props, body));
        } else {
            matchingReceivers
                .forEach(e -> e.publish(name, routingKey, props, body));
        }
    }

    private Optional<Receiver> getAlternateExchange() {
        return arguments.getAlternateExchange().flatMap(receiverRegistry::getReceiver);
    }

    protected abstract boolean match(String bindingKey, Map<String, Object> bindArguments, String routingKey, Map<String, Object> headers);

    @Override
    public void bind(ReceiverPointer receiver, String routingKey, Map<String, Object> arguments) {
        bindConfigurations.add(new BindConfiguration(routingKey, receiver, arguments));
    }

    @Override
    public void unbind(ReceiverPointer receiver, String routingKey) {
        bindConfigurations.remove(new BindConfiguration(routingKey, receiver, Collections.emptyMap()));
    }

    @Override
    public ReceiverPointer pointer() {
        return pointer;
    }

    static class BindConfiguration {
        final String bindingKey;
        final ReceiverPointer receiverPointer;
        final Map<String, Object> bindArguments;

        private BindConfiguration(String bindingKey, ReceiverPointer receiverPointer, Map<String, Object> bindArguments) {
            this.bindingKey = bindingKey;
            this.receiverPointer = receiverPointer;
            this.bindArguments = bindArguments;
        }

        public ReceiverPointer receiverPointer() {
            return receiverPointer;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BindConfiguration that = (BindConfiguration) o;
            return Objects.equals(bindingKey, that.bindingKey) &&
                Objects.equals(receiverPointer, that.receiverPointer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bindingKey, receiverPointer);
        }
    }
}
