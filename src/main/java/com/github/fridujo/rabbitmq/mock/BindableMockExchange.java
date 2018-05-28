package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class BindableMockExchange implements MockExchange {

    private final Map<String, ReceiverPointer> receiversByBindingKeys = new ConcurrentHashMap<>();
    private final String name;
    private final Map<String, Object> arguments;
    private final ReceiverPointer pointer;
    private final ReceiverRegistry receiverRegistry;

    protected BindableMockExchange(String name, Map<String, Object> arguments, ReceiverRegistry receiverRegistry) {
        this.name = name;
        this.arguments = arguments;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, name);
        this.receiverRegistry = receiverRegistry;
    }

    @Override
    public void publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        Set<Receiver> matchingReceivers = receiversByBindingKeys
            .entrySet()
            .stream()
            .filter(e -> match(e.getKey(), routingKey))
            .map(Map.Entry::getValue)
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
        return Optional.ofNullable(arguments.get(ALTERNATE_EXCHANGE_KEY))
            .filter(aeObject -> aeObject instanceof String)
            .map(String.class::cast)
            .map(aeName -> new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, aeName))
            .map(receiverRegistry::getReceiver)
            .filter(Optional::isPresent)
            .map(Optional::get);
    }

    protected abstract boolean match(String bindingKey, String routingKey);

    @Override
    public void bind(ReceiverPointer receiver, String routingKey) {
        receiversByBindingKeys.put(routingKey, receiver);
    }

    @Override
    public void unbind(ReceiverPointer receiver, String routingKey) {
        receiversByBindingKeys.remove(routingKey);
    }

    @Override
    public ReceiverPointer pointer() {
        return pointer;
    }
}
