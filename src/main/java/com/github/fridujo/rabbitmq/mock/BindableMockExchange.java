package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BindableMockExchange implements MockExchange {

    private final Map<String, ReceiverPointer> receiversByBindingKeys = new ConcurrentHashMap<>();
    private final String name;
    private final ReceiverPointer pointer;
    private final ReceiverRegistry receiverRegistry;

    protected BindableMockExchange(String name, ReceiverRegistry receiverRegistry) {
        this.name = name;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, name);
        this.receiverRegistry = receiverRegistry;
    }

    @Override
    public void publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        receiversByBindingKeys
            .entrySet()
            .stream()
            .filter(e -> match(e.getKey(), routingKey))
            .map(Map.Entry::getValue)
            .map(receiverRegistry::getReceiver)
            .distinct()
            .forEach(e -> e.publish(name, routingKey, props, body));
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
