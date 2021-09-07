package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.MockPolicy;
import com.github.fridujo.rabbitmq.mock.Receiver;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.AMQP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public abstract class BindableMockExchange implements MockExchange {
    private static final Logger LOGGER = LoggerFactory.getLogger(BindableMockExchange.class);

    protected final Set<BindConfiguration> bindConfigurations = new LinkedHashSet<>();
    private final String name;
    private final String type;
    private final AmqArguments arguments;
    private final ReceiverPointer pointer;
    private final ReceiverRegistry receiverRegistry;
    private Optional<MockPolicy> mockPolicy = Optional.empty();

    protected BindableMockExchange(String name, String type, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        this.name = name;
        this.type = type;
        this.arguments = arguments;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, name);
        this.receiverRegistry = receiverRegistry;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public boolean publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        Set<Receiver> matchingReceivers = matchingReceivers(routingKey, props)
            .map(receiverRegistry::getReceiver)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

        if (matchingReceivers.isEmpty()) {
            return getAlternateExchange().map(e -> {
                LOGGER.debug(localized("message to alternate " + e));
                return e.publish(name, routingKey, props, body);
            }).orElse(false);
        } else {
            matchingReceivers
                .forEach(e -> {
                    LOGGER.debug(localized("message to " + e));
                    e.publish(name, routingKey, props, body);
                });
            return true;
        }
    }

    private Optional<Receiver> getAlternateExchange() {
        Optional<ReceiverPointer> policyAlternativeExchange = mockPolicy.flatMap(MockPolicy::getAlternateExchange);
        Optional<ReceiverPointer> exchangeArgumentAlternativeExchange = arguments.getAlternateExchange();

        return exchangeArgumentAlternativeExchange.map(Optional::of)
            .orElse(policyAlternativeExchange)
            .flatMap(receiverRegistry::getReceiver);
    }

    protected abstract Stream<ReceiverPointer> matchingReceivers(String routingKey, AMQP.BasicProperties props);

    private String localized(String message) {
        return "[E " + name + "] " + message;
    }

    @Override
    public void bind(ReceiverPointer receiver, String routingKey, Map<String, Object> arguments) {
        bindConfigurations.add(new BindConfiguration(routingKey, receiver, arguments));
    }

    @Override
    public void unbind(ReceiverPointer receiver, String routingKey) {
        bindConfigurations.remove(new BindConfiguration(routingKey, receiver, Collections.emptyMap()));
    }

    @Override
    public void setPolicy(Optional<MockPolicy> mockPolicy) {
        mockPolicy.ifPresent(p -> LOGGER.info(localized(format("Applied policy %s", p))));
        this.mockPolicy = mockPolicy;
    }

    @Override
    public ReceiverPointer pointer() {
        return pointer;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " " + name;
    }

    public static class BindConfiguration {
        public final String bindingKey;
        public final ReceiverPointer receiverPointer;
        public final Map<String, Object> bindArguments;

        public BindConfiguration(String bindingKey, ReceiverPointer receiverPointer, Map<String, Object> bindArguments) {
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
