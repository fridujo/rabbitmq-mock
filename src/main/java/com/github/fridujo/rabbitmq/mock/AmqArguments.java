package com.github.fridujo.rabbitmq.mock;

import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsExchangePointer;
import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsPositiveInteger;
import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsPositiveLong;
import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsPositiveShort;
import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsString;
import static java.util.Collections.emptyMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class AmqArguments {
    public static final String DEAD_LETTER_EXCHANGE_KEY = "x-dead-letter-exchange";
    public static final String DEAD_LETTER_ROUTING_KEY_KEY = "x-dead-letter-routing-key";
    public static final String MESSAGE_TTL_KEY = "x-message-ttl";
    public static final String QUEUE_MAX_LENGTH_KEY = "x-max-length";
    public static final String QUEUE_MAX_LENGTH_BYTES_KEY = "x-max-length-bytes";
    public static final String OVERFLOW_KEY = "x-overflow";
    public static final String MAX_PRIORITY_KEY = "x-max-priority";
    public static final String ALTERNATE_EXCHANGE_KEY = "alternate-exchange";
    private final Map<String, Object> arguments;

    public static AmqArguments empty() {
        return new AmqArguments(emptyMap());
    }

    public AmqArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    public Optional<ReceiverPointer> getAlternateExchange() {
        return getParameterAsExchangePointer.apply(ALTERNATE_EXCHANGE_KEY, arguments);
    }

    public Optional<ReceiverPointer> getDeadLetterExchange() {
        return getParameterAsExchangePointer.apply(DEAD_LETTER_EXCHANGE_KEY, arguments);
    }

    public Optional<String> getDeadLetterRoutingKey() {
        return getParameterAsString.apply(DEAD_LETTER_ROUTING_KEY_KEY, arguments);
    }

    public Optional<Integer> queueLengthLimit() {
        return getParameterAsPositiveInteger.apply(QUEUE_MAX_LENGTH_KEY, arguments);
    }

    public Optional<Integer> queueLengthBytesLimit() {
        return getParameterAsPositiveInteger.apply(QUEUE_MAX_LENGTH_BYTES_KEY, arguments);
    }

    public Overflow overflow() {
        return getParameterAsString.apply(OVERFLOW_KEY, arguments)
            .flatMap(Overflow::parse)
            .orElse(Overflow.DROP_HEAD);
    }

    public Optional<Long> getMessageTtlOfQueue() {
        return getParameterAsPositiveLong.apply(MESSAGE_TTL_KEY, arguments);
    }

    public Optional<Short> queueMaxPriority() {
        return getParameterAsPositiveShort.apply(MAX_PRIORITY_KEY, arguments);
    }

    public enum Overflow {
        DROP_HEAD("drop-head"), REJECT_PUBLISH("reject-publish");

        private final String stringValue;

        Overflow(String stringValue) {
            this.stringValue = stringValue;
        }

        private static Optional<Overflow> parse(String value) {
            return Arrays.stream(values()).filter(v -> value.equals(v.stringValue)).findFirst();
        }

        @Override
        public String toString() {
            return stringValue;
        }
    }
}
