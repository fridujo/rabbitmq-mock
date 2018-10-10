package com.github.fridujo.rabbitmq.mock;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class AmqArguments {
    private final String ALTERNATE_EXCHANGE_KEY = "alternate-exchange";
    private final String DEAD_LETTER_EXCHANGE_KEY = "x-dead-letter-exchange";
    private final String DEAD_LETTER_ROUTING_KEY_KEY = "x-dead-letter-routing-key";
    private final String MESSAGE_TTL_KEY = "x-message-ttl";
    private final String QUEUE_MAX_LENGTH_KEY = "x-max-length";
    private final String QUEUE_MAX_LENGTH_BYTES_KEY = "x-max-length-bytes";
    private final String OVERFLOW_KEY = "x-overflow";
    private final String MAX_PRIORITY_KEY = "x-max-priority";

    private final Map<String, Object> arguments;

    public AmqArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    public Optional<ReceiverPointer> getAlternateExchange() {
        return string(ALTERNATE_EXCHANGE_KEY)
            .map(aeName -> new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, aeName));
    }

    public Optional<ReceiverPointer> getDeadLetterExchange() {
        return string(DEAD_LETTER_EXCHANGE_KEY)
            .map(aeName -> new ReceiverPointer(ReceiverPointer.Type.EXCHANGE, aeName));
    }

    public Optional<String> getDeadLetterRoutingKey() {
        return string(DEAD_LETTER_ROUTING_KEY_KEY);
    }

    public Optional<Integer> queueLengthLimit() {
        return positiveInteger(QUEUE_MAX_LENGTH_KEY);
    }

    public Optional<Integer> queueLengthBytesLimit() {
        return positiveInteger(QUEUE_MAX_LENGTH_BYTES_KEY);
    }

    public Overflow overflow() {
        return string(OVERFLOW_KEY)
            .flatMap(Overflow::parse)
            .orElse(Overflow.DROP_HEAD);
    }

    public Optional<Long> getMessageTtlOfQueue() {
        return Optional.ofNullable(arguments.get(MESSAGE_TTL_KEY))
            .filter(aeObject -> aeObject instanceof Number)
            .map(Number.class::cast)
            .map(number -> number.longValue());
    }

    public Optional<Short> queueMaxPriority() {
        return positiveInteger(MAX_PRIORITY_KEY)
            .filter(i -> i < 256)
            .map(Integer::shortValue);
    }

    private Optional<Integer> positiveInteger(String key) {
        return Optional.ofNullable(arguments.get(key))
            .filter(aeObject -> aeObject instanceof Number)
            .map(Number.class::cast)
            .map(num -> num.intValue())
            .filter(i -> i > 0);
    }

    private Optional<String> string(String key) {
        return Optional.ofNullable(arguments.get(key))
            .filter(aeObject -> aeObject instanceof String)
            .map(String.class::cast);
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

    }
}
