package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public class MockHeadersExchange extends MultipleReceiverExchange {

    public static final String TYPE = "headers";

    private static final String MATCH_ALL = "all";
    private static final Set<String> X_MATCH_VALID_VALUES = new HashSet<>(Arrays.asList("any", MATCH_ALL));

    public MockHeadersExchange(String exchangeName, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(exchangeName, TYPE, arguments, receiverRegistry);
    }

    @Override
    protected boolean match(BindConfiguration bindConfiguration, String routingKey, Map<String, Object> headers) {
        String xMatch = Optional.ofNullable(bindConfiguration.bindArguments.get(X_MATCH_KEY))
            .filter(xMatchObject -> xMatchObject instanceof String)
            .map(String.class::cast)
            .filter(X_MATCH_VALID_VALUES::contains)
            .orElse(MATCH_ALL);

        Predicate<Map.Entry<String, Object>> argumentPredicate = e -> e.getValue() == null ? headers.containsKey(e.getKey()) : Objects.equals(e.getValue(), headers.get(e.getKey()));
        Stream<Map.Entry<String, Object>> argumentsToMatch = bindConfiguration.bindArguments.entrySet().stream()
            .filter(e -> !X_MATCH_KEY.equals(e.getKey()));

        final boolean match;
        if (MATCH_ALL.equals(xMatch)) {
            match = argumentsToMatch.allMatch(argumentPredicate);
        } else {
            match = argumentsToMatch.anyMatch(argumentPredicate);
        }
        return match;
    }
}
