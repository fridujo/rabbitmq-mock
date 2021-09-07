package com.github.fridujo.rabbitmq.mock;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.github.fridujo.rabbitmq.mock.MockPolicy.ApplyTo.ALL;
import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsExchangePointer;
import static com.github.fridujo.rabbitmq.mock.tool.ParameterMarshaller.getParameterAsString;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Getter
@ToString
public class MockPolicy {
    public static final String ALTERNATE_EXCHANGE = "alternate-exchange";
    public static final String DEAD_LETTER_EXCHANGE = "dead-letter-exchange";
    public static final String DEAD_LETTER_ROUTING_KEY = "dead-letter-routing-key";

    private String name;
    private String pattern;
    private Integer priority;
    private Map<String, Object> definitions;
    private ApplyTo applyTo;

    static final Comparator<MockPolicy> comparator = Comparator.comparing(MockPolicy::getPriority).reversed();

    final Predicate<Receiver> receiverMatchesPolicyPattern =
        r -> r.pointer().name.matches(this.pattern) && this.applyTo.matches(r) ;

    @Builder(toBuilder = true)
    public MockPolicy(@NonNull String name, @NonNull String pattern, @NonNull @Singular Map<String, Object> definitions,
                      Integer priority, ApplyTo applyTo) {
        this.name = name;
        this.pattern = pattern;
        this.definitions = definitions;
        this.priority = priority == null ? 0 : priority;
        this.applyTo = applyTo == null ? ALL : applyTo;
    }

    public Optional<ReceiverPointer> getAlternateExchange() {
        return getParameterAsExchangePointer.apply(ALTERNATE_EXCHANGE, definitions);
    }

    public Optional<ReceiverPointer> getDeadLetterExchange() {
        return getParameterAsExchangePointer.apply(DEAD_LETTER_EXCHANGE, definitions);
    }

    public Optional<String> getDeadLetterRoutingKey() {
        return getParameterAsString.apply(DEAD_LETTER_ROUTING_KEY, definitions);
    }

    public enum ApplyTo {
        ALL(asList(ReceiverPointer.Type.QUEUE, ReceiverPointer.Type.EXCHANGE)),
        EXCHANGE(singletonList(ReceiverPointer.Type.EXCHANGE)),
        QUEUE(singletonList(ReceiverPointer.Type.QUEUE));

        private List<ReceiverPointer.Type> supportedTypes;

        ApplyTo(List<ReceiverPointer.Type> supportTypes) {
            this.supportedTypes = supportTypes;
        }

        public boolean matches(Receiver r) {
            return supportedTypes.contains(r.pointer().type);
        }
    }
}

