package com.github.fridujo.rabbitmq.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.rabbitmq.client.AMQP;

public interface DeadLettering {

    String X_DEATH_HEADER = "x-death";

    enum ReasonType {
        /**
         * The message was rejected with requeue parameter set to false.
         */
        REJECTED("rejected"),
        /**
         * The message TTL has expired.
         */
        EXPIRED("expired"),
        /**
         * The maximum allowed queue length was exceeded.
         */
        MAX_LEN("maxlen");

        public final String headerValue;

        ReasonType(String headerValue) {
            this.headerValue = headerValue;
        }
    }

    class Event {
        private static final String QUEUE_KEY = "queue";
        private static final String REASON_KEY = "reason";
        private static final String EXCHANGE_KEY = "exchange";
        private static final String ROUTING_KEYS_KEY = "routing-keys";
        private static final String COUNT_KEY = "count";

        private final String queue;
        private final ReasonType reason;
        private final String exchange;
        private final List<String> routingKeys;
        private final long count;

        public Event(String queue, ReasonType reason, Message message, int count) {
            this.queue = queue;
            this.reason = reason;
            this.exchange = message.exchangeName;
            this.routingKeys = Collections.singletonList(message.routingKey);
            this.count = count;
        }

        public Map<String, Object> asHeaderEntry() {
            Map<String, Object> entry = new HashMap<>();
            entry.put(QUEUE_KEY, queue);
            entry.put(REASON_KEY, reason.headerValue);
            entry.put(EXCHANGE_KEY, exchange);
            entry.put(ROUTING_KEYS_KEY, routingKeys);
            entry.put(COUNT_KEY, count);
            return entry;
        }

        public AMQP.BasicProperties prependOn(AMQP.BasicProperties props) {
            Map<String, Object> headers = Optional.ofNullable(props.getHeaders()).map(HashMap::new).orElseGet(HashMap::new);

            List<Map<String, Object>> xDeathHeader = (List<Map<String, Object>>) headers.computeIfAbsent(X_DEATH_HEADER, key -> new ArrayList<>());

            Optional<Map<String, Object>> previousEvent = xDeathHeader.stream()
                .filter(this::sameQueueAndReason)
                .findFirst();

            final Map<String, Object> currentEvent;
            if (previousEvent.isPresent()) {
                xDeathHeader.remove(previousEvent.get());
                currentEvent = incrementCount(previousEvent.get());
            } else {
                currentEvent = asHeaderEntry();
            }
            xDeathHeader.add(0, currentEvent);

            return props.builder().headers(Collections.unmodifiableMap(headers)).build();
        }

        private Map<String, Object> incrementCount(Map<String, Object> previousEvent) {
            previousEvent.compute(COUNT_KEY, (key, count) -> (long) count + 1);
            return previousEvent;
        }

        private boolean sameQueueAndReason(Map<String, Object> event) {
            return queue.equals(event.get(QUEUE_KEY)) && reason.headerValue.equals(event.get(REASON_KEY));
        }
    }
}
