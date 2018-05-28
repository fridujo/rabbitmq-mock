package com.github.fridujo.rabbitmq.mock;

import java.util.Map;

public interface MockExchange extends Receiver {

    String ALTERNATE_EXCHANGE_KEY = "alternate-exchange";
    String X_MATCH_KEY = "x-match";

    void bind(ReceiverPointer mockQueue, String routingKey, Map<String, Object> arguments);

    void unbind(ReceiverPointer pointer, String routingKey);
}
