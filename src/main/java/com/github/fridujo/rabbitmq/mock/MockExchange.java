package com.github.fridujo.rabbitmq.mock;

import java.util.Map;

public interface MockExchange extends Receiver {

    void bind(ReceiverPointer mockQueue, String routingKey, Map<String, Object> arguments);

    void unbind(ReceiverPointer pointer, String routingKey);
}
