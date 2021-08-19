package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.Receiver;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;

import java.util.Map;

public interface MockExchange extends Receiver {
    
    String getType();

    void bind(ReceiverPointer mockQueue, String routingKey, Map<String, Object> arguments);

    void unbind(ReceiverPointer pointer, String routingKey, Map<String, Object> arguments);
}
