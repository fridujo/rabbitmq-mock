package com.github.fridujo.rabbitmq.mock;

public interface MockExchange extends Receiver {

    void bind(ReceiverPointer mockQueue, String routingKey);

    void unbind(ReceiverPointer pointer, String routingKey);
}
