package com.github.fridujo.rabbitmq.mock;

public interface MockExchange extends Receiver {

    String ALTERNATE_EXCHANGE_KEY = "alternate-exchange";

    void bind(ReceiverPointer mockQueue, String routingKey);

    void unbind(ReceiverPointer pointer, String routingKey);
}
