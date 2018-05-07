package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

public interface MockExchange {
    void publish(String routingKey, AMQP.BasicProperties props, byte[] body);

    void bind(MockQueue mockQueue, String routingKey);
}
