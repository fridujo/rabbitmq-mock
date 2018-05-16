package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

/**
 * Leverage the receiving capability of both Queues and Exchanges.
 */
interface Receiver {
    void publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body);

    ReceiverPointer pointer();
}
