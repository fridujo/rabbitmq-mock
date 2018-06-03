package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

/**
 * Leverage the receiving capability of both Queues and Exchanges.
 */
interface Receiver {
    String ALTERNATE_EXCHANGE_KEY = "alternate-exchange";
    String DEAD_LETTER_EXCHANGE_KEY = "x-dead-letter-exchange";
    String X_MATCH_KEY = "x-match";

    void publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body);

    ReceiverPointer pointer();
}
