package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

/**
 * Leverage the receiving capability of both Queues and Exchanges.
 */
public interface Receiver {
    String ALTERNATE_EXCHANGE_KEY = "alternate-exchange";
    String DEAD_LETTER_EXCHANGE_KEY = "x-dead-letter-exchange";
    String MESSAGE_TTL_KEY = "x-message-ttl";
    String X_MATCH_KEY = "x-match";
    String QUEUE_MAX_LENGTH_KEY = "x-max-length";
    String QUEUE_MAX_LENGTH_BYTES_KEY = "x-max-length-bytes";
    String OVERFLOW_KEY = "x-overflow";

    void publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body);

    ReceiverPointer pointer();
}
