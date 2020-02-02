package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

/**
 * Leverage the receiving capability of both Queues and Exchanges.
 */
public interface Receiver {
    String X_MATCH_KEY = "x-match";

    /**
     * @return true if message got routed, false otherwise
     */
    boolean publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body);

    ReceiverPointer pointer();
}
