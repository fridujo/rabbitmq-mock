package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

public interface TransactionalOperations {

    void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body);

    void basicReject(long deliveryTag, boolean requeue);

    void basicNack(long deliveryTag, boolean multiple, boolean requeue);

    void basicAck(long deliveryTag, boolean multiple);
}
