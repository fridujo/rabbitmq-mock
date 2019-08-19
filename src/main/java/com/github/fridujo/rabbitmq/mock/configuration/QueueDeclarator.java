package com.github.fridujo.rabbitmq.mock.configuration;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.MockChannel;

public class QueueDeclarator {

    private final String queueName;
    private final Map<String, Object> queueArgs = new LinkedHashMap<>();

    private QueueDeclarator(String queueName) {
        this.queueName = queueName;
    }

    public static QueueDeclarator queue(String queueName) {
        return new QueueDeclarator(queueName);
    }

    public static QueueDeclarator dynamicQueue() {
        return queue("");
    }

    public QueueDeclarator withMessageTtl(long messageTtlInMs) {
        queueArgs.put(AmqArguments.MESSAGE_TTL_KEY, messageTtlInMs);
        return this;
    }


    public QueueDeclarator withDeadLetterExchange(String deadLetterExchange) {
        queueArgs.put(AmqArguments.DEAD_LETTER_EXCHANGE_KEY, deadLetterExchange);
        return this;
    }

    public QueueDeclarator withDeadLetterRoutingKey(String deadLetterRoutingKey) {
        queueArgs.put(AmqArguments.DEAD_LETTER_ROUTING_KEY_KEY, deadLetterRoutingKey);
        return this;
    }

    public QueueDeclarator withMaxLength(int maxLength) {
        queueArgs.put(AmqArguments.QUEUE_MAX_LENGTH_KEY, maxLength);
        return this;
    }

    public QueueDeclarator withMaxLengthBytes(int maxLengthBytes) {
        queueArgs.put(AmqArguments.QUEUE_MAX_LENGTH_BYTES_KEY, maxLengthBytes);
        return this;
    }

    public QueueDeclarator withOverflow(AmqArguments.Overflow overflow) {
        queueArgs.put(AmqArguments.OVERFLOW_KEY, overflow.toString());
        return this;
    }

    public QueueDeclarator withMaxPriority(int maxPriority) {
        queueArgs.put(AmqArguments.MAX_PRIORITY_KEY, maxPriority);
        return this;
    }

    public AMQP.Queue.DeclareOk declare(Channel channel) throws IOException {
        return channel.queueDeclare(queueName, true, false, false, queueArgs);
    }

    public AMQP.Queue.DeclareOk declare(MockChannel channel) {
        return channel.queueDeclare(queueName, true, false, false, queueArgs);
    }
}
