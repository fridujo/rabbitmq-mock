package com.github.fridujo.rabbitmq.mock;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;

public abstract class AmqpExceptions {

    public static IOException inequivalentExchangeRedeclare(Channel ref,
                                                            String vhost,
                                                            String exchangeName,
                                                            String currentType,
                                                            String receivedType) {
        StringBuilder replyText = new StringBuilder("PRECONDITION_FAILED - inequivalent arg 'type' ")
            .append("for exchange '").append(exchangeName).append("' ")
            .append("in vhost '").append(vhost).append("' ")
            .append("received '").append(receivedType).append("' ")
            .append("but current is '").append(currentType).append("'");

        AMQImpl.Channel.Close reason = new AMQImpl.Channel.Close(
            406,
            replyText.toString(),
            AMQImpl.Exchange.INDEX,
            AMQImpl.Exchange.Declare.INDEX);
        ShutdownSignalException sse = new ShutdownSignalException(
            false,
            false,
            reason,
            ref);
        return new IOException(sse.sensibleClone());
    }

    public static IOException exchangeNotFound(Channel ref,
                                               String vhost,
                                               String exchangeName) {
        StringBuilder replyText = new StringBuilder("NOT_FOUND - no exchange '").append(exchangeName).append("' ")
            .append("in vhost '").append(vhost).append("'");

        ShutdownSignalException reason = new ShutdownSignalException(
            false,
            false,
            new AMQImpl.Channel.Close(
                404,
                replyText.toString(),
                AMQImpl.Exchange.INDEX,
                AMQImpl.Exchange.Declare.INDEX),
            ref);
        return new IOException(reason.sensibleClone());
    }

    public static IOException queueNotFound(Channel ref,
                                               String vhost,
                                               String queueName) {
        StringBuilder replyText = new StringBuilder("NOT_FOUND - no queue '").append(queueName).append("' ")
            .append("in vhost '").append(vhost).append("'");

        ShutdownSignalException reason = new ShutdownSignalException(
            false,
            false,
            new AMQImpl.Channel.Close(
                404,
                replyText.toString(),
                AMQImpl.Queue.INDEX,
                AMQImpl.Queue.Declare.INDEX),
            ref);
        return new IOException(reason.sensibleClone());
    }
}
