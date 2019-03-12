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
        ShutdownSignalException initialEx = new ShutdownSignalException(
            false,
            false,
            reason,
            ref);
        return new IOException(initialEx.sensibleClone());
    }
}
