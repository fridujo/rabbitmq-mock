package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;

import java.io.IOException;

public class ConfirmListenerWrapper implements ConfirmListener {
    private final ConfirmCallback ackCallback;
    private final ConfirmCallback nackCallback;

    public ConfirmListenerWrapper(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
        this.ackCallback = ackCallback;
        this.nackCallback = nackCallback;
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        ackCallback.handle(deliveryTag, multiple);
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        nackCallback.handle(deliveryTag, multiple);
    }
}
