package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;

public class ConsumerWrapper implements Consumer {
    private final DeliverCallback deliverCallback;
    private final CancelCallback cancelCallback;
    private final ConsumerShutdownSignalCallback shutdownSignalCallback;

    public ConsumerWrapper(DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        this.deliverCallback = deliverCallback;
        this.cancelCallback = cancelCallback;
        this.shutdownSignalCallback = shutdownSignalCallback;
    }

    @Override
    public void handleConsumeOk(String consumerTag) {
    }

    @Override
    public void handleCancelOk(String consumerTag) {
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        if (cancelCallback != null) {
            cancelCallback.handle(consumerTag);
        }
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        if (shutdownSignalCallback != null) {
            shutdownSignalCallback.handleShutdownSignal(consumerTag, sig);
        }
    }

    @Override
    public void handleRecoverOk(String consumerTag) {

    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        deliverCallback.handle(consumerTag, new Delivery(envelope, properties, body));
    }
}
