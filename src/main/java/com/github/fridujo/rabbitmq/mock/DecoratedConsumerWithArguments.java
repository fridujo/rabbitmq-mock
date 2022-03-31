package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.Map;

public class DecoratedConsumerWithArguments implements ConsumerWithArguments {

    final private Consumer consumer;
    final private Map<String, Object> arguments;

    DecoratedConsumerWithArguments(Consumer consumer, Map<String, Object> arguments) {
        this.consumer = consumer;
        this.arguments = arguments;
    }

    @Override
    public Map<String, Object> getArguments() { return arguments; }

    @Override
    public void handleConsumeOk(String consumerTag) { consumer.handleConsumeOk(consumerTag); }

    @Override
    public void handleCancelOk(String consumerTag) { consumer.handleCancelOk(consumerTag); }

    @Override
    public void handleCancel(String consumerTag) throws IOException { consumer.handleCancel(consumerTag); }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) { consumer.handleShutdownSignal(consumerTag, sig); }

    @Override
    public void handleRecoverOk(String consumerTag) { consumer.handleRecoverOk(consumerTag); }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException { consumer.handleDelivery(consumerTag, envelope, properties, body); }
}
