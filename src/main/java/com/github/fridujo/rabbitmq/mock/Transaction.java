package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

import java.util.ArrayList;
import java.util.List;

public class Transaction implements TransactionalOperations {
    private final MockNode mockNode;
    private final List<PublishedMessage> publishedMessages = new ArrayList<>();
    private final List<Reject> rejects = new ArrayList<>();
    private final List<Nack> nacks = new ArrayList<>();
    private final List<Ack> acks = new ArrayList<>();

    public Transaction(MockNode mockNode) {
        this.mockNode = mockNode;
    }

    public void commit() {
        publishedMessages.forEach(publishedMessage -> mockNode.basicPublish(
            publishedMessage.exchange(),
            publishedMessage.routingKey(),
            publishedMessage.mandatory(),
            publishedMessage.immediate(),
            publishedMessage.props(),
            publishedMessage.body()
        ));
        publishedMessages.clear();

        rejects.forEach(reject -> mockNode.basicReject(reject.deliveryTag(), reject.requeue()));
        rejects.clear();

        nacks.forEach(nack -> mockNode.basicNack(nack.deliveryTag(), nack.multiple(), nack.requeue()));
        nacks.clear();

        acks.forEach(ack -> mockNode.basicAck(ack.deliveryTag(), ack.multiple()));
        acks.clear();
    }

    @Override
    public boolean basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        publishedMessages.add(new PublishedMessage(exchange, routingKey, mandatory, immediate, props, body));
        return true;//behavior is not defined in spec
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) {
        rejects.add(new Reject(deliveryTag, requeue));
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        nacks.add(new Nack(deliveryTag, multiple, requeue));
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) {
        acks.add(new Ack(deliveryTag, multiple));
    }

    private static record PublishedMessage(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {}

    private static record Reject(long deliveryTag, boolean requeue) {}

    private static record Nack(long deliveryTag, boolean multiple, boolean requeue) {}

    private static record Ack(long deliveryTag, boolean multiple) {}

}
