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
            publishedMessage.exchange,
            publishedMessage.routingKey,
            publishedMessage.mandatory,
            publishedMessage.immediate,
            publishedMessage.props,
            publishedMessage.body
        ));
        publishedMessages.clear();

        rejects.forEach(reject -> mockNode.basicReject(reject.deliveryTag, reject.requeue));
        rejects.clear();

        nacks.forEach(nack -> mockNode.basicNack(nack.deliveryTag, nack.multiple, nack.requeue));
        nacks.clear();

        acks.forEach(ack -> mockNode.basicAck(ack.deliveryTag, ack.multiple));
        acks.clear();
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        publishedMessages.add(new PublishedMessage(exchange, routingKey, mandatory, immediate, props, body));
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

    private static class PublishedMessage {
        private final String exchange;
        private final String routingKey;
        private final boolean mandatory;
        private final boolean immediate;
        private final AMQP.BasicProperties props;
        private final byte[] body;

        private PublishedMessage(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.mandatory = mandatory;
            this.immediate = immediate;
            this.props = props;
            this.body = body;
        }
    }

    private static class Reject {
        private final long deliveryTag;
        private final boolean requeue;

        private Reject(long deliveryTag, boolean requeue) {
            this.deliveryTag = deliveryTag;
            this.requeue = requeue;
        }
    }

    private static class Nack {
        private final long deliveryTag;
        private final boolean multiple;
        private final boolean requeue;

        private Nack(long deliveryTag, boolean multiple, boolean requeue) {
            this.deliveryTag = deliveryTag;
            this.multiple = multiple;
            this.requeue = requeue;
        }
    }

    private static class Ack {
        private final long deliveryTag;
        private final boolean multiple;

        private Ack(long deliveryTag, boolean multiple) {
            this.deliveryTag = deliveryTag;
            this.multiple = multiple;
        }
    }
}
