package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;

import java.time.Instant;
import java.util.Optional;

public class Message {

    public final int id;
    public final String exchangeName;
    public final String routingKey;
    public final AMQP.BasicProperties props;
    public final byte[] body;
    public final long expiryTime;
    public final boolean redelivered;

    public Message(int id, String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body, long expiryTime) {
        this(id, exchangeName, routingKey, props, body, expiryTime, false);
    }

    private Message(int id, String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body, long expiryTime, boolean redelivered) {
        this.id = id;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
        this.props = props;
        this.body = body;
        this.expiryTime = expiryTime;
        this.redelivered = redelivered;
    }

    public Message asRedelivered() {
        return new Message(id, exchangeName, routingKey, props, body, expiryTime, true);
    }

    public boolean isExpired() {
        return expiryTime > -1 && System.currentTimeMillis() > expiryTime;
    }

    public int priority() {
        return Optional.ofNullable(props.getPriority()).orElse(0);
    }

    @Override
    public String toString() {
        return "Message{" +
            "exchangeName='" + exchangeName + '\'' +
            ", routingKey='" + routingKey + '\'' +
            ", body=" + new String(body) +
            ", expiryTime=" + Instant.ofEpochMilli(expiryTime) +
            '}';
    }
}
