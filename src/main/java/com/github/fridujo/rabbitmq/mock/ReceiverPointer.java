package com.github.fridujo.rabbitmq.mock;

public record ReceiverPointer(Type type, String name) {
    public enum Type {
        QUEUE, EXCHANGE
    }
}
