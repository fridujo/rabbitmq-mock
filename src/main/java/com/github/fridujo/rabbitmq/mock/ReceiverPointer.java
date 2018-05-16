package com.github.fridujo.rabbitmq.mock;

class ReceiverPointer {
    final Type type;
    final String name;

    public ReceiverPointer(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    public enum Type {
        QUEUE, EXCHANGE;
    }
}
