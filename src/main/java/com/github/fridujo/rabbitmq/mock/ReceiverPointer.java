package com.github.fridujo.rabbitmq.mock;

import java.util.Objects;

class ReceiverPointer {
    final Type type;
    final String name;

    public ReceiverPointer(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReceiverPointer that = (ReceiverPointer) o;
        return type == that.type &&
            Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }

    public enum Type {
        QUEUE, EXCHANGE;
    }
}
