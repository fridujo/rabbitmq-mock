package com.github.fridujo.rabbitmq.mock;

import java.util.Comparator;

public class MessageComparator implements Comparator<Message> {

    public MessageComparator(AmqArguments queueArguments) {

    }

    @Override
    public int compare(Message m1, Message m2) {
        return m1.id - m2.id;
    }
}
