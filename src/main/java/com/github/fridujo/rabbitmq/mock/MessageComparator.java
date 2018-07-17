package com.github.fridujo.rabbitmq.mock;

import java.util.Comparator;

import static java.lang.Math.min;

public class MessageComparator implements Comparator<Message> {

    private final AmqArguments queueArguments;

    public MessageComparator(AmqArguments queueArguments) {
        this.queueArguments = queueArguments;
    }

    @Override
    public int compare(Message m1, Message m2) {
        int priorityComparison = queueArguments.queueMaxPriority()
            .map(max -> min(max, m2.priority()) - min(max, m1.priority()))
            .orElse(0);
        int publicationOrderComparison = m1.id - m2.id;

        return priorityComparison != 0 ? priorityComparison : publicationOrderComparison;
    }
}
