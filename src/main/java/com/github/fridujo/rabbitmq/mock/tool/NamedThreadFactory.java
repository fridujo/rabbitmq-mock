package com.github.fridujo.rabbitmq.mock.tool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class NamedThreadFactory implements ThreadFactory {

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final Function<Integer, String> nameCreator;

    public NamedThreadFactory(Function<Integer, String> nameCreator) {
        this.nameCreator = nameCreator;
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(group, r, nameCreator.apply(threadNumber.getAndIncrement()), 0);
    }
}
