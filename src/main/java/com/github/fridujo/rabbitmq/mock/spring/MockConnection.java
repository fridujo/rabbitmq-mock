package com.github.fridujo.rabbitmq.mock.spring;

import com.github.fridujo.rabbitmq.mock.MockChannel;
import com.github.fridujo.rabbitmq.mock.MockNode;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

class MockConnection implements Connection {

    private final MockNode node;
    private final Set<ConnectionListener> connectionListeners;

    private AtomicBoolean opened = new AtomicBoolean(true);

    MockConnection(MockNode node, Set<ConnectionListener> connectionListeners) {
        this.node = node;
        this.connectionListeners = connectionListeners;
    }

    @Override
    public Channel createChannel(boolean transactional) throws AmqpException {
        if (opened.get()) {
            return new MockChannel(node);
        } else {
            throw RabbitExceptionTranslator.convertRabbitAccessException(
                new AlreadyClosedException(new ShutdownSignalException(false, true, null, this)));
        }
    }

    @Override
    public void close() throws AmqpException {
        opened.getAndSet(false);
        connectionListeners.forEach(connectionListener -> connectionListener.onClose(this));
    }

    @Override
    public boolean isOpen() {
        return opened.get();
    }

    @Override
    public int getLocalPort() {
        return -1;
    }

    @Override
    public void addBlockedListener(BlockedListener blockedListener) {
    }

    @Override
    public boolean removeBlockedListener(BlockedListener blockedListener) {
        return false;
    }
}
