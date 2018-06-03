package com.github.fridujo.rabbitmq.mock.spring;

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

    private final com.github.fridujo.rabbitmq.mock.MockConnection connection;
    private final Set<ConnectionListener> connectionListeners;

    private AtomicBoolean opened = new AtomicBoolean(true);

    MockConnection(com.github.fridujo.rabbitmq.mock.MockConnection mockConnection, Set<ConnectionListener> connectionListeners) {
        this.connection = mockConnection;
        this.connectionListeners = connectionListeners;
    }

    @Override
    public Channel createChannel(boolean transactional) throws AmqpException {
        if (isOpen()) {
            return connection.createChannel();
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
        return 0;
    }

    @Override
    public void addBlockedListener(BlockedListener blockedListener) {
    }

    @Override
    public boolean removeBlockedListener(BlockedListener blockedListener) {
        return false;
    }
}
