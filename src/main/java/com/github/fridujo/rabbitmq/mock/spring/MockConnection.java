package com.github.fridujo.rabbitmq.mock.spring;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;

import java.util.Set;

class MockConnection implements Connection {

    private final com.github.fridujo.rabbitmq.mock.MockConnection connection;
    private final Set<ConnectionListener> connectionListeners;

    MockConnection(com.github.fridujo.rabbitmq.mock.MockConnection mockConnection, Set<ConnectionListener> connectionListeners) {
        this.connection = mockConnection;
        this.connectionListeners = connectionListeners;
        this.connectionListeners.forEach(connectionListener -> connectionListener.onCreate(this));
    }

    @Override
    public Channel createChannel(boolean transactional) throws AmqpException {
        try {
            return connection.createChannel();
        } catch (AlreadyClosedException e) {
            throw RabbitExceptionTranslator.convertRabbitAccessException(e);
        }
    }

    @Override
    public void close() throws AmqpException {
        connection.close();
        connectionListeners.forEach(connectionListener -> connectionListener.onClose(this));
    }

    @Override
    public boolean isOpen() {
        return connection.isOpen();
    }

    @Override
    public int getLocalPort() {
        return 0;
    }

    @Override
    public void addBlockedListener(BlockedListener blockedListener) {
        connection.addBlockedListener(blockedListener);
    }

    @Override
    public boolean removeBlockedListener(BlockedListener blockedListener) {
        return connection.removeBlockedListener(blockedListener);
    }
}
