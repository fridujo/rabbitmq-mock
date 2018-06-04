package com.github.fridujo.rabbitmq.mock.spring;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class MockConnectionFactory implements ConnectionFactory {

    private final com.github.fridujo.rabbitmq.mock.MockConnectionFactory mockConnectionFactory = new com.github.fridujo.rabbitmq.mock.MockConnectionFactory();
    private final Set<ConnectionListener> connectionListeners = new CopyOnWriteArraySet<>();

    @Override
    public Connection createConnection() throws AmqpException {
        return new MockConnection(mockConnectionFactory.newConnection(), connectionListeners);
    }

    @Override
    public String getHost() {
        return com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;
    }

    @Override
    public int getPort() {
        return com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT;
    }

    @Override
    public String getVirtualHost() {
        return com.rabbitmq.client.ConnectionFactory.DEFAULT_VHOST;
    }

    @Override
    public String getUsername() {
        return com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        connectionListeners.add(listener);
    }

    @Override
    public boolean removeConnectionListener(ConnectionListener listener) {
        return connectionListeners.remove(listener);
    }

    @Override
    public void clearConnectionListeners() {
        connectionListeners.clear();
    }
}
