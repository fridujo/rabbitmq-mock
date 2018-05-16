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
        Connection connection = new MockConnection(mockConnectionFactory.newConnection(), connectionListeners);
        connectionListeners.forEach(connectionListener -> connectionListener.onCreate(connection));
        return connection;
    }

    @Override
    public String getHost() {
        return "localtest";
    }

    @Override
    public int getPort() {
        return -1;
    }

    @Override
    public String getVirtualHost() {
        return null;
    }

    @Override
    public String getUsername() {
        return null;
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
