package com.github.fridujo.rabbitmq.mock.spring;

import com.rabbitmq.client.ShutdownSignalException;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;

import static org.assertj.core.api.Assertions.assertThat;

class MockConnectionFactoryTest {

    @Test
    void connectionParams_are_default_ones() {
        ConnectionFactory connectionFactory = new MockConnectionFactory();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(connectionFactory.getHost()).isEqualTo("localhost");
        softly.assertThat(connectionFactory.getPort()).isEqualTo(5672);
        softly.assertThat(connectionFactory.getVirtualHost()).isEqualTo("/");
        softly.assertThat(connectionFactory.getUsername()).isEqualTo("guest");
        softly.assertAll();
    }

    @Test
    void connectionListener_is_notified_when_connection_is_created_then_closed() {
        ConnectionFactory connectionFactory = new MockConnectionFactory();

        TestConnectionListener connectionListener = new TestConnectionListener();
        connectionFactory.addConnectionListener(connectionListener);

        assertThat(connectionListener.created).isFalse();
        assertThat(connectionListener.closed).isFalse();

        Connection connection = connectionFactory.createConnection();
        assertThat(connectionListener.created).isTrue();
        assertThat(connectionListener.closed).isFalse();

        connection.close();
        assertThat(connectionListener.created).isTrue();
        assertThat(connectionListener.closed).isTrue();
    }

    @Test
    void notifications_are_not_sent_if_listener_is_removed() {
        ConnectionFactory connectionFactory = new MockConnectionFactory();

        TestConnectionListener connectionListener = new TestConnectionListener();
        connectionFactory.addConnectionListener(connectionListener);

        Connection connection = connectionFactory.createConnection();
        assertThat(connectionListener.created).isTrue();
        assertThat(connectionListener.closed).isFalse();

        assertThat(connectionFactory.removeConnectionListener(connectionListener)).isTrue();

        connection.close();
        assertThat(connectionListener.created).isTrue();
        assertThat(connectionListener.closed).isFalse();
    }

    @Test
    void notifications_are_not_sent_if_listeners_are_cleared() {
        ConnectionFactory connectionFactory = new MockConnectionFactory();

        TestConnectionListener connectionListener = new TestConnectionListener();
        connectionFactory.addConnectionListener(connectionListener);

        connectionFactory.clearConnectionListeners();

        Connection connection = connectionFactory.createConnection();
        assertThat(connectionListener.created).isFalse();

        connection.close();
        assertThat(connectionListener.closed).isFalse();
    }

    private static class TestConnectionListener implements ConnectionListener {

        private boolean created = false;
        private boolean closed = false;

        @Override
        public void onCreate(Connection connection) {
            created = !created;
        }

        @Override
        public void onClose(Connection connection) {
            closed = !closed;
        }

        @Override
        public void onShutDown(ShutdownSignalException signal) {

        }
    }
}
