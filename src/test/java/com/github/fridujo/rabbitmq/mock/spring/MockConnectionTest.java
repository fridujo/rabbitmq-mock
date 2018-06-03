package com.github.fridujo.rabbitmq.mock.spring;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedListener;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.SimpleConnection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

class MockConnectionTest {

    @Test
    void blockedListeners_are_not_stored() {
        Connection connection = new MockConnectionFactory().createConnection();
        BlockedListener blockedListener = mock(BlockedListener.class);

        connection.addBlockedListener(blockedListener);

        assertThat(connection.removeBlockedListener(blockedListener)).isFalse();
    }

    @Test
    void localPort_is_the_default_used_when_underlying_connection_does_not_support_it() {
        Connection connection = new MockConnectionFactory().createConnection();
        com.rabbitmq.client.Connection rabbitmqConnection = mock(com.rabbitmq.client.Connection.class);
        assertThat(connection.getLocalPort()).isEqualTo(new SimpleConnection(rabbitmqConnection, -1).getLocalPort());
    }

    @Test
    void createChannel_whereas_connection_is_closed_throws() {
        Connection connection = new MockConnectionFactory().createConnection();
        connection.close();
        assertThatExceptionOfType(AmqpConnectException.class)
            .isThrownBy(() -> connection.createChannel(false))
            .withCauseExactlyInstanceOf(AlreadyClosedException.class)
            .withMessageContaining("channel is already closed due to clean channel shutdown");
    }
}
