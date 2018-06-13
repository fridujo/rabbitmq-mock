package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class MockConnectionTest {

    @Test
    void connectionParams_are_default_ones() {
        Connection connection = new MockConnectionFactory().newConnection();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(connection.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
        softly.assertThat(connection.getPort()).isEqualTo(ConnectionFactory.DEFAULT_AMQP_PORT);
        softly.assertThat(connection.getChannelMax()).isEqualTo(0);
        softly.assertThat(connection.getFrameMax()).isEqualTo(0);
        softly.assertThat(connection.getHeartbeat()).isEqualTo(0);
        softly.assertThat(connection.getClientProperties()).isEqualTo(AMQConnection.defaultClientProperties());
        softly.assertThat(connection.getClientProvidedName()).isNull();
        softly.assertThat(connection.getServerProperties()).isEmpty();
        softly.assertThat(connection.getExceptionHandler()).isExactlyInstanceOf(DefaultExceptionHandler.class);
        softly.assertAll();
    }

    @Test
    void close_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.close();
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void close_with_timeout_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.close(10);
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void abort_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.abort();
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void abort_with_timeout_closes_connection() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.abort(15);
            assertThat(connection.isOpen()).isFalse();
        }
    }

    @Test
    void blockedListeners_and_shutdown_listeners_are_not_stored() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            assertThat(connection.removeBlockedListener(null)).isTrue();
            assertThat(connection.addBlockedListener(null, null)).isNull();
            connection.clearBlockedListeners();
            connection.addShutdownListener(null);
            connection.removeShutdownListener(null);
            assertThat(connection.getCloseReason()).isNull();
        }
    }

    @Test
    void id_is_null_before_being_set() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            assertThat(connection.getId()).isNull();
            String id = UUID.randomUUID().toString();
            connection.setId(id);

            assertThat(connection.getId()).isEqualTo(id);
        }
    }

    @Test
    void createChannel_throws_when_connection_is_closed() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            connection.close();

            assertThatExceptionOfType(AlreadyClosedException.class)
                .isThrownBy(() -> connection.createChannel());
        }
    }

    @Test
    void protectedApiMethods_throw() throws IOException {
        try (Connection connection = new MockConnectionFactory().newConnection()) {
            assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> connection.notifyListeners());
        }
    }
}
