package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import com.rabbitmq.client.impl.LongStringHelper;
import com.rabbitmq.client.impl.Version;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        softly.assertThat(connection.getServerProperties().get("version"))
            .isEqualTo(LongStringHelper.asLongString(new Version(AMQP.PROTOCOL.MAJOR, AMQP.PROTOCOL.MINOR).toString()));
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

    private static final String EXCHANGE = "exchange";
    private static final String QUEUE = "queue";

    @Test
    void exclusive_queue_cleanup() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, true);
        }

        try (Connection conn2 = factory.newConnection()) {
            assertThrows(IllegalArgumentException.class, () -> {
                readFromQueue(conn2);
            });
        }
    }

    @Test
    void autoDelete_queue_cleanup() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, false);
            BlockingQueue<String> queue = readFromQueue(conn);
            queue.poll(1000, TimeUnit.MILLISECONDS);
        }

        try (Connection conn2 = factory.newConnection()) {
            assertThrows(IllegalArgumentException.class, () -> {
                readFromQueue(conn2);
            });
        }
    }

    @Test
    void multiConnection_overlapping() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        Connection conn = factory.newConnection();

        try (Connection conn2 = factory.newConnection()) {
            createQueueAndPublish(conn2, false);
        }

        BlockingQueue<String> queue = readFromQueue(conn);
        assertNotNull(queue.poll(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    void multiConnection_sequential() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, false);
        }

        try (Connection conn2 = factory.newConnection()) {
            BlockingQueue<String> queue = readFromQueue(conn2);
            assertNotNull(queue.poll(1000, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    void singleConnection() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            createQueueAndPublish(conn, false);

            BlockingQueue<String> queue = readFromQueue(conn);
            assertNotNull(queue.poll(1000, TimeUnit.MILLISECONDS));
        }
    }

    private void createQueueAndPublish(Connection conn, boolean exclusive) throws IOException {
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(QUEUE, false, exclusive, true, Map.of());
        channel.queueBind(QUEUE, EXCHANGE, "");

        channel.basicPublish(EXCHANGE, "", new AMQP.BasicProperties(), "hello".getBytes());
    }

    @Test
    void throwKillsConsumer() throws Exception {
        MockConnectionFactory factory = new MockConnectionFactory();

        try (Connection conn = factory.newConnection()) {
            Channel channel = conn.createChannel();

            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
            channel.queueDeclare(QUEUE, false, true, true, Map.of());
            channel.queueBind(QUEUE, EXCHANGE, "");

            channel.basicPublish(EXCHANGE, "", new AMQP.BasicProperties(), "throw".getBytes());
            channel.basicPublish(EXCHANGE, "", new AMQP.BasicProperties(), "complete".getBytes());

            BlockingQueue<String> queue = readFromQueue(conn);
            assertEquals("throw", queue.poll(1000, TimeUnit.MILLISECONDS)); // nack'ed message
            assertEquals("throw", queue.poll(1000, TimeUnit.MILLISECONDS)); // requeue'ed message
            assertEquals("complete", queue.poll(1000, TimeUnit.MILLISECONDS)); // next message
        }
    }

    private BlockingQueue<String> readFromQueue(Connection conn) throws IOException {
        Channel channel = conn.createChannel();
        BlockingQueue<String> queue = new ArrayBlockingQueue<String>(3);

        Consumer consumer = new DefaultConsumer(channel) {
            boolean throwUpOnce = true;

            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                try {
                    queue.put(new String(body));
                } catch (InterruptedException e) {}

                if (throwUpOnce && new String(body).equals("throw")) {
                    throwUpOnce = false;
                    throw new RuntimeException("oops");
                }
            }
        };

        channel.basicConsume(QUEUE, true, consumer);
        return queue;
    }    
}
