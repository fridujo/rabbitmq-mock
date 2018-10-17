package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class IntegrationTest {

    @Test
    void basic_consume_case() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        try (Connection conn = new MockConnectionFactory().newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                List<String> messages = new ArrayList<>();
                channel.basicConsume(queueName, false, "myConsumerTag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body) throws IOException {
                            long deliveryTag = envelope.getDeliveryTag();
                            messages.add(new String(body));
                            // (process the message components here ...)
                            channel.basicAck(deliveryTag, false);
                        }
                    });

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

                TimeUnit.MILLISECONDS.sleep(200L);

                assertThat(messages).containsExactly("Hello, world!");
            }
        }
    }

    @Test
    void basic_get_case() throws IOException, TimeoutException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        try (Connection conn = new MockConnectionFactory().newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

                GetResponse response = channel.basicGet(queueName, false);
                if (response == null) {
                    fail("AMQP GetReponse must not be null");
                } else {
                    byte[] body = response.getBody();
                    assertThat(new String(body)).isEqualTo("Hello, world!");
                    long deliveryTag = response.getEnvelope().getDeliveryTag();

                    channel.basicAck(deliveryTag, false);
                }
            }
        }
    }

    @Test
    void basic_consume_nack_case() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        AtomicInteger atomicInteger = new AtomicInteger();
        final Semaphore semaphore = new Semaphore(0);

        try (Connection conn = new MockConnectionFactory().newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                channel.basicConsume(queueName, false, "myConsumerTag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body) throws IOException {
                            semaphore.release();
                            long deliveryTag = envelope.getDeliveryTag();
                            atomicInteger.incrementAndGet();
                            channel.basicNack(deliveryTag, false, true);
                        }
                    });

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
                semaphore.acquire();

            }

        }

        // WHEN after closing the connection and resetting the counter
        atomicInteger.set(0);

        TimeUnit.MILLISECONDS.sleep(100L);
        assertThat(atomicInteger.get())
            .describedAs("even after a few milliseconds, the consumer should not have been invoked on the message")
            .isZero();


    }
}
