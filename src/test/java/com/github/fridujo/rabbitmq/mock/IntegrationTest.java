package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class IntegrationTest {

    @Test
    void basic_consume_case() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        ConnectionFactory factory = new MockConnectionFactory();

        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setHost("localhost");
        factory.setPort(5672);

        try (Connection conn = factory.newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                List<String> messages = new ArrayList<>();
                boolean autoAck = false;
                channel.basicConsume(queueName, autoAck, "myConsumerTag",
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body)
                            throws IOException {
                            String routingKey = envelope.getRoutingKey();
                            String contentType = properties.getContentType();
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
    void basic_get_case() throws IOException, TimeoutException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        ConnectionFactory factory = new MockConnectionFactory();

        factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");

        try (Connection conn = factory.newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

                boolean autoAck = false;
                GetResponse response = channel.basicGet(queueName, autoAck);
                if (response == null) {
                    fail("AMQP GetReponse must not be null");
                } else {
                    AMQP.BasicProperties props = response.getProps();
                    byte[] body = response.getBody();
                    assertThat(new String(body)).isEqualTo("Hello, world!");
                    long deliveryTag = response.getEnvelope().getDeliveryTag();

                    channel.basicAck(deliveryTag, false);
                }
            }
        }
    }
}
