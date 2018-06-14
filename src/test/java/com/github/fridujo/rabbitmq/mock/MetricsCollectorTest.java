package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.MicrometerMetricsCollector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricsCollectorTest {

    @Test
    void metrics_collector_is_invoked_on_connection_creation_and_closing() {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        assertThat(registry.get("rabbitmq.connections").gauge().value()).isEqualTo(0);
        try (MockConnection connection = mockConnectionFactory.newConnection()) {
            assertThat(registry.get("rabbitmq.connections").gauge().value()).isEqualTo(1);
        }
        assertThat(registry.get("rabbitmq.connections").gauge().value()).isEqualTo(0);
    }

    @Test
    void metrics_collector_is_invoked_on_channel_creation_and_closing() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection()) {
            assertThat(registry.get("rabbitmq.channels").gauge().value()).isEqualTo(0);
            try (Channel channel = connection.createChannel(42)) {
                assertThat(registry.get("rabbitmq.channels").gauge().value()).isEqualTo(1);
            }
            assertThat(registry.get("rabbitmq.channels").gauge().value()).isEqualTo(0);
        }
    }

    @Test
    void metrics_collector_is_invoked_on_message_published() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {
            assertThat(registry.get("rabbitmq.published").counter().count()).isEqualTo(0);
            channel.basicPublish("", "", null, "".getBytes());
            assertThat(registry.get("rabbitmq.published").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void metrics_collector_is_invoked_on_basic_get_consumption() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {
            String queueName = channel.queueDeclare().getQueue();
            channel.basicPublish("", queueName, null, "".getBytes());

            assertThat(registry.get("rabbitmq.consumed").counter().count()).isEqualTo(0);
            channel.basicGet(queueName, true);
            assertThat(registry.get("rabbitmq.consumed").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void metrics_collector_is_invoked_on_basic_ack() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {
            String queueName = channel.queueDeclare().getQueue();
            channel.basicPublish("", queueName, null, "".getBytes());
            GetResponse getResponse = channel.basicGet(queueName, false);

            assertThat(registry.get("rabbitmq.acknowledged").counter().count()).isEqualTo(0);
            channel.basicAck(getResponse.getEnvelope().getDeliveryTag(), false);
            assertThat(registry.get("rabbitmq.acknowledged").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void metrics_collector_is_invoked_on_basic_nack() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {
            String queueName = channel.queueDeclare().getQueue();
            channel.basicPublish("", queueName, null, "".getBytes());
            GetResponse getResponse = channel.basicGet(queueName, false);

            assertThat(registry.get("rabbitmq.rejected").counter().count()).isEqualTo(0);
            channel.basicNack(getResponse.getEnvelope().getDeliveryTag(), false, false);
            assertThat(registry.get("rabbitmq.rejected").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void metrics_collector_is_invoked_on_basic_reject() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {
            String queueName = channel.queueDeclare().getQueue();
            channel.basicPublish("", queueName, null, "".getBytes());
            GetResponse getResponse = channel.basicGet(queueName, false);

            assertThat(registry.get("rabbitmq.rejected").counter().count()).isEqualTo(0);
            channel.basicReject(getResponse.getEnvelope().getDeliveryTag(), false);
            assertThat(registry.get("rabbitmq.rejected").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void metrics_collector_is_invoked_on_consumer_consumption() throws IOException, TimeoutException, InterruptedException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {
            String queueName = channel.queueDeclare().getQueue();
            Semaphore semaphore = new Semaphore(0);
            channel.basicConsume("", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body) {
                    semaphore.release();
                }

                @Override
                public void handleCancelOk(String consumerTag) {
                }
            });
            assertThat(registry.get("rabbitmq.consumed").counter().count()).isEqualTo(0);
            channel.basicPublish("", queueName, null, "".getBytes());
            semaphore.acquire();
            assertThat(registry.get("rabbitmq.consumed").counter().count()).isEqualTo(1);
        }
    }

    @Test
    void metrics_collector_reference_the_last_set_in_connection_factory() throws IOException, TimeoutException {
        MockConnectionFactory mockConnectionFactory = new MockConnectionFactory();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        try (MockConnection connection = mockConnectionFactory.newConnection();
             Channel channel = connection.createChannel(42)) {

            mockConnectionFactory.setMetricsCollector(new MicrometerMetricsCollector(registry));

            String queueName = channel.queueDeclare().getQueue();
            channel.basicPublish("", queueName, null, "".getBytes());
            assertThat(registry.get("rabbitmq.published").counter().count()).isEqualTo(1);
        }
    }
}
