package com.github.fridujo.rabbitmq.mock;

import static com.github.fridujo.rabbitmq.mock.configuration.QueueDeclarator.queue;
import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndEatExceptions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Test;

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
        final Semaphore waitForAtLeastOneDelivery = new Semaphore(0);
        final Semaphore waitForCancellation = new Semaphore(0);

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
                            waitForAtLeastOneDelivery.release();
                            long deliveryTag = envelope.getDeliveryTag();
                            atomicInteger.incrementAndGet();
                            channel.basicNack(deliveryTag, false, true);
                        }

                        @Override
                        public void handleCancel(String consumerTag) {
                            waitForCancellation.release();
                        }
                    });

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
                waitForAtLeastOneDelivery.acquire();
            }
        }

        // WHEN after closing the connection and resetting the counter
        atomicInteger.set(0);

        waitForCancellation.acquire();
        assertThat(atomicInteger.get())
            .describedAs("After connection closed, and Consumer cancellation, no message should be delivered anymore")
            .isZero();
    }

    @Test
    void redelivered_message_should_have_redelivery_marked_as_true() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            CountDownLatch messagesToBeProcessed = new CountDownLatch(2);
            try (Channel channel = conn.createChannel()) {
                queue("fruits").declare(channel);
                AtomicReference<Envelope> redeliveredMessageEnvelope = new AtomicReference();

                channel.basicConsume("fruits", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        if(messagesToBeProcessed.getCount() == 1){
                            redeliveredMessageEnvelope.set(envelope);
                            runAndEatExceptions(messagesToBeProcessed::countDown);

                        }else{
                            runAndEatExceptions(() -> channel.basicNack(envelope.getDeliveryTag(), false, true));
                            runAndEatExceptions(messagesToBeProcessed::countDown);
                        }

                    }
                });

                channel.basicPublish("", "fruits", null, "banana".getBytes());

                final boolean finishedProperly = messagesToBeProcessed.await(1000, TimeUnit.SECONDS);
                assertThat(finishedProperly).isTrue();
                assertThat(redeliveredMessageEnvelope.get()).isNotNull();
                assertThat(redeliveredMessageEnvelope.get().isRedeliver()).isTrue();
            }
        }
    }

    @Test
    void basic_consume_with_multiple_bindings_should_receive_messages_via_all_bindings() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "test-exchange";

        try (Connection conn = new MockConnectionFactory().newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "headers", true);
                String queueName = channel.queueDeclare().getQueue();

                // Create multiple bindings that only differ in header parameter value:
                channel.queueBind(queueName, exchangeName, "", args("my-header", "x", "x-match", "all"));
                channel.queueBind(queueName, exchangeName, "", args("my-header", "y", "x-match", "all"));
                channel.queueBind(queueName, exchangeName, "", args("my-header", "z", "x-match", "all"));

                List<String> messages = new ArrayList<>();
                channel.basicConsume(queueName, false, "",
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

                // publish messages that only differ in header parameter value:
                channel.basicPublish(exchangeName, "", new AMQP.BasicProperties.Builder().headers(args("my-header", "x")).build(), "Hello, world!".getBytes());
                channel.basicPublish(exchangeName, "", new AMQP.BasicProperties.Builder().headers(args("my-header", "y")).build(), "How are you, world!".getBytes());
                channel.basicPublish(exchangeName, "", new AMQP.BasicProperties.Builder().headers(args("my-header", "z")).build(), "Goodbye, world!".getBytes());

                TimeUnit.MILLISECONDS.sleep(200L);

                // expect three messages:
                assertThat(messages).containsExactly("Hello, world!", "How are you, world!", "Goodbye, world!");

                // remove one of the bindings:
                channel.queueUnbind(queueName, exchangeName, "", args("my-header", "y", "x-match", "all"));
                messages.clear();

                // publish messages that only differ in header parameter value:
                channel.basicPublish(exchangeName, "", new AMQP.BasicProperties.Builder().headers(args("my-header", "x")).build(), "Hello, world!".getBytes());
                channel.basicPublish(exchangeName, "", new AMQP.BasicProperties.Builder().headers(args("my-header", "y")).build(), "How are you, world!".getBytes());
                channel.basicPublish(exchangeName, "", new AMQP.BasicProperties.Builder().headers(args("my-header", "z")).build(), "Goodbye, world!".getBytes());

                TimeUnit.MILLISECONDS.sleep(200L);

                // expect two messages from remaining bindings:
                assertThat(messages).containsExactly("Hello, world!", "Goodbye, world!");
            }
        }
    }

    private static Map<String, Object> args(Object... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("must provide arguments in pairs");
        }

        Map<String, Object> map = new HashMap<>();

        for (int i = 0; i < args.length; i += 2) {
            if (!(args[i] instanceof String)) {
                throw new IllegalArgumentException("argument " + i + " must be a String: " + args[i]);
            }

            map.put((String)args[i], args[i + 1]);
        }

        return map;
    }
}
