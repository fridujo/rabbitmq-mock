package com.github.fridujo.rabbitmq.mock;

import static com.github.fridujo.rabbitmq.mock.configuration.QueueDeclarator.queue;
import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndEatExceptions;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

class ComplexUseCasesTests {

    @Test
    void expired_message_should_be_consumable_after_being_dead_lettered() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBindNoWait("rejected", "rejected-ex", "unused", null);
                queue("fruits").withMessageTtl(10L).withDeadLetterExchange("rejected-ex").declare(channel);

                List<String> messages = new ArrayList<>();
                channel.basicConsume("rejected", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.add(new String(body));
                    }
                });

                channel.basicPublish("", "fruits", null, "banana".getBytes());
                TimeUnit.MILLISECONDS.sleep(100L);
                assertThat(messages).hasSize(1);
            }
        }
    }

    @Test
    void multiple_expired_messages_are_not_delivered_to_consumer() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                queue("fruits").withMessageTtl(-1L).declare(channel);

                List<String> messages = new ArrayList<>();
                channel.basicConsume("fruits", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.add(new String(body));
                    }
                });

                channel.basicPublish("", "fruits", null, "banana".getBytes());
                channel.basicPublish("", "fruits", null, "orange".getBytes());
                TimeUnit.MILLISECONDS.sleep(100L);
                assertThat(messages).hasSize(0);
            }
        }
    }

    @Test
    void expired_message_should_be_consumable_after_being_dead_lettered_with_ttl_per_message() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                queue("rejected").declare(channel);
                channel.queueBindNoWait("rejected", "rejected-ex", "unused", null);
                queue("fruits").withDeadLetterExchange("rejected-ex").declare(channel);

                List<String> messages = new ArrayList<>();
                channel.basicConsume("rejected", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.add(new String(body));
                    }
                });

                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .expiration("50")
                    .build();
                channel.basicPublish("", "fruits", properties, "banana".getBytes());
                TimeUnit.MILLISECONDS.sleep(150L);
                assertThat(messages).hasSize(1);
            }
        }
    }

    @Test
    void basicGet_expired_message_while_consumer_is_active() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            Semaphore consuming = new Semaphore(0);
            try (Channel channel = conn.createChannel()) {
                queue("fruits").declare(channel);

                channel.basicConsume("fruits", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        runAndEatExceptions(consuming::acquire); // will consume banana
                    }
                });

                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .expiration("50")
                    .build();
                channel.basicPublish("", "fruits", null, "banana".getBytes());
                channel.basicPublish("", "fruits", properties, "kiwi".getBytes());

                TimeUnit.MILLISECONDS.sleep(80L);

                GetResponse kiwiMessage = channel.basicGet("fruits", true);
                assertThat(kiwiMessage).isNull(); // dead-lettered but not by the queue event-loop
                consuming.release();
            }
        }
    }

    @Test
    void can_consume_messages_published_in_a_previous_connection() throws InterruptedException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (MockConnection conn = connectionFactory.newConnection()) {
            try (MockChannel channel = conn.createChannel()) {
                queue("numbers").declare(channel);
                Arrays.asList("one", "two", "three").stream().forEach(message ->
                    channel.basicPublish("", "numbers", null, message.getBytes())
                );
            }
        }

        try (MockConnection conn = connectionFactory.newConnection()) {
            try (MockChannel channel = conn.createChannel()) {

                List<String> messages = new ArrayList<>();
                Semaphore deliveries = new Semaphore(-2);

                channel.basicConsume("numbers", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.add(new String(body));
                        deliveries.release();
                    }
                });

                assertThat(deliveries.tryAcquire(1, TimeUnit.SECONDS)).as("Messages have been delivered").isTrue();

                assertThat(messages).containsExactly("one", "two", "three");
            }
        }
    }
}
