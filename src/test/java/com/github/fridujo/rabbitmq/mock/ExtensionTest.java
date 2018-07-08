package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class ExtensionTest {

    @Test
    void alternate_exchange_is_used_when_routing_fails() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("ex-1", BuiltinExchangeType.TOPIC, true, false, Collections.singletonMap("alternate-exchange", "ex-2"));
                channel.exchangeDeclare("ex-2", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("fruits", true, false, false, Collections.emptyMap());
                channel.queueDeclare("unrouted", true, false, false, Collections.emptyMap());

                assertThat(channel.queueBind("fruits", "ex-1", "fruit.*")).isNotNull();
                assertThat(channel.queueBind("unrouted", "ex-2", "")).isNotNull();

                channel.basicPublish("ex-1", "vegetable.carrot", null, "carrot".getBytes());
                channel.basicPublish("ex-1", "fruit.orange", null, "orange".getBytes());

                GetResponse response = channel.basicGet("fruits", true);
                assertThat(response).isNotNull();
                assertThat(new String(response.getBody())).isEqualTo("orange");
                assertThat(channel.basicGet("fruits", true)).isNull();

                response = channel.basicGet("unrouted", true);
                assertThat(response).isNotNull();
                assertThat(new String(response.getBody())).isEqualTo("carrot");
                assertThat(channel.basicGet("unrouted", true)).isNull();
            }
        }
    }

    @Test
    void dead_letter_exchange_is_used_when_a_message_is_rejected_without_requeue() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBindNoWait("rejected", "rejected-ex", "unused", null);
                channel.queueDeclare("fruits", true, false, false, Collections.singletonMap("x-dead-letter-exchange", "rejected-ex"));

                channel.basicPublish("", "fruits", null, "banana".getBytes());
                GetResponse getResponse = channel.basicGet("fruits", false);
                channel.basicNack(getResponse.getEnvelope().getDeliveryTag(), false, true);
                assertThat(channel.messageCount("rejected")).isEqualTo(0);
                assertThat(channel.messageCount("fruits")).isEqualTo(1);

                getResponse = channel.basicGet("fruits", false);
                channel.basicReject(getResponse.getEnvelope().getDeliveryTag(), false);
                assertThat(channel.messageCount("rejected")).isEqualTo(1);
                assertThat(channel.messageCount("fruits")).isEqualTo(0);
            }
        }
    }

    @Nested
    class TimeToLiveExtension {

        @Test
        void message_ttl_for_queue_keeps_messages_while_expiration_not_reached() throws IOException, TimeoutException {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    Map<String, Object> args = new HashMap<>();
                    args.put("x-message-ttl", 700);
                    channel.queueDeclare("fruits", true, false, false, args);
                    channel.basicPublish("", "fruits", null, "banana".getBytes());
                    GetResponse getResponse = channel.basicGet("fruits", true);
                    assertThat(getResponse).isNotNull();
                }
            }
        }

        @Test
        void message_ttl_for_queue_reject_messages_after_expiration_is_reached() throws IOException, TimeoutException, InterruptedException {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                    channel.queueDeclare("rejected", true, false, false, null);
                    channel.queueBindNoWait("rejected", "rejected-ex", "unused", null);

                    Map<String, Object> args = new HashMap<>();
                    args.put("x-message-ttl", 200);
                    args.put("x-dead-letter-exchange", "rejected-ex");
                    channel.queueDeclare("fruits", true, false, false, args);
                    channel.basicPublish("", "fruits", null, "banana".getBytes());
                    TimeUnit.MILLISECONDS.sleep(400L);
                    GetResponse getResponse = channel.basicGet("fruits", true);
                    assertThat(getResponse).isNull();

                    getResponse = channel.basicGet("rejected", true);
                    assertThat(getResponse).isNotNull();
                }
            }
        }

        @Test
        void message_ttl_for_queue_reject_messages_after_expiration_is_reached_using_consumer() throws IOException, TimeoutException, InterruptedException {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {

                    Map<String, Object> args = new HashMap<>();
                    args.put("x-message-ttl", 100);
                    channel.queueDeclare("fruits", true, false, false, args);
                    channel.basicPublish("", "fruits", null, "banana".getBytes());
                    TimeUnit.MILLISECONDS.sleep(300L);

                    List<String> messages = new ArrayList<>();
                    channel.basicConsume("fruits", new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body) {
                            messages.add(new String(body));
                        }

                        @Override
                        public void handleCancelOk(String consumerTag) {
                            // Not the purpose of the test
                        }
                    });

                    TimeUnit.MILLISECONDS.sleep(120L);

                    assertThat(messages).isEmpty();
                }
            }
        }

        @Test
        void non_long_message_ttl_for_queue_is_not_used() throws IOException, TimeoutException, InterruptedException {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    Map<String, Object> args = new HashMap<>();
                    args.put("x-message-ttl", "test");
                    channel.queueDeclare("fruits", true, false, false, args);
                    channel.basicPublish("", "fruits", null, "banana".getBytes());
                    TimeUnit.MILLISECONDS.sleep(100L);
                    GetResponse getResponse = channel.basicGet("fruits", true);
                    assertThat(getResponse).isNotNull();
                }
            }
        }

        @Test
        void message_ttl_in_publishers_reject_messages_after_expiration_is_reached() throws IOException, TimeoutException, InterruptedException {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    channel.queueDeclare("fruits", true, false, false, null);
                    AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .expiration("200")
                        .build();
                    channel.basicPublish("", "fruits", properties, "banana".getBytes());
                    TimeUnit.MILLISECONDS.sleep(400L);
                    GetResponse getResponse = channel.basicGet("fruits", true);
                    assertThat(getResponse).isNull();
                }
            }
        }

        @Test
        void non_long_message_ttl_in_publishers_is_not_used() throws IOException, TimeoutException, InterruptedException {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    channel.queueDeclare("fruits", true, false, false, null);
                    AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .expiration("test")
                        .build();
                    channel.basicPublish("", "fruits", properties, "banana".getBytes());
                    TimeUnit.MILLISECONDS.sleep(100L);
                    GetResponse getResponse = channel.basicGet("fruits", true);
                    assertThat(getResponse).isNotNull();
                }
            }
        }
    }
}
