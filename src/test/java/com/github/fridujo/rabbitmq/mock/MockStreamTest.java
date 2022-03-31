package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class MockStreamTest {

    private Logger log = LoggerFactory.getLogger(getClass());
    @Test
    void basic_consume_case() throws IOException, TimeoutException, InterruptedException {

        final String exchangeName = "test-exchange";
        final String queueName = "test-queue";
        final String routingKey = "test-rk";
        final String consumerTag = "myConsumerTag";
        final int numMessages = 5;
        final Long initialOffset = 2L;

        HashMap<String, Object> queueArguments = new HashMap<>();
        queueArguments.put("x-queue-type", "stream");

        HashMap<String, Object> consumeArguments = new HashMap<>();
        consumeArguments.put("x-stream-offset", initialOffset);

        try (Connection conn = new MockConnectionFactory().newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                channel.queueDeclare(queueName, true, false, false, queueArguments);
                channel.queueBind(queueName, exchangeName, routingKey);


                for (int i = 0; i < numMessages; i++) {
                    byte[] messageBodyBytes = String.format("Hello number %d!", i).getBytes();
                    channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
                }

                TimeUnit.MILLISECONDS.sleep(200);

                List<String> messages = new ArrayList<>();
                channel.basicConsume(queueName, false, consumerTag, false, false, consumeArguments,
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(
                            String consumerTag,
                            Envelope envelope,
                            AMQP.BasicProperties properties,
                            byte[] body
                        ) throws IOException {

                            long deliveryTag = envelope.getDeliveryTag();
                            messages.add(new String(body));
                            channel.basicAck(deliveryTag, false);
                        }
                    });

                TimeUnit.SECONDS.sleep(1);

                assertThat(messages).hasSize(numMessages - initialOffset.intValue());
                assertThat(messages.get(0)).contains("Hello number 2!");
                assertThat(messages.get(1)).contains("Hello number 3!");
                assertThat(messages.get(2)).contains("Hello number 4!");
            }
        }
    }

    @Test
    void multiple_consume_case() throws IOException, TimeoutException, InterruptedException {

        final String exchangeName = "test-exchange";
        final String queueName = "test-queue";
        final String routingKey = "test-rk";
        final String consumerTagA = "myConsumerTagA";
        final String consumerTagB = "myConsumerTagB";
        final int numMessages = 5;
        final Long initialOffsetA = 3L;
        final Long initialOffsetB = 1L;

        HashMap<String, Object> queueArguments = new HashMap<>();
        queueArguments.put("x-queue-type", "stream");

        HashMap<String, Object> consumeArgumentsA = new HashMap<>();
        HashMap<String, Object> consumeArgumentsB = new HashMap<>();
        consumeArgumentsA.put("x-stream-offset", initialOffsetA);
        consumeArgumentsB.put("x-stream-offset", initialOffsetB);

        try (Connection conn = new MockConnectionFactory().newConnection()) {
            assertThat(conn).isInstanceOf(MockConnection.class);

            try (Channel channel = conn.createChannel()) {
                assertThat(channel).isInstanceOf(MockChannel.class);

                channel.exchangeDeclare(exchangeName, "direct", true);
                channel.queueDeclare(queueName, true, false, false, queueArguments);
                channel.queueBind(queueName, exchangeName, routingKey);


                for (int i = 0; i < numMessages; i++) {
                    byte[] messageBodyBytes = String.format("Hello number %d!", i).getBytes();
                    channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
                }

                TimeUnit.MILLISECONDS.sleep(200);

                List<String> messagesA = new ArrayList<>();
                List<String> messagesB = new ArrayList<>();
                DefaultConsumer callbackA = createConsumer(channel, messagesA);
                DefaultConsumer callbackB = createConsumer(channel, messagesB);
                channel.basicConsume(queueName, false, consumerTagA, false, false, consumeArgumentsA, callbackA);
                channel.basicConsume(queueName, false, consumerTagB, false, false, consumeArgumentsB, callbackB);

                TimeUnit.SECONDS.sleep(2);

                assertThat(messagesA).hasSize(numMessages - initialOffsetA.intValue());
                assertThat(messagesA.get(0)).contains("Hello number 3!");
                assertThat(messagesA.get(1)).contains("Hello number 4!");
                assertThat(messagesB).hasSize(numMessages - initialOffsetB.intValue());
                assertThat(messagesB.get(0)).contains("Hello number 1!");
                assertThat(messagesB.get(1)).contains("Hello number 2!");
                assertThat(messagesB.get(2)).contains("Hello number 3!");
                assertThat(messagesB.get(3)).contains("Hello number 4!");
            }
        }
    }

    private DefaultConsumer createConsumer(Channel channel, List<String> messages) {

        return new DefaultConsumer(channel) {

            @Override public void handleDelivery(
                String consumerTag,
                Envelope envelope,
                AMQP.BasicProperties properties,
                byte[] body
            ) throws IOException {

                long deliveryTag = envelope.getDeliveryTag();
                String m = new String(body);
                log.info("Consumer [{}] message: {}", consumerTag, m);
                log.info("Header offset: {}", properties.getHeaders().get("x-stream-offset"));
                messages.add(m);
                channel.basicAck(deliveryTag, false);
            }
        };
    }
}
