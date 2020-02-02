package com.github.fridujo.rabbitmq.mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.RpcClient;
import com.rabbitmq.client.RpcClientParams;
import com.rabbitmq.client.UnroutableRpcRequestException;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.Return;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class ChannelTest {

    @Test
    void close_closes_channel() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.close();
                assertThat(channel.isOpen()).isFalse();
            }
        }
    }

    @Test
    void abort_closes_channel() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.abort();
                assertThat(channel.isOpen()).isFalse();
            }
        }
    }

    @Test
    void channel_number_can_be_accessed() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            int channelNumber = new Random().nextInt();
            try (Channel channel = conn.createChannel(channelNumber)) {
                assertThat(channel.getChannelNumber()).isEqualTo(channelNumber);
            }
        }
    }

    @Test
    void getConnection_returns_the_actual_connection_which_created_the_channel() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.getConnection()).isEqualTo(conn);
            }
        }
    }

    @Test
    void basicQos_does_not_throw() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.basicQos(30);
            }
        }
    }

    @Test
    void exchangeDeclare_creates_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDeclare("test1", "fanout")).isNotNull();
                assertThat(channel.exchangeDeclarePassive("test1")).isNotNull();
            }
        }
    }

    @Test
    void exchangeDeclare_twice_keeps_existing_bindings() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String exchangeName = "test1";
                channel.exchangeDeclare(exchangeName, "fanout");
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, "unused");
                // Declare the same exchange a second time
                channel.exchangeDeclare(exchangeName, "fanout");

                channel.basicPublish("test1", "unused", null, "test".getBytes());
                GetResponse getResponse = channel.basicGet(queueName, true);

                assertThat(getResponse).isNotNull();
            }
        }
    }

    @Test
    void exchangeDeclare_twice_with_a_different_type_throws() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String exchangeName = "test1";
                channel.exchangeDeclare(exchangeName, "fanout");
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, "unused");

                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.exchangeDeclare(exchangeName, "topic"))
                    .withCauseInstanceOf(ShutdownSignalException.class)
                    .withMessageContaining("channel error; protocol method: #method<channel.close>" +
                        "(reply-code=406, reply-text=PRECONDITION_FAILED - inequivalent arg 'type' " +
                        "for exchange 'test1' in vhost '/' received 'topic' but current is 'fanout', class-id=40, method-id=10)");
            }
        }
    }

    @Test
    void exchangeDeclareNoWait_creates_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclareNoWait("test1", BuiltinExchangeType.DIRECT, true, false, false, null);
                assertThat(channel.exchangeDeclarePassive("test1")).isNotNull();
            }
        }
    }

    @Test
    void exchangeDeclarePassive_throws_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.exchangeDeclarePassive("test1"))
                    .withMessage("com.rabbitmq.client.ShutdownSignalException: channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no exchange 'test1' in vhost '/', class-id=40, method-id=10)")
                    .withCauseExactlyInstanceOf(ShutdownSignalException.class);
            }
        }
    }

    @Test
    void exchangeDelete_removes_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclareNoWait("test1", BuiltinExchangeType.DIRECT, true, false, false, null);
                assertThat(channel.exchangeDelete("test1")).isNotNull();
                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.exchangeDeclarePassive("test1"));
            }
        }
    }

    @Test
    void exchangeDeleteNoWait_removes_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclareNoWait("test1", BuiltinExchangeType.DIRECT, true, false, false, null);
                channel.exchangeDeleteNoWait("test1", false);
                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.exchangeDeclarePassive("test1"));
            }
        }
    }

    @Test
    void exchangeDelete_does_nothing_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDelete(UUID.randomUUID().toString())).isNotNull();
                assertThat(channel.exchangeDelete(UUID.randomUUID().toString(), false)).isNotNull();
                channel.exchangeDeleteNoWait(UUID.randomUUID().toString(), false);
            }
        }
    }

    @Test
    void exchangeBind_binds_two_exchanges() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.queueDeclare()).isNotNull();

                assertThat(channel.exchangeBind("ex-to", "ex-from", "test.key")).isNotNull();
                assertThat(channel.queueBind("", "ex-to", "queue.used")).isNotNull();

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", false);
                assertThat(response).isNotNull();
                assertThat(new String(response.getBody())).isEqualTo("test message");
            }
        }
    }

    @Test
    void exchangeBindNoWait_binds_two_exchanges() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.queueDeclare()).isNotNull();

                channel.exchangeBindNoWait("ex-to", "ex-from", "test.key", null);
                assertThat(channel.queueBind("", "ex-to", "queue.used")).isNotNull();

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", true);
                assertThat(response).isNotNull();
                assertThat(new String(response.getBody())).isEqualTo("test message");
            }
        }
    }

    @Test
    void exchangeUnbind_removes_binding() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.queueDeclare()).isNotNull();

                channel.exchangeBindNoWait("ex-to", "ex-from", "test.key", null);
                assertThat(channel.queueBind("", "ex-to", "queue.used")).isNotNull();

                assertThat(channel.exchangeUnbind("ex-to", "ex-from", "test.key")).isNotNull();

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", true);
                assertThat(response).isNull();
            }
        }
    }

    @Test
    void exchangeUnbindNoWait_removes_binding() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.queueDeclare()).isNotNull();

                channel.exchangeBindNoWait("ex-to", "ex-from", "test.key", null);
                assertThat(channel.queueBind("", "ex-to", "queue.used")).isNotNull();

                channel.exchangeUnbindNoWait("ex-to", "ex-from", "test.key", null);

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", true);
                assertThat(response).isNull();
            }
        }
    }

    @Test
    void queueDeclareNoWait_declares_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.queueDeclareNoWait("", true, false, false, null);
                assertThat(channel.queueDeclarePassive("")).isNotNull();
            }
        }
    }

    @Test
    void queueDeclarePassive_throws_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.queueDeclarePassive("test1"))
                    .withMessage("com.rabbitmq.client.ShutdownSignalException: channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND - no queue 'test1' in vhost '/', class-id=50, method-id=10)")
                    .withCauseExactlyInstanceOf(ShutdownSignalException.class);
            }
        }
    }

    @Test
    void queueDelete_deletes_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.queueDeclare()).isNotNull();
                assertThat(channel.queueDelete("")).isNotNull();
                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.queueDeclarePassive(""));
            }
        }
    }

    @Test
    void queueDeleteNoWait_deletes_it() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.queueDeclare()).isNotNull();
                channel.queueDeleteNoWait("", false, false);
                assertThatExceptionOfType(IOException.class)
                    .isThrownBy(() -> channel.queueDeclarePassive(""));
            }
        }
    }

    @Test
    void queueDelete_does_nothing_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.queueDelete(UUID.randomUUID().toString())).isNotNull();
                assertThat(channel.queueDelete(UUID.randomUUID().toString(), false, false)).isNotNull();
                channel.queueDeleteNoWait(UUID.randomUUID().toString(), false, false);
            }
        }
    }

    @Test
    void queuePurge_removes_all_messages() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());
                channel.basicPublish("", queueName, null, "test message".getBytes());

                assertThat(channel.messageCount("")).isEqualTo(2);
                assertThat(channel.queuePurge("")).isNotNull();
                assertThat(channel.messageCount("")).isEqualTo(0);
            }
        }
    }

    @Test
    void basicNack_with_requeue_replaces_message_in_queue() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                GetResponse getResponse = channel.basicGet("", false);

                channel.basicNack(getResponse.getEnvelope().getDeliveryTag(), true, true);

                getResponse = channel.basicGet("", false);

                channel.basicReject(getResponse.getEnvelope().getDeliveryTag(), false);

                assertThat(channel.basicGet("", false)).isNull();
            }
        }
    }

    @Test
    void basicConsume_with_consumer() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                AtomicBoolean cancelled = new AtomicBoolean();
                String consumerTag = channel.basicConsume("", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.add(new String(body));
                    }

                    @Override
                    public void handleCancelOk(String consumerTag) {
                        cancelled.set(true);
                    }
                });

                TimeUnit.MILLISECONDS.sleep(200L);

                assertThat(messages).hasSize(1);
                assertThat(cancelled).isFalse();
                assertThat(channel.consumerCount("")).isEqualTo(1);

                messages.clear();
                channel.basicCancel(consumerTag);

                assertThat(cancelled).isTrue();
                assertThat(channel.consumerCount("")).isEqualTo(0);

                channel.basicPublish("", queueName, null, "test message".getBytes());
                TimeUnit.MILLISECONDS.sleep(50L);

                assertThat(messages).hasSize(0);
            }
        }
    }

    @RepeatedTest(31)
    void basicConsume_concurrent_queue_access() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();

                BlockingQueue<String> messages = new LinkedBlockingQueue<>();
                channel.basicConsume("", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.offer(new String(body));
                    }
                });

                int totalMessages = 101;
                for (int i = 1; i <= totalMessages; i++) {
                    channel.basicPublish("", queueName, null, "test message".getBytes());
                }
                for (int i = 1; i <= totalMessages; i++) {
                    assertThat(messages.poll(200L, TimeUnit.MILLISECONDS)).isNotNull();
                }
            }
        }
    }


    @Test
    void basicConsume_with_callbacks() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                AtomicBoolean cancelled = new AtomicBoolean();
                channel.basicConsume("",
                    (ct, delivery) -> messages.add(new String(delivery.getBody())),
                    ct -> cancelled.set(true));

                TimeUnit.MILLISECONDS.sleep(200L);

                assertThat(messages).hasSize(1);
                assertThat(cancelled).isFalse();

                channel.queueDelete("");

                assertThat(cancelled).isTrue();
            }
        }
    }

    @Test
    void basicConsume_with_shutdown_callback() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                channel.basicConsume("",
                    (ct, delivery) -> messages.add(new String(delivery.getBody())),
                    (ct, sig) -> {
                    });

                TimeUnit.MILLISECONDS.sleep(200L);

                assertThat(messages).hasSize(1);
            }
        }
    }

    @Test
    void basicRecover_requeue_all_unacked_messages() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message 1".getBytes());
                channel.basicPublish("", queueName, null, "test message 2".getBytes());

                assertThat(channel.messageCount(queueName)).isEqualTo(2);

                assertThat(channel.basicGet("", false)).isNotNull();
                assertThat(channel.basicGet("", false)).isNotNull();

                assertThat(channel.messageCount(queueName)).isEqualTo(0);

                assertThat(channel.basicRecover()).isNotNull();

                assertThat(channel.messageCount(queueName)).isEqualTo(2);
            }
        }
    }

    @Test
    void queueUnbind_removes_binding() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThat(channel.exchangeDeclare("ex-test", BuiltinExchangeType.FANOUT)).isNotNull();
                assertThat(channel.queueDeclare()).isNotNull();
                channel.queueBindNoWait("", "ex-test", "some.key", null);

                channel.basicPublish("ex-test", "unused", null, "test message".getBytes());
                assertThat(channel.basicGet("", false)).isNotNull();

                assertThat(channel.queueUnbind("", "ex-test", "some.key")).isNotNull();

                channel.basicPublish("ex-test", "unused", null, "test message".getBytes());
                assertThat(channel.basicGet("", false)).isNull();
            }
        }
    }

    @Test
    void transaction_commit_propagate_publish_ack_and_reject() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String firstQueue = channel.queueDeclare().getQueue();
                String secondQueue = channel.queueDeclare().getQueue();

                channel.basicPublish("", secondQueue, null, "to_ack".getBytes());
                channel.basicPublish("", secondQueue, null, "to_reject".getBytes());
                channel.basicPublish("", secondQueue, null, "to_nack".getBytes());
                assertThat(channel.messageCount(secondQueue)).isEqualTo(3);

                channel.txSelect();

                channel.basicPublish("", firstQueue, null, "test message".getBytes());
                assertThat(channel.messageCount(firstQueue)).isEqualTo(0L);

                GetResponse getResponse;
                while ((getResponse = channel.basicGet(secondQueue, false)) != null) {
                    if (new String(getResponse.getBody()).contains("reject")) {
                        channel.basicReject(getResponse.getEnvelope().getDeliveryTag(), false);
                    } else if (new String(getResponse.getBody()).contains("nack")) {
                        channel.basicNack(getResponse.getEnvelope().getDeliveryTag(), false, false);
                    } else {
                        channel.basicAck(getResponse.getEnvelope().getDeliveryTag(), false);
                    }
                }
                assertThat(channel.messageCount(secondQueue)).isEqualTo(0L);


                assertThat(channel.txCommit()).isNotNull();
                assertThat(channel.messageCount(firstQueue)).isEqualTo(1L);
                assertThat(channel.messageCount(secondQueue)).isEqualTo(0L);
            }
        }
    }

    @Test
    void transaction_rollback_propagate_publish_ack_and_reject() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String firstQueue = channel.queueDeclare().getQueue();
                String secondQueue = channel.queueDeclare().getQueue();

                channel.basicPublish("", secondQueue, null, "to_ack".getBytes());
                channel.basicPublish("", secondQueue, null, "to_reject".getBytes());
                channel.basicPublish("", secondQueue, null, "to_nack".getBytes());
                assertThat(channel.messageCount(secondQueue)).isEqualTo(3);

                channel.txSelect();

                channel.basicPublish("", firstQueue, null, "test message".getBytes());
                assertThat(channel.messageCount(firstQueue)).isEqualTo(0L);

                GetResponse getResponse;
                while ((getResponse = channel.basicGet(secondQueue, false)) != null) {
                    if (new String(getResponse.getBody()).contains("reject")) {
                        channel.basicReject(getResponse.getEnvelope().getDeliveryTag(), false);
                    } else if (new String(getResponse.getBody()).contains("nack")) {
                        channel.basicNack(getResponse.getEnvelope().getDeliveryTag(), false, false);
                    } else {
                        channel.basicAck(getResponse.getEnvelope().getDeliveryTag(), false);
                    }
                }
                assertThat(channel.messageCount(secondQueue)).isEqualTo(0L);


                assertThat(channel.txRollback()).isNotNull();
                assertThat(channel.messageCount(firstQueue)).isEqualTo(0L);
                assertThat(channel.messageCount(secondQueue)).isEqualTo(0L);
            }
        }
    }

    @Test
    void rollback_without_select_throws() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThatExceptionOfType(IllegalStateException.class)
                    .isThrownBy(() -> channel.txRollback())
                    .withMessage("No started transaction (make sure you called txSelect before txRollback");
            }
        }
    }

    @Test
    void commit_without_select_throws() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThatExceptionOfType(IllegalStateException.class)
                    .isThrownBy(() -> channel.txCommit())
                    .withMessage("No started transaction (make sure you called txSelect before txCommit");
            }
        }
    }

    @Test
    void commit_or_rollback_can_be_called_multiple_times_after_a_single_select() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.txSelect();
                String queue = channel.queueDeclare().getQueue();
                channel.txRollback();
                channel.txCommit();

                channel.basicPublish("", queue, null, "first message".getBytes());
                assertThat(channel.basicGet(queue, true)).isNull();
                channel.txCommit();
                assertThat(channel.basicGet(queue, true)).isNotNull();

                channel.basicPublish("", queue, null, "second message".getBytes());
                assertThat(channel.basicGet(queue, true)).isNull();
                channel.txCommit();
                assertThat(channel.basicGet(queue, true)).isNotNull();
                // Channel contained only one message as transactions are cleared after commit
                assertThat(channel.basicGet(queue, true)).isNull();

                channel.basicPublish("", queue, null, "third message".getBytes());
                assertThat(channel.basicGet(queue, true)).isNull();
                channel.txRollback();
                assertThat(channel.basicGet(queue, true)).isNull();
            }
        }
    }

    @Test
    void directReplyTo_basicPublish_basicGet() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {

                String queue = channel.queueDeclare().getQueue();

                channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().replyTo("amq.rabbitmq.reply-to").build(), "ping".getBytes());
                assertThat(channel.messageCount(queue)).isEqualTo(1);

                final GetResponse basicGet = channel.basicGet(queue, true);
                final String replyTo = basicGet.getProps().getReplyTo();
                assertThat(replyTo).startsWith("amq.gen-");

                channel.basicPublish("", replyTo, null, "pong".getBytes());

                final GetResponse reply = channel.basicGet("amq.rabbitmq.reply-to", true);
                assertThat(new String(reply.getBody())).isEqualTo("pong");
            }
        }
    }

    @Test
    void directReplyTo_basicPublish_basicConsume() throws IOException, TimeoutException, InterruptedException {
        final String replyTo;
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {

                String queue = channel.queueDeclare().getQueue();

                channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().replyTo("amq.rabbitmq.reply-to").build(), "ping".getBytes());
                assertThat(channel.messageCount(queue)).isEqualTo(1);

                final GetResponse basicGet = channel.basicGet(queue, true);
                replyTo = basicGet.getProps().getReplyTo();
                assertThat(replyTo).startsWith("amq.gen-");

                channel.basicPublish("", replyTo, null, "pong".getBytes());

                CountDownLatch latch = new CountDownLatch(1);
                AtomicBoolean cancelled = new AtomicBoolean();
                AtomicReference<String> reply = new AtomicReference<>();
                String consumerTag = channel.basicConsume("amq.rabbitmq.reply-to", true, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        assertThat(reply.compareAndSet(null, new String(body))).isTrue();
                        latch.countDown();
                    }

                    @Override
                    public void handleCancelOk(String consumerTag) {
                        cancelled.set(true);
                    }
                });

                latch.await(1, TimeUnit.SECONDS);
                channel.basicCancel(consumerTag);

                assertThat(cancelled).isTrue();
                assertThat(reply.get()).isEqualTo("pong");
            }
        }

        // assert that internal queue is removed
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThatThrownBy(() -> channel.queueDeclarePassive(replyTo)).isInstanceOf(IOException.class);
            }
        }
    }

    @ParameterizedTest(name = "publish to {0} should return replyCode {1}")
    @CsvSource({
        "existingQueue, -1",
        "unexistingQueue, 312",
    })
    void mandatory_publish_with_default_exchange(String routingKey, int expectedReplyCode) throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {

                channel.queueDeclare("existingQueue", false, false, false, Collections.emptyMap()).getQueue();
                AtomicInteger replyCodeHolder = new AtomicInteger(-1);
                ReturnListener returnListener = (replyCode, replyText, exchange, routingKey1, properties, body) -> replyCodeHolder.set(replyCode);
                channel.addReturnListener(returnListener);
                channel.basicPublish("", routingKey, true, MessageProperties.BASIC, "msg".getBytes());
                assertThat(replyCodeHolder.get()).isEqualTo(expectedReplyCode);
            }
        }
    }


    @ParameterizedTest(name = "publish to {0} should return replyCode {1}")
    @CsvSource({
        "boundRoutingKey, -1",
        "unboundRoutingKey, 312",
    })
    void mandatory_publish_with_direct_exchange(String routingKey, int expectedReplyCode) throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("test", "direct");
                channel.queueDeclare("existingQueue", false, false, false, Collections.emptyMap()).getQueue();
                channel.queueBind("existingQueue","test", "boundRoutingKey");
                AtomicInteger replyCodeHolder = new AtomicInteger(-1);
                ReturnCallback returnListener = r -> replyCodeHolder.set(r.getReplyCode());
                channel.addReturnListener(returnListener);
                channel.basicPublish("test", routingKey, true, MessageProperties.BASIC, "msg".getBytes());
                assertThat(replyCodeHolder.get()).isEqualTo(expectedReplyCode);
            }
        }
    }

    private static class TrackingCallback implements ReturnCallback {

        int invocationCount;

        @Override
        public void handle(Return returnMessage) {
            invocationCount++;
        }
    }

    @Test
    void mandatory_publish_with_multiple_listeners() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                TrackingCallback firstCallbackThatIsRegistedTwice = new TrackingCallback();
                TrackingCallback secondCallbackThatWillBeRemoved = new TrackingCallback();
                TrackingCallback thirdCallback = new TrackingCallback();
                channel.addReturnListener(firstCallbackThatIsRegistedTwice);
                channel.addReturnListener(firstCallbackThatIsRegistedTwice);
                channel.addReturnListener(r -> {throw new RuntimeException("Listener throwing exception");});
                ReturnListener returnListener = channel.addReturnListener(secondCallbackThatWillBeRemoved);
                channel.addReturnListener(thirdCallback);
                channel.removeReturnListener(returnListener);
                channel.basicPublish("", "unexisting", true, MessageProperties.BASIC, "msg".getBytes());
                assertThat(firstCallbackThatIsRegistedTwice.invocationCount).isEqualTo(1);
                assertThat(secondCallbackThatWillBeRemoved.invocationCount).isEqualTo(0);
                assertThat(thirdCallback.invocationCount).isEqualTo(1);
            }
        }
    }

    @Test
    void rpcClient() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {

                String queue = channel.queueDeclare().getQueue();
                channel.basicConsume(queue, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        channel.basicPublish(
                            "",
                            properties.getReplyTo(),
                            MessageProperties.BASIC.builder().correlationId(properties.getCorrelationId()).build(), "pong".getBytes()
                        );
                    }
                });

                RpcClientParams params = new RpcClientParams();
                params.channel(channel);
                params.exchange("");
                params.routingKey(queue);
                RpcClient client = new RpcClient(params);
                RpcClient.Response response = client.responseCall("ping".getBytes());
                assertThat(response.getBody()).isEqualTo("pong".getBytes());
            }
        }
    }

    @Test
    void rpcClientSupportsMandatory() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                RpcClientParams params = new RpcClientParams();
                params.channel(channel);
                params.exchange("");
                params.routingKey("unexistingQueue");
                params.useMandatory(true);
                RpcClient client = new RpcClient(params);
                try {
                    client.responseCall("ping".getBytes());
                    fail("Expected exception");
                } catch (UnroutableRpcRequestException e) {
                    assertThat(e.getReturnMessage().getReplyText()).isEqualTo("No route");
                }
            }
        }
    }


    @Test
    void directReplyTo_basicConsume_noAutoAck() {
        assertThatThrownBy(() -> {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    channel.basicConsume("amq.rabbitmq.reply-to", false, new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body) {
                            fail("not implemented");
                        }

                        @Override
                        public void handleCancelOk(String consumerTag) {
                            fail("not implemented");
                        }
                    });
                }
            }
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void directReplyTo_basicGet_noAutoAck() {
        assertThatThrownBy(() -> {
            try (Connection conn = new MockConnectionFactory().newConnection()) {
                try (Channel channel = conn.createChannel()) {
                    channel.basicGet("amq.rabbitmq.reply-to", false);
                }
            }
        }).isInstanceOf(IllegalStateException.class);
    }
}
