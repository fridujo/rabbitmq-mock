package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
                    .withMessage("com.rabbitmq.client.ShutdownSignalException: no exchange 'test1' in vhost '/' channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND, class-id=40, method-id=10)")
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
                    .withMessage("com.rabbitmq.client.ShutdownSignalException: no queue 'test1' in vhost '/' channel error; protocol method: #method<channel.close>(reply-code=404, reply-text=NOT_FOUND, class-id=50, method-id=10)")
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

    @Test
    void basicConsume_with_callbacks() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                AtomicBoolean cancelled = new AtomicBoolean();
                String consumerTag = channel.basicConsume("",
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
                assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> channel.txRollback())
                    .withMessage("No started transaction (make sure you called txSelect before txRollback");
            }
        }
    }

    @Test
    void commit_without_select_throws() throws IOException, TimeoutException {
        try (Connection conn = new MockConnectionFactory().newConnection()) {
            try (Channel channel = conn.createChannel()) {
                assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> channel.txCommit())
                    .withMessage("No started transaction (make sure you called txSelect before txCommit");
            }
        }
    }

}
