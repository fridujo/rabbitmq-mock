package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
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
}
