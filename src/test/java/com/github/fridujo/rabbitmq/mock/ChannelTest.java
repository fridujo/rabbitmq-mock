package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

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
}
