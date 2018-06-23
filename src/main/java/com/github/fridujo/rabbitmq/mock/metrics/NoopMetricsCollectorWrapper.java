package com.github.fridujo.rabbitmq.mock.metrics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class NoopMetricsCollectorWrapper implements MetricsCollectorWrapper {
    @Override
    public void newConnection(Connection connection) {
        // no implementation
    }

    @Override
    public void closeConnection(Connection connection) {
        // no implementation
    }

    @Override
    public void newChannel(Channel channel) {
        // no implementation
    }

    @Override
    public void closeChannel(Channel channel) {
        // no implementation
    }

    @Override
    public void basicPublish(Channel channel) {
        // no implementation
    }

    @Override
    public void consumedMessage(Channel channel, long deliveryTag, boolean autoAck) {
        // no implementation
    }

    @Override
    public void consumedMessage(Channel channel, long deliveryTag, String consumerTag) {
        // no implementation
    }

    @Override
    public void basicAck(Channel channel, long deliveryTag, boolean multiple) {
        // no implementation
    }

    @Override
    public void basicNack(Channel channel, long deliveryTag) {
        // no implementation
    }

    @Override
    public void basicReject(Channel channel, long deliveryTag) {
        // no implementation
    }

    @Override
    public void basicConsume(Channel channel, String consumerTag, boolean autoAck) {
        // no implementation
    }

    @Override
    public void basicCancel(Channel channel, String consumerTag) {
        // no implementation
    }
}
