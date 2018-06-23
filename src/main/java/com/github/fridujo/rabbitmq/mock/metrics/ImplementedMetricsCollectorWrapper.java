package com.github.fridujo.rabbitmq.mock.metrics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MetricsCollector;
import com.rabbitmq.client.NoOpMetricsCollector;

public class ImplementedMetricsCollectorWrapper implements MetricsCollectorWrapper {

    private final ConnectionFactory connectionFactory;

    ImplementedMetricsCollectorWrapper(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        if (connectionFactory.getMetricsCollector() == null) {
            connectionFactory.setMetricsCollector(new NoOpMetricsCollector());
        }
    }

    private MetricsCollector mc() {
        return connectionFactory.getMetricsCollector();
    }

    @Override
    public void newConnection(Connection connection) {
        mc().newConnection(connection);
    }

    @Override
    public void closeConnection(Connection connection) {
        mc().closeConnection(connection);
    }

    @Override
    public void newChannel(Channel channel) {
        mc().newChannel(channel);
    }

    @Override
    public void closeChannel(Channel channel) {
        mc().closeChannel(channel);
    }

    @Override
    public void basicPublish(Channel channel) {
        mc().basicPublish(channel);
    }

    @Override
    public void consumedMessage(Channel channel, long deliveryTag, boolean autoAck) {
        mc().consumedMessage(channel, deliveryTag, autoAck);
    }

    @Override
    public void consumedMessage(Channel channel, long deliveryTag, String consumerTag) {
        mc().consumedMessage(channel, deliveryTag, consumerTag);
    }

    @Override
    public void basicAck(Channel channel, long deliveryTag, boolean multiple) {
        mc().basicAck(channel, deliveryTag, multiple);
    }

    @Override
    public void basicNack(Channel channel, long deliveryTag) {
        mc().basicNack(channel, deliveryTag);
    }

    @Override
    public void basicReject(Channel channel, long deliveryTag) {
        mc().basicReject(channel, deliveryTag);
    }

    @Override
    public void basicConsume(Channel channel, String consumerTag, boolean autoAck) {
        mc().basicConsume(channel, consumerTag, autoAck);
    }

    @Override
    public void basicCancel(Channel channel, String consumerTag) {
        mc().basicCancel(channel, consumerTag);
    }
}
