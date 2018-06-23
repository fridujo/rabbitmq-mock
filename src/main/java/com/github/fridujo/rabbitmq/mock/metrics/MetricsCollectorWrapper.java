package com.github.fridujo.rabbitmq.mock.metrics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static com.github.fridujo.rabbitmq.mock.tool.Classes.missingClass;

public interface MetricsCollectorWrapper {

    void newConnection(Connection connection);

    void closeConnection(Connection connection);

    void newChannel(Channel channel);

    void closeChannel(Channel channel);

    void basicPublish(Channel channel);

    void consumedMessage(Channel channel, long deliveryTag, boolean autoAck);

    void consumedMessage(Channel channel, long deliveryTag, String consumerTag);

    void basicAck(Channel channel, long deliveryTag, boolean multiple);

    void basicNack(Channel channel, long deliveryTag);

    void basicReject(Channel channel, long deliveryTag);

    void basicConsume(Channel channel, String consumerTag, boolean autoAck);

    void basicCancel(Channel channel, String consumerTag);

    class Builder {

        public static MetricsCollectorWrapper build(ConnectionFactory connectionFactory) {
            return build(MetricsCollectorWrapper.Builder.class.getClassLoader(), connectionFactory);
        }

        public static MetricsCollectorWrapper build(ClassLoader classLoader, ConnectionFactory connectionFactory) {
            final MetricsCollectorWrapper metricsCollectorWrapper;
            if (missingClass(classLoader, "com.rabbitmq.client.MetricsCollector")) {
                metricsCollectorWrapper = new NoopMetricsCollectorWrapper();

            } else {
                metricsCollectorWrapper = new ImplementedMetricsCollectorWrapper(connectionFactory);
            }
            return metricsCollectorWrapper;
        }
    }
}
