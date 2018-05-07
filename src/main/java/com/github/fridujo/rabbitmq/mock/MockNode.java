package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQImpl;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MockNode {

    private final Map<String, MockExchange> exchanges = new ConcurrentHashMap<>();

    private final Map<String, MockQueue> queues = new ConcurrentHashMap<>();


    public MockNode() {
        exchanges.put("", new MockDefaultExchange(this));
        queues.put("unrouted", new MockQueue("unrouted"));
    }

    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        if (!exchanges.containsKey(exchange)) {
            throw new IllegalArgumentException("No exchange named " + exchange);
        }
        AMQP.BasicProperties nonNullProperties = props != null ? props : new AMQP.BasicProperties.Builder().build();

        exchanges.get(exchange).publish(routingKey, nonNullProperties, body);
    }

    public String basicConsume(String queueName, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) {
        final String definitiveConsumerTag;
        if ("".equals(consumerTag)) {
            definitiveConsumerTag = "amq.ctag-test" + UUID.randomUUID();
        } else {
            definitiveConsumerTag = consumerTag;
        }

        getQueueUnchecked(queueName).addConsumer(definitiveConsumerTag, callback, autoAck);

        return definitiveConsumerTag;
    }

    Optional<MockQueue> getQueue(String name) {
        return Optional.ofNullable(queues.get(name));
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchangeName, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        exchanges.put(exchangeName, MockExchangeFactory.build(exchangeName, type));
        return new AMQImpl.Exchange.DeclareOk();
    }

    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        queues.putIfAbsent(queue, new MockQueue(queue));
        return new AMQP.Queue.DeclareOk.Builder()
            .queue(queue)
            .build();
    }

    public AMQP.Queue.BindOk queueBind(String queueName, String exchangeName, String routingKey, Map<String, Object> arguments) {
        MockExchange exchange = getExchangeUnchecked(exchangeName);
        MockQueue queue = getQueueUnchecked(queueName);
        exchange.bind(queue, routingKey);
        return new AMQImpl.Queue.BindOk();
    }

    public void basicAck(long deliveryTag) {
        queues.values().forEach(q -> q.basicAck(deliveryTag));
    }

    public GetResponse basicGet(String queueName, boolean autoAck) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.basicGet(autoAck);
    }


    private MockExchange getExchangeUnchecked(String exchangeName) {
        if (!exchanges.containsKey(exchangeName)) {
            throw new IllegalArgumentException("No exchange named " + exchangeName);
        }
        return exchanges.get(exchangeName);
    }

    private MockQueue getQueueUnchecked(String queueName) {
        if (!queues.containsKey(queueName)) {
            throw new IllegalArgumentException("No queue named " + queueName);
        }
        return queues.get(queueName);
    }
}
