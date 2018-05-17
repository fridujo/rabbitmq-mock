package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQImpl;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class MockNode implements ReceiverRegistry {

    private final MockDefaultExchange defaultExchange = new MockDefaultExchange(this);
    private final MockQueue unroutedQueue = new MockQueue("unrouted");
    private final Map<String, MockExchange> exchanges = new ConcurrentHashMap<>();
    private final Map<String, MockQueue> queues = new ConcurrentHashMap<>();
    private final RandomStringGenerator consumerTagGenerator = new RandomStringGenerator(
        "amq.ctag-",
        "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        22);


    public MockNode() {
        exchanges.put("", defaultExchange);
        queues.put("unrouted", unroutedQueue);
    }

    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        if (!exchanges.containsKey(exchange)) {
            throw new IllegalArgumentException("No exchange named " + exchange);
        }
        AMQP.BasicProperties nonNullProperties = props != null ? props : new AMQP.BasicProperties.Builder().build();

        exchanges.get(exchange).publish(null, routingKey, nonNullProperties, body);
    }

    public String basicConsume(String queueName, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback, Supplier<Long> deliveryTagSupplier) {
        final String definitiveConsumerTag;
        if ("".equals(consumerTag)) {
            definitiveConsumerTag = consumerTagGenerator.generate();
        } else {
            definitiveConsumerTag = consumerTag;
        }

        getQueueUnchecked(queueName).basicConsume(definitiveConsumerTag, callback, autoAck, deliveryTagSupplier);

        return definitiveConsumerTag;
    }

    Optional<MockQueue> getQueue(String name) {
        return Optional.ofNullable(queues.get(name));
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchangeName, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        exchanges.put(exchangeName, MockExchangeFactory.build(exchangeName, type, this));
        return new AMQImpl.Exchange.DeclareOk();
    }

    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) {
        exchanges.remove(exchange);
        return new AMQImpl.Exchange.DeleteOk();
    }

    public AMQP.Exchange.BindOk exchangeBind(String destinationName, String sourceName, String routingKey, Map<String, Object> arguments) {
        MockExchange source = getExchangeUnchecked(sourceName);
        MockExchange destination = getExchangeUnchecked(destinationName);
        source.bind(destination.pointer(), routingKey);
        return new AMQImpl.Exchange.BindOk();
    }

    public AMQP.Exchange.UnbindOk exchangeUnbind(String destinationName, String sourceName, String routingKey, Map<String, Object> arguments) {
        MockExchange source = getExchangeUnchecked(sourceName);
        MockExchange destination = getExchangeUnchecked(destinationName);
        source.unbind(destination.pointer(), routingKey);
        return new AMQImpl.Exchange.UnbindOk();
    }

    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        queues.putIfAbsent(queue, new MockQueue(queue));
        return new AMQP.Queue.DeclareOk.Builder()
            .queue(queue)
            .build();
    }

    public AMQP.Queue.DeleteOk queueDelete(String queueName, boolean ifUnused, boolean ifEmpty) {
        MockQueue queue = queues.remove(queueName);
        return new AMQImpl.Queue.DeleteOk(queue != null ? queue.messageCount() : 0);
    }

    public AMQP.Queue.BindOk queueBind(String queueName, String exchangeName, String routingKey, Map<String, Object> arguments) {
        MockExchange exchange = getExchangeUnchecked(exchangeName);
        MockQueue queue = getQueueUnchecked(queueName);
        exchange.bind(queue.pointer(), routingKey);
        return new AMQImpl.Queue.BindOk();
    }

    public AMQP.Queue.PurgeOk queuePurge(String queueName) {
        MockQueue queue = getQueueUnchecked(queueName);
        int messageCount = queue.purge();
        return new AMQImpl.Queue.PurgeOk(messageCount);
    }

    public GetResponse basicGet(String queueName, boolean autoAck, Supplier<Long> deliveryTagSupplier) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.basicGet(autoAck, deliveryTagSupplier);
    }

    public void basicAck(long deliveryTag, boolean multiple) {
        queues.values().forEach(q -> q.basicAck(deliveryTag, multiple));
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        queues.values().forEach(q -> q.basicNack(deliveryTag, multiple, requeue));
    }

    public void basicReject(long deliveryTag, boolean requeue) {
        queues.values().forEach(q -> q.basicReject(deliveryTag, requeue));
    }

    public void basicCancel(String consumerTag) {
        queues.values().forEach(q -> q.basicCancel(consumerTag));
    }

    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) {
        queues.values().forEach(q -> q.basicRecover(requeue));
        return new AMQImpl.Basic.RecoverOk();
    }

    @Override
    public Receiver getReceiver(ReceiverPointer receiverPointer) {
        final Receiver receiver;
        if (receiverPointer.type == ReceiverPointer.Type.EXCHANGE) {
            receiver = exchanges.getOrDefault(receiverPointer.name, defaultExchange);
        } else {
            receiver = queues.getOrDefault(receiverPointer.name, unroutedQueue);
        }
        return receiver;
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

    Optional<MockExchange> getExchange(String name) {
        return Optional.ofNullable(exchanges.get(name));
    }

    public int messageCount(String queueName) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.messageCount();
    }

    public long consumerCount(String queueName) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.consumerCount();
    }
}
